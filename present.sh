#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

usage() {
    cat <<'HELP'
Usage: ./present.sh [1|20|100|500|all|upgrade|mapbox|pitch]

  no argument   Start a local dashboard and choose a stage in the terminal.
  1, 20, 100,
  500           Send exactly that many synthetic spectator events.
  all           Run the four stages in order.
  upgrade       Refresh the pinned Kafka/Redis images and locked Python dependencies.
  mapbox        Check the public Mapbox token for the dashboard URL.
  pitch         Serve the standalone simulation and French slides without Docker.

Optional settings:
  DEMO_DASH_PORT=8050  DEMO_KAFKA_PORT=19092  DEMO_REDIS_PORT=16379
  DEMO_PITCH_PORT=8080  DEMO_STAGE_PAUSE=6  DEMO_STAGE_TIMEOUT=90
HELP
}

mode="${1:-interactive}"
if (( $# > 1 )); then usage >&2; exit 2; fi
case "$mode" in
    -h|--help|help) usage; exit 0 ;;
    interactive|1|20|100|500|all|upgrade|mapbox|pitch) ;;
    *) usage >&2; exit 2 ;;
esac

for port in "${DEMO_DASH_PORT:-8050}" "${DEMO_KAFKA_PORT:-19092}" "${DEMO_REDIS_PORT:-16379}" "${DEMO_PITCH_PORT:-8080}"; do
    if [[ ! "$port" =~ ^[1-9][0-9]{0,4}$ ]] || (( port > 65535 )); then
        printf 'Invalid demo port: %s\n' "$port" >&2
        exit 2
    fi
done
if [[ "$mode" == pitch ]]; then
    port="${DEMO_PITCH_PORT:-8080}"
    printf 'Rabat Flow   http://localhost:%s/\nSlides       http://localhost:%s/slides.html\n' "$port" "$port"
    exec python3 -m http.server "$port" --bind 127.0.0.1 --directory pitch/dist
fi

export DASH_PORT="${DEMO_DASH_PORT:-8050}"
origin="http://localhost:$DASH_PORT/"
env_args=()
if [[ -f .env ]]; then env_args=(--env-file .env); fi

need() {
    if ! command -v "$1" >/dev/null; then
        printf '%s is required. See the presentation setup in README.md.\n' "$1" >&2
        exit 1
    fi
}

need uv
if [[ "$mode" == mapbox ]]; then
    exec uv run --locked "${env_args[@]}" python -m worldcup.presentation mapbox "$origin"
fi

if docker compose version >/dev/null 2>&1; then
    compose=(docker compose -f compose.yaml)
elif command -v docker-compose >/dev/null; then
    compose=(docker-compose -f compose.yaml)
else
    printf 'Install Docker Compose to run Kafka and Redis.\n' >&2
    exit 1
fi
if [[ "$mode" == upgrade ]]; then
    "${compose[@]}" pull kafka redis
    exec uv sync --locked
fi

need java
if [[ ! -t 0 && "$mode" == interactive ]]; then
    printf 'Use ./present.sh all or a stage count when running without a terminal.\n' >&2
    exit 2
fi
pause="${DEMO_STAGE_PAUSE:-6}"
stage_timeout="${DEMO_STAGE_TIMEOUT:-90}"
if [[ ! "$pause" =~ ^[0-9]+$ || ! "$stage_timeout" =~ ^[1-9][0-9]*$ ]]; then
    printf 'Set DEMO_STAGE_PAUSE to a nonnegative number of seconds and DEMO_STAGE_TIMEOUT to a positive integer.\n' >&2
    exit 2
fi

if ! "${compose[@]}" ps >/dev/null; then
    printf 'Docker is unavailable. Start Docker and check your local Docker permissions.\n' >&2
    exit 1
fi
uv sync --locked
python="$(uv run --no-sync python -c 'import sys; print(sys.executable)')"
if ! uv run --no-sync "${env_args[@]}" python -m worldcup.presentation mapbox "$origin"; then
    export WORLDCUP_OFFLINE_MAP=1
    printf 'The 3D stadium view runs locally.\n'
fi

mkdir -p artifacts/presentation
session="$(mktemp -d "$PWD/artifacts/presentation/run.XXXXXX")"
project="worldcup-demo-${session##*.}"
project="${project,,}"
compose+=(-p "$project")
export KAFKA_PORT="${DEMO_KAFKA_PORT:-19092}"
export REDIS_PORT="${DEMO_REDIS_PORT:-16379}"
export KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:$KAFKA_PORT"
export REDIS_URL="redis://127.0.0.1:$REDIS_PORT/0"
export STREAM_ID="$project"
export SPARK_CHECKPOINT_ROOT="$session/checkpoints"
export REFERENCE_PATH="$session/reference.json"
export H3_RESOLUTION=10
export WORLDCUP_PRESENTATION_STAGE="$session/stage.json"
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$python"
export PYSPARK_SUBMIT_ARGS="${PYSPARK_SUBMIT_ARGS:---master local[4] --conf spark.sql.shuffle.partitions=2 --conf spark.ui.showConsoleProgress=false --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 pyspark-shell}"
spark_pid=
dash_pid=

cleanup() {
    status=$?
    trap - EXIT INT TERM
    if [[ -n "$dash_pid" ]]; then kill "$dash_pid" 2>/dev/null || true; fi
    if [[ -n "$spark_pid" ]]; then kill "$spark_pid" 2>/dev/null || true; fi
    if [[ -n "$dash_pid" ]]; then wait "$dash_pid" 2>/dev/null || true; fi
    if [[ -n "$spark_pid" ]]; then wait "$spark_pid" 2>/dev/null || true; fi
    if ! "${compose[@]}" down --volumes >"$session/cleanup.log" 2>&1; then
        printf 'Compose cleanup failed. See %s/cleanup.log\n' "$session" >&2
    fi
    printf 'Session logs and stage results: %s\n' "$session"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"$python" -m worldcup.presentation prepare "$REFERENCE_PATH"
printf '\nStarting isolated Kafka and Redis services…\n'
"${compose[@]}" up -d kafka redis
for attempt in {1..6}; do
    if "$python" scripts/create_topics.py --partitions 2 >"$session/kafka.log" 2>&1; then break; fi
    if (( attempt == 6 )); then cat "$session/kafka.log" >&2; exit 1; fi
    sleep 2
done

printf 'Starting Spark and the FastAPI WebSocket dashboard…\n'
"$python" spark_worldcup_traffic.py --trigger-interval '2 seconds' >"$session/spark.log" 2>&1 &
spark_pid=$!
uv run --no-sync "${env_args[@]}" python app_worldcup_control.py >"$session/dashboard.log" 2>&1 &
dash_pid=$!

# A fresh Spark query takes its "latest" offsets on its first trigger. Wait for
# those offsets so the first presentation event cannot be skipped at startup.
for (( second=0; second<120; second++ )); do
    if ! kill -0 "$spark_pid" "$dash_pid" 2>/dev/null; then
        printf 'Spark or FastAPI exited during startup. Check spark.log and dashboard.log.\n' >&2
        exit 1
    fi
    if [[ -f "$SPARK_CHECKPOINT_ROOT/crowd/sources/0/0" &&
          -f "$SPARK_CHECKPOINT_ROOT/hotels/sources/0/0" &&
          -f "$SPARK_CHECKPOINT_ROOT/invalid/sources/0/0" ]] &&
       "$python" -m worldcup.presentation ready "ws://127.0.0.1:$DASH_PORT/ws"; then
        break
    fi
    if (( second == 119 )); then
        printf 'Startup timed out. Check spark.log and dashboard.log.\n' >&2
        exit 1
    fi
    sleep 1
done
"$python" -m worldcup.presentation warmup --timeout "$stage_timeout"

printf '\nDashboard    %s\nSlides       %spitch/slides.html\nRabat Flow   %spitch/\n\n' "$origin" "$origin" "$origin"

stage() {
    printf '\nSending %s synthetic spectator events…\n' "$1"
    "$python" -m worldcup.presentation stage "$WORLDCUP_PRESENTATION_STAGE" "$1" --timeout "$stage_timeout"
}

all_stages() {
    local count
    for count in 1 20 100 500; do
        stage "$count"
        if [[ "$count" != 500 ]]; then sleep "$pause"; fi
    done
}

case "$mode" in
    all) all_stages ;;
    interactive)
        while read -r -p 'Stage [1 / 20 / 100 / 500 / all / q]: ' choice; do
            case "$choice" in
                1|20|100|500) stage "$choice" ;;
                all) all_stages ;;
                q) break ;;
                *) printf 'Choose 1, 20, 100, 500, all or q.\n' ;;
            esac
        done
        ;;
    *) stage "$mode" ;;
esac
if [[ -t 0 && "$mode" != interactive ]]; then
    read -r -p 'Press Enter to close the dashboard and stop the demo services.' || true
fi
