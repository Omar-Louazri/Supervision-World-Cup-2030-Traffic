"""Prepare and run exact synthetic event stages on the local presentation stack."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from uuid import uuid4

from worldcup.config import Settings
from worldcup.contracts import HOTEL_TOPIC, TOPICS
from worldcup.mapbox import check_access
from worldcup.producer import crowd_events, hotel_snapshots
from worldcup.redis_store import PREFIX, recent_hotels
from worldcup.reference import cities_by_name, load_reference
from worldcup_event_producer import KafkaSink


STAGES = (1, 20, 100, 500)


def write_state(path: Path, state: dict) -> None:
    pending = path.with_suffix(".tmp")
    pending.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    pending.replace(path)


def prepare(path: Path) -> None:
    reference = load_reference(Settings().reference_path)
    rabat = cities_by_name(reference)["Rabat"].copy()
    rabat.update(
        venue="Stade de démonstration de Rabat",
        stadium_capacity_per_minute=250,
        transit_capacity_per_minute=340,
        site={
            "pitch": rabat["center"],
            "gates": [[34.02015, -6.83890], [34.01905, -6.84015], [34.02020, -6.84112]],
            "transit": [34.02027, -6.83608],
        },
    )
    reference["cities"] = [rabat]
    write_state(path, reference)


def warmup(settings: Settings, client, timeout: int) -> None:
    sink = KafkaSink(settings.kafka_bootstrap_servers)
    for snapshot in hotel_snapshots(load_reference(settings.reference_path), seed=2030, run_id=uuid4().hex):
        sink.send(HOTEL_TOPIC, snapshot.to_mapping())
    sink.close()
    deadline = time.monotonic() + timeout
    while not recent_hotels(client):
        if time.monotonic() >= deadline:
            raise RuntimeError("Spark did not publish the hotel warm-up to Redis.")
        time.sleep(1)


def run_stage(settings: Settings, client, path: Path, count: int, timeout: int) -> None:
    key = f"{PREFIX}:metrics:processed:{settings.stream_id}"
    baseline = int(client.get(key) or 0)
    start = time.monotonic()
    run_id = uuid4().hex
    state = dict(run_id=run_id, requested=count, published=0, processed=0, elapsed_s=0, status="publishing")
    write_state(path, state)
    try:
        reference = load_reference(settings.reference_path)
        events = crowd_events(
            reference, count=count, seed=2030, run_id=run_id,
            timestamp=datetime.now(timezone.utc),
        )
        points_by_city = {}
        sink = KafkaSink(settings.kafka_bootstrap_servers)
        for event in events:
            sink.send(TOPICS[event.source], event.to_mapping())
            points_by_city.setdefault(event.city, []).append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [event.lon, event.lat]},
                "properties": {"source": event.source},
            })
        sink.close()
        state.update(published=count, status="processing")
        print(f"Kafka acknowledged {count:,} / {count:,}. Waiting for Redis confirmation.", flush=True)
        deadline = time.monotonic() + timeout
        while True:
            state.update(processed=int(client.get(key) or 0) - baseline, elapsed_s=round(time.monotonic() - start, 1))
            if state["processed"] == count:
                # Release only the confirmed burst's synthetic locations to the presentation.
                write_state(path.with_name("passages.json"), {
                    city: {"type": "FeatureCollection", "features": features}
                    for city, features in points_by_city.items()
                })
                state["status"] = "complete"
                break
            if not 0 <= state["processed"] < count:
                raise RuntimeError(f"Unexpected Redis count delta {state['processed']}; check for other producers.")
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Timed out at {state['processed']} / {count} confirmed events.")
            write_state(path, state)
            time.sleep(0.5)
        print(f"Spark → Redis confirmed {count:,} / {count:,} in {state['elapsed_s']:.1f}s.", flush=True)
    except (Exception, KeyboardInterrupt):
        state.update(status="failed", elapsed_s=round(time.monotonic() - start, 1))
        raise
    finally:
        write_state(path, state)
        write_state(path.with_name(f"stage-{run_id}.json"), state)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("mapbox").add_argument("origin")
    actions.add_parser("ready").add_argument("websocket_url")
    actions.add_parser("prepare").add_argument("reference", type=Path)
    warmup_args = actions.add_parser("warmup")
    stage_args = actions.add_parser("stage")
    stage_args.add_argument("path", type=Path)
    stage_args.add_argument("count", type=int, choices=STAGES)
    for action in (warmup_args, stage_args):
        action.add_argument("--timeout", type=int, default=90)
    args = parser.parse_args()

    if args.action == "ready":
        from websockets.exceptions import WebSocketException
        from websockets.sync.client import connect

        try:
            with connect(args.websocket_url, open_timeout=2, close_timeout=1, proxy=None) as socket:
                config = json.loads(socket.recv(timeout=3))
                if config.get("type") != "config" or not config.get("presentation"):
                    return 1
                deadline = time.monotonic() + 3
                while (remaining := deadline - time.monotonic()) > 0:
                    frame = json.loads(socket.recv(timeout=remaining))
                    if frame.get("type") == "view":
                        return 0 if frame["available"] else 1
                return 1
        except (OSError, ValueError, WebSocketException):
            return 1

    if args.action == "prepare":
        prepare(args.reference)
        return 0
    settings = Settings.from_env()
    if args.action == "mapbox":
        ok, message = check_access(settings.mapbox_access_token, args.origin)
        print(message)
        return 0 if ok else 1
    if args.timeout < 1:
        parser.error("--timeout must be positive")

    import redis

    with redis.Redis.from_url(settings.redis_url, decode_responses=True, socket_connect_timeout=2, socket_timeout=2) as client:
        if args.action == "warmup":
            warmup(settings, client, args.timeout)
        else:
            run_stage(settings, client, args.path, args.count, args.timeout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
