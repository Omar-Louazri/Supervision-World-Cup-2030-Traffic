"""Measure fixed-rate Kafka → Spark → Redis stages on a dedicated local stack."""

from __future__ import annotations

import argparse
from contextlib import nullcontext
import json
import math
import os
from pathlib import Path
import platform
from threading import Event, Thread
import time
from uuid import uuid4

from worldcup.benchmark import (
    Checkpoints, DashboardProbe, HostMonitor, KafkaOffsets, SparkMetrics, cover_offsets,
    latency, offset_delta, percentile_summary, utc_text,
)
from worldcup.config import ROOT, Settings
from worldcup.contracts import TOPICS
from worldcup.reference import load_reference
from worldcup.redis_store import PREFIX

DEFAULT_RATES = [1000, 5000, 10000, 25000, 50000]
RATE_TOLERANCE = 0.95
PACING_INTERVAL_S = 0.01


def write_result(path: Path, result: dict) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def redis_count(client, stream_id: str) -> int:
    return int(client.get(f"{PREFIX}:metrics:processed:{stream_id}") or 0)


def redis_status(client) -> dict:
    info = client.info()
    return {key: info.get(key) for key in (
        "used_memory_rss", "instantaneous_ops_per_sec", "evicted_keys", "aof_enabled",
        "aof_buffer_length", "aof_pending_bio_fsync", "aof_delayed_fsync",
        "rdb_bgsave_in_progress", "aof_rewrite_in_progress", "redis_version",
    )}


def submit_stage(sink, events, rate: int, count: int, start: float, end: float, stop: Event, stats: dict) -> None:
    """Pace bounded groups of records and close admission at the monotonic deadline."""
    attempted = 0
    try:
        stop.wait(max(0, start - time.monotonic()))
        while not stop.is_set():
            now = time.monotonic()
            if now >= end:
                break
            budget = min(count, int((now - start + PACING_INTERVAL_S) * rate))
            if attempted >= budget:
                sink.poll(0)
                stop.wait(min(PACING_INTERVAL_S, end - now))
                continue
            records = {topic: [] for topic in TOPICS.values()}
            for _ in range(min(budget - attempted, 1000)):
                topic, payload = next(events)
                records[topic].append(payload)
                attempted += 1
            for topic, payloads in records.items():
                if not stop.is_set() and payloads:
                    sink.send_payloads(topic, payloads, deadline=end)
            sink.poll(0)
    except Exception as exc:
        stats["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        stats["admission_finished"] = time.monotonic()
        try:
            sink.close()
        except Exception as exc:
            stats["flush_error"] = f"{type(exc).__name__}: {exc}"
        stats["flush_finished"] = time.monotonic()


def checkpoint_ready(kafka, checkpoints, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        checkpoints.update()
        broker = kafka.latest()
        _, consumed = checkpoints.latest()
        if consumed == broker:
            return broker
        if time.monotonic() >= deadline:
            missing = "no matching crowd checkpoint" if not consumed else "crowd checkpoint has outstanding offsets"
            raise RuntimeError(f"Spark preflight failed ({missing}); start Spark with the same checkpoint and isolate benchmark traffic")
        time.sleep(min(0.5, max(0, deadline - time.monotonic())))


def stage_profile(batch_records: list[dict], commit_records: list[dict]) -> dict:
    duration_names = sorted({key for batch in batch_records for key in batch.get("duration_ms", {})})
    by_query = {}
    for query_name in sorted({batch["query_name"] for batch in batch_records}):
        batches = [batch for batch in batch_records
                   if batch["query_name"] == query_name and batch.get("num_input_rows", 0)]
        by_query[query_name] = {
            "input_rows": sum(batch["num_input_rows"] for batch in batches),
            "measured_batches": len(batches),
            "duration_ms": {name: percentile_summary(
                batch["duration_ms"][name] for batch in batches if name in batch.get("duration_ms", {})
            ) for name in duration_names},
        }
    sink = percentile_summary(commit["sink_ms"] for commit in commit_records if commit.get("sink_ms") is not None)
    return {"queries": by_query, "redis_sink_ms": sink}


def stage_safety(resources: dict, args) -> str | None:
    if args.min_disk_free_mib and min(resources["disk_free_mib"].values()) < args.min_disk_free_mib:
        return f"Free space fell below {args.min_disk_free_mib} MiB"
    available = resources["memory_available_mib"]
    if args.min_available_mib and available is not None and available < args.min_available_mib:
        return f"Available memory fell below {args.min_available_mib} MiB"
    return None


def run_stage(args, *, rate, duration, kind, settings, reference, client, kafka, checkpoints, metrics, host, probe):
    from worldcup.kafka import KafkaSink
    from worldcup.producer import crowd_payloads

    baseline = checkpoint_ready(kafka, checkpoints, args.preflight_timeout)
    processed_baseline = redis_count(client, settings.stream_id)
    initial_resources = host.snapshot()
    if reason := stage_safety(initial_resources, args):
        raise RuntimeError(reason)
    sink = KafkaSink(settings.kafka_bootstrap_servers)
    sink.wait_ready()
    run_id = uuid4().hex
    requested = rate * duration
    events = crowd_payloads(reference, count=requested, seed=args.seed, run_id=run_id)
    now = time.monotonic()
    wall_anchor = time.time()
    start = now + 0.05
    end = start + duration
    start_wall = wall_anchor + start - now
    started_ms = round(start_wall * 1000)
    boundary_ms = round((start_wall + duration) * 1000)
    drain_deadline_ms = boundary_ms + round(args.drain_timeout * 1000)
    sink.stage_deadline = end
    stats = {}
    stop = Event()
    thread = Thread(target=submit_stage, args=(sink, events, rate, requested, start, end, stop, stats), daemon=True)
    samples = []
    broker_final = None
    processed_final = 0
    consumed_final = 0
    source_drained = False
    status = "drain_timeout"
    stop_reason = None
    next_sample = start
    thread.start()
    try:
        while True:
            tick = time.monotonic()
            if tick < next_sample:
                time.sleep(next_sample - tick)

            producer_flushed = not thread.is_alive()
            processed = redis_count(client, settings.stream_id) - processed_baseline
            redis_observed_ms = time.time_ns() // 1_000_000
            checkpoints.update()
            metrics.update()
            source_ms, source = checkpoints.latest()
            broker_observation_started_ms = time.time_ns() // 1_000_000
            broker = kafka.latest()
            broker_observed_ms = time.time_ns() // 1_000_000
            host_usage = host.snapshot()
            consumed = offset_delta(source, baseline)
            broker_count = offset_delta(broker, baseline)
            acknowledged = sink.acknowledged
            sample = {
                "elapsed_s": round(time.monotonic() - start, 3),
                "redis_observed_ms": redis_observed_ms,
                "broker_observation_started_ms": broker_observation_started_ms,
                "broker_observed_ms": broker_observed_ms,
                "submitted": sink.submitted,
                "acknowledged": acknowledged,
                "broker_log_delta": broker_count,
                "spark_consumed": consumed,
                "redis_processed": processed,
                "kafka_lag": offset_delta(broker, source),
                "application_backlog": max(0, acknowledged - processed),
                "resources": host_usage,
                "redis": redis_status(client),
            }
            samples.append(sample)
            processed_final, consumed_final = processed, consumed
            if stop_reason is None:
                stop_reason = stage_safety(host_usage, args)
                if stop_reason:
                    stop.set()
            if producer_flushed:
                broker_final = broker
                source_drained = cover_offsets(source, broker_final)
                if stop_reason:
                    status = "resource_guard"
                    break
                if "error" in stats or "flush_error" in stats or sink.failed:
                    status = "producer_failed"
                    break
                if broker_count != acknowledged:
                    status = "unexpected_topic_traffic"
                    break
                if source_drained:
                    processed_final = redis_count(client, settings.stream_id) - processed_baseline
                    if processed_final != acknowledged:
                        status = "redis_count_mismatch"
                    elif source_ms <= drain_deadline_ms:
                        status = "drained"
                    break
            tick = time.monotonic()
            if tick >= end + args.drain_timeout:
                break
            next_sample = max(next_sample + args.sample_seconds, tick)
            if tick < end < next_sample:
                next_sample = end
            if not thread.is_alive():
                next_sample = min(next_sample, end + args.drain_timeout)
    finally:
        stop.set()
        thread.join(timeout=35)

    if thread.is_alive():
        raise RuntimeError("Kafka producer did not terminate after the bounded queue flush")
    if stop_reason:
        status = "resource_guard"
    drain_wait = round(max(0, time.monotonic() - end), 3)
    # Read telemetry once more so a completed callback in the last polling window is included.
    metrics.update()
    checkpoints.update()
    committed = metrics.commits(started_ms)
    on_time = [entry for entry in committed if entry["committed_at_ms"] <= boundary_ms]
    prior_samples = [sample for sample in samples if sample["redis_observed_ms"] <= boundary_ms]
    if metrics.path is None or not committed:
        processed_boundary = prior_samples[-1]["redis_processed"] if prior_samples else 0
        precise_redis_boundary = False
    else:
        processed_boundary = max((entry.get("processed_total") or processed_baseline for entry in on_time),
                                 default=processed_baseline) - processed_baseline
        precise_redis_boundary = True
    _, boundary_offsets = checkpoints.latest(boundary_ms)
    consumed_boundary = offset_delta(boundary_offsets, baseline)
    batch_records = metrics.batches(started_ms)
    committed_samples = [sample for sample in committed if sample["event_count"] > 0]
    complete_ms = max((entry["committed_at_ms"] for entry in committed_samples), default=None)
    source_completion_ms = source_ms if source_drained else None
    drained = status == "drained"
    if drained and complete_ms is None:
        complete_ms = next((sample["redis_observed_ms"] for sample in samples
                            if sample["redis_processed"] == sink.acknowledged), None)
    if probe is not None and drained and metrics.path is not None:
        deadline = time.monotonic() + args.dashboard_tail_timeout
        while time.monotonic() < deadline:
            if probe.frames and (probe.frames[-1]["processed_total"] or 0) >= processed_baseline + processed_final:
                break
            time.sleep(min(0.1, max(0, deadline - time.monotonic())))
    frames = [] if probe is None else [frame for frame in probe.frames if frame["received_at_ms"] >= started_ms]
    dashboard_errors = [] if probe is None else [error for error in probe.errors if error["at_ms"] >= started_ms]
    ack_in_window = sink.acknowledged_before_deadline
    backlog_boundary = max(0, ack_in_window - processed_boundary)
    before_boundary = [sample for sample in samples if sample["broker_observed_ms"] <= boundary_ms]
    after_boundary = [sample for sample in samples if sample["broker_observation_started_ms"] >= boundary_ms]
    broker_before = before_boundary[-1]["broker_log_delta"] if before_boundary else None
    broker_after = after_boundary[0]["broker_log_delta"] if after_boundary else None
    submitted_rate = sink.submitted / duration
    ack_rate = ack_in_window / duration
    processed_rate = processed_boundary / duration
    # Fixed-window rates exclude both producer flush and Spark drain time.
    passed = bool(drained and ack_rate >= rate * RATE_TOLERANCE and processed_rate >= rate * RATE_TOLERANCE)
    return {
        "kind": kind,
        "run_id": run_id,
        "status": status,
        "stop_reason": stop_reason,
        "started_at": utc_text(start_wall),
        "production_deadline": utc_text(start_wall + duration),
        "production_window_seconds": duration,
        "rate_requested": rate,
        "events_requested": requested,
        "producer": {
            "submitted": sink.submitted,
            "acknowledged_in_window": ack_in_window,
            "acknowledged_after_flush": sink.acknowledged,
            "failed": sink.failed,
            "last_ack_at": utc_text(wall_anchor + sink.last_ack_monotonic - now) if sink.last_ack_monotonic else None,
            "admission_finished_after_s": round(stats["admission_finished"] - start, 3),
            "flush_finished_after_s": round(stats["flush_finished"] - start, 3),
            "error": stats.get("error"),
            "flush_error": stats.get("flush_error"),
        },
        "submitted_per_second": round(submitted_rate, 3),
        "acknowledged_per_second": round(ack_rate, 3),
        "kafka_consumed_per_second": round(consumed_boundary / duration, 3),
        "sustained_processed_per_second": round(processed_rate, 3),
        "kafka_consumed_at_deadline": consumed_boundary,
        "processed_at_deadline": processed_boundary,
        "precise_redis_deadline": precise_redis_boundary,
        "application_backlog_at_deadline": backlog_boundary,
        "kafka_lag_acknowledged_at_deadline": max(0, ack_in_window - consumed_boundary),
        "kafka_lag_bounds_at_deadline": {
            "lower": max(0, max(broker_before or 0, ack_in_window) - consumed_boundary),
            "upper": max(0, broker_after - consumed_boundary) if broker_after is not None else None,
        },
        "producer_pending_at_deadline": sink.submitted - ack_in_window,
        "broker_log_delta_final": offset_delta(broker_final, baseline) if broker_final is not None else None,
        "kafka_consumed_final": consumed_final,
        "processed_after_drain": processed_final,
        "drained": drained,
        "sink_drain_seconds": round(max(0, complete_ms - boundary_ms) / 1000, 3) if drained and complete_ms else None,
        "sink_drain_observation": "commit_timestamp" if committed_samples else "first_poll_with_complete_count",
        "source_drain_seconds": round(max(0, source_completion_ms - boundary_ms) / 1000, 3) if source_completion_ms else None,
        "drain_wait_seconds": drain_wait,
        "remaining_application_backlog": max(0, sink.acknowledged - processed_final),
        "meets_requested_rate": passed,
        "latency": latency(committed, frames),
        "spark": stage_profile(batch_records, committed),
        "spark_batches": batch_records,
        "dashboard": None if probe is None else {"city": probe.city, "frames": frames, "errors": dashboard_errors},
        "resources_before": initial_resources,
        "samples": samples,
    }


def parse_pid(value: str) -> tuple[str, int]:
    name, separator, pid = value.partition("=")
    if not separator or not name or not pid.isdecimal() or int(pid) <= 0:
        raise argparse.ArgumentTypeError("Use name=PID with a positive process ID")
    return name, int(pid)


def main() -> int:
    import redis

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rates", type=int, nargs="+", default=DEFAULT_RATES)
    parser.add_argument("--duration", type=int, default=60, help="Production seconds per matrix stage")
    parser.add_argument("--stability", action="store_true", help="Add a 900-second stage at the highest passing rate")
    parser.add_argument("--stability-rate", type=int, help="Run stability at this rate instead of selecting a matrix result")
    parser.add_argument("--stability-duration", type=int, default=900)
    parser.add_argument("--stability-only", action="store_true", help="Run a stability stage without the rate matrix")
    parser.add_argument("--warmup-duration", type=int, default=15, help="Warm-up seconds at 1000 events/s before measurement (0 disables)")
    parser.add_argument("--drain-timeout", type=float, default=300)
    parser.add_argument("--preflight-timeout", type=float, default=30)
    parser.add_argument("--sample-seconds", type=float, default=1)
    parser.add_argument("--seed", type=int, default=2030)
    parser.add_argument("--spark-metrics", type=Path, default=os.getenv("SPARK_METRICS_PATH"))
    parser.add_argument("--legacy-spark", action="store_true", help="Allow a checkpoint-only baseline without Spark latency or timing")
    parser.add_argument("--dashboard-ws", help="Open a persistent client on this ws://.../ws endpoint and observe dashboard frames")
    parser.add_argument("--dashboard-city", help="City to subscribe to instead of the dashboard default")
    parser.add_argument("--dashboard-tail-timeout", type=float, default=5, help="Seconds to wait for dashboard visibility after drain")
    parser.add_argument("--watch-pid", type=parse_pid, action="append", default=[], metavar="NAME=PID")
    parser.add_argument("--watch-disk", type=Path, action="append", default=[], help="Additional disk paths to watch for free space")
    parser.add_argument("--min-disk-free-mib", type=int, default=1024, help="Stop safely below this much free disk space (0 disables)")
    parser.add_argument("--min-available-mib", type=int, default=768, help="Stop safely below this much available memory (0 disables)")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts" / "load-results.json")
    args = parser.parse_args()
    if min(*args.rates, args.duration, args.stability_duration, args.sample_seconds, args.preflight_timeout,
           args.stability_rate if args.stability_rate is not None else 1) <= 0:
        parser.error("rates, durations and sampling interval must be positive")
    if any(not math.isfinite(value) for value in (args.drain_timeout, args.preflight_timeout, args.sample_seconds,
                                                  args.dashboard_tail_timeout)):
        parser.error("sampling and timeout values must be finite")
    if min(args.drain_timeout, args.min_disk_free_mib, args.min_available_mib,
           args.dashboard_tail_timeout, args.warmup_duration) < 0:
        parser.error("drain timeout, warm-up duration and resource floors cannot be negative")
    if args.stability_only and args.stability_rate is None:
        parser.error("--stability-only needs --stability-rate")
    if args.dashboard_city and not args.dashboard_ws:
        parser.error("--dashboard-city needs --dashboard-ws")
    if args.spark_metrics is None and not args.legacy_spark:
        parser.error("set SPARK_METRICS_PATH/--spark-metrics or use --legacy-spark for a baseline")

    settings = Settings.from_env()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result = {
        "schema_version": 2,
        "started_at": utc_text(time.time()),
        "host": {"platform": platform.platform(), "python": platform.python_version(), "logical_cpus": os.cpu_count()},
        "configuration": {
            "kafka_bootstrap_servers": settings.kafka_bootstrap_servers,
            "stream_id": settings.stream_id,
            "spark_checkpoint_root": settings.spark_checkpoint_root,
            "spark_metrics_path": str(args.spark_metrics) if args.spark_metrics else None,
            "rates": [] if args.stability_only else args.rates,
            "duration_seconds": args.duration,
            "drain_timeout_seconds": args.drain_timeout,
            "sample_interval_seconds": args.sample_seconds,
            "seed": args.seed,
            "producer_acknowledgments": "all; idempotent",
            "stability_requested": bool(args.stability or args.stability_rate),
            "stability_rate_requested": args.stability_rate,
            "stability_duration_seconds": args.stability_duration,
            "warmup_duration_seconds": args.warmup_duration,
            "h3_resolution": settings.h3_resolution,
            "window_duration": settings.window_duration,
            "window_slide": settings.window_slide,
            "watermark_delay": settings.watermark_delay,
            "redis_ttl_seconds": settings.redis_ttl_seconds,
            "rate_threshold_fraction": RATE_TOLERANCE,
        },
        "methodology": [
            "Each rate uses a monotonic admission deadline. Acknowledgments are timed when producer callbacks report success. Flush has its own timeline.",
            "Consumed counts use committed Spark Kafka offsets for the two crowd topics. Redis processed counts represent valid unique crowd events.",
            "An isolated Kafka and Spark/Redis run is required. Settled broker offset delta must equal successful producer acknowledgments.",
            "Drain is measured from the fixed production deadline to the last Redis sink commit and Spark checkpoint commit separately.",
            "Kafka lag at the exact deadline is bounded by successful acknowledgment callbacks and broker offset observations on either side of the deadline.",
            "Kafka-to-dashboard latency uses sampled events from the subscribed city and the first received aggregate snapshot covering their Redis commit; browser rendering is outside this measurement.",
            "The pass threshold is 95% of offered rate for both acknowledgments and Redis commits inside the window, with complete drain and count reconciliation.",
        ],
        "status": "preflight",
        "stages": [],
    }
    write_result(args.output, result)
    client = redis.Redis.from_url(settings.redis_url, decode_responses=True, socket_connect_timeout=2, socket_timeout=2)
    try:
        client.ping()
        result["configuration"]["redis"] = redis_status(client)
        kafka = KafkaOffsets(settings.kafka_bootstrap_servers)
        result["configuration"]["kafka_partitions"] = len(kafka.partitions)
        checkpoint_path = Path(settings.spark_checkpoint_root) / "crowd"
        checkpoints = Checkpoints(checkpoint_path)
        checkpoint_ready(kafka, checkpoints, args.preflight_timeout)
        metrics = SparkMetrics(args.spark_metrics, settings.stream_id)
        metrics.update()
        if args.spark_metrics is not None and not metrics.records:
            raise RuntimeError("No Spark metrics were found for this stream; set SPARK_METRICS_PATH when launching Spark")
        reference = load_reference(settings.reference_path)
        host = HostMonitor(dict(args.watch_pid), list(dict.fromkeys([args.output.parent, checkpoint_path, *args.watch_disk])))
        with DashboardProbe(args.dashboard_ws, args.dashboard_city) if args.dashboard_ws else nullcontext() as probe:
            result["status"] = "running"
            stage_options = dict(settings=settings, reference=reference, client=client, kafka=kafka,
                                 checkpoints=checkpoints, metrics=metrics, host=host, probe=probe)

            def record(rate: int, duration: int, kind: str) -> bool:
                stage = run_stage(args, rate=rate, duration=duration, kind=kind, **stage_options)
                if kind == "warmup":
                    result["warmup"] = stage
                else:
                    result["stages"].append(stage)
                write_result(args.output, result)
                print(f"{kind} {rate}/s over {duration}s: "
                      f"acked {stage['acknowledged_per_second']:.1f}/s, "
                      f"consumed {stage['kafka_consumed_per_second']:.1f}/s, "
                      f"processed {stage['sustained_processed_per_second']:.1f}/s, "
                      f"status {stage['status']}", flush=True)
                return stage["drained"]

            clean = True
            if args.warmup_duration:
                clean = record(1000, args.warmup_duration, "warmup")
            for rate in [] if args.stability_only or not clean else args.rates:
                if not record(rate, args.duration, "matrix"):
                    clean = False
                    break
            if args.stability or args.stability_rate is not None:
                passing_rate = max((stage["rate_requested"] for stage in result["stages"] if stage["meets_requested_rate"]), default=None)
                stability_rate = args.stability_rate or passing_rate
                if clean and stability_rate is None:
                    stability_rate = min(stage["rate_requested"] for stage in result["stages"])
                    result["stability_selection"] = "Lowest tested rate because no matrix stage met the pass threshold"
                if clean:
                    clean = record(stability_rate, args.stability_duration, "stability")
                else:
                    result["stability_skipped"] = "Prior stage did not reconcile"
            result["status"] = "completed" if clean else "incomplete"
    except KeyboardInterrupt:
        result["status"] = "interrupted"
    except Exception as exc:
        result.update(status="preflight_failed" if result["status"] == "preflight" else "failed",
                      error=f"{type(exc).__name__}: {exc}")
        print(result["error"], flush=True)
    finally:
        client.close()
        result["finished_at"] = utc_text(time.time())
        write_result(args.output, result)
    print(f"Wrote {args.output}", flush=True)
    return 0 if result["status"] == "completed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
