"""Publish synthetic stadium, transit and hotel events to Kafka or stdout."""

from __future__ import annotations

import argparse
from itertools import islice
import json
import math
import sys
import time
from uuid import uuid4

from worldcup.config import Settings
from worldcup.contracts import HOTEL_TOPIC, TOPICS
from worldcup.kafka import KafkaSink
from worldcup.producer import crowd_events, crowd_payloads, hotel_snapshots
from worldcup.reference import load_reference


class StdoutSink:
    def send(self, topic: str, item: dict, *, key: str | None = None) -> None:
        print(json.dumps({"topic": topic, "value": item, **({"key": key} if key is not None else {})}, ensure_ascii=False))

    def close(self) -> None:
        return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sink", choices=("kafka", "stdout"), default="kafka")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--rate", type=float, default=1000, help="Target crowd events per second")
    parser.add_argument("--seed", type=int, default=2030)
    parser.add_argument("--run-id", default=None, help="Stable ID for a reproducible run")
    parser.add_argument("--without-hotels", action="store_true")
    parser.add_argument("--hotel-interval", type=float, default=60, help="Seconds between synthetic hotel snapshots")
    args = parser.parse_args(argv)
    if args.count < 0 or any(not math.isfinite(value) or value <= 0 for value in (args.rate, args.hotel_interval)):
        parser.error("Count must be nonnegative and rate and hotel interval must be finite and positive")

    settings = Settings.from_env()
    reference = load_reference(settings.reference_path)
    run_id = args.run_id or uuid4().hex
    sink = KafkaSink(settings.kafka_bootstrap_servers) if args.sink == "kafka" else StdoutSink()
    start = time.monotonic()
    sent = 0
    hotels_sent = 0
    last_hotel_at = start
    hotel_round = 0
    batch_interval = 0.02 if args.without_hotels else min(0.02, args.hotel_interval)
    chunk_size = min(1024, max(1, int(args.rate * batch_interval))) if args.sink == "kafka" else 1
    generation = dict(count=args.count, seed=args.seed, run_id=run_id)
    events = crowd_payloads(reference, **generation) if args.sink == "kafka" else crowd_events(reference, **generation)
    try:
        if not args.without_hotels:
            for snapshot in hotel_snapshots(reference, seed=args.seed, run_id=run_id):
                sink.send(HOTEL_TOPIC, snapshot.to_mapping())
                hotels_sent += 1
        while sent < args.count:
            target = sent / args.rate
            delay = target - (time.monotonic() - start)
            if delay > 0:
                time.sleep(delay)
            if not args.without_hotels and time.monotonic() - last_hotel_at >= args.hotel_interval:
                hotel_round += 1
                for snapshot in hotel_snapshots(reference, seed=args.seed + hotel_round, run_id=f"{run_id}-h{hotel_round}"):
                    sink.send(HOTEL_TOPIC, snapshot.to_mapping())
                    hotels_sent += 1
                last_hotel_at = time.monotonic()
            if args.sink == "kafka":
                by_topic: dict[str, list[bytes]] = {}
                for topic, payload in islice(events, chunk_size):
                    by_topic.setdefault(topic, []).append(payload)
                sent += sum(sink.send_payloads(topic, payloads) for topic, payloads in by_topic.items())
            else:
                event = next(events)
                sink.send(TOPICS[event.source], event.to_mapping())
                sent += 1
    finally:
        sink.close()
    elapsed = max(time.monotonic() - start, 1e-9)
    summary = {"run_id": run_id, "sent": sent, "elapsed_s": elapsed, "observed_rate": sent / elapsed}
    if args.sink == "kafka":
        summary.update(acknowledged=sink.acknowledged - hotels_sent, hotel_acknowledged=hotels_sent)
    print(json.dumps(summary), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
