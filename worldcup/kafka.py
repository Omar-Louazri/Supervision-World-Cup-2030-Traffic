"""Bounded Kafka publishing with delivery accounting."""

from __future__ import annotations

import json
import sys
import time


class KafkaSink:
    def __init__(self, bootstrap_servers: str, *, stage_deadline: float | None = None):
        from confluent_kafka import KafkaError, Producer

        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "lz4",
                "linger.ms": 20,
                "batch.size": 256 * 1024,
                "batch.num.messages": 2048,
                "queue.buffering.max.messages": 50_000,
                "queue.buffering.max.kbytes": 16 * 1024,
                "delivery.timeout.ms": 30_000,
            }
        )
        self._produce_batch = getattr(self._producer, "produce_batch", None)
        self._queue_full = KafkaError._QUEUE_FULL
        self.stage_deadline = stage_deadline
        self.submitted = 0
        self.acknowledged = 0
        self.acknowledged_before_deadline = 0
        self.last_ack_monotonic: float | None = None
        self.failed = 0
        self.pending = 0

    def _delivered(self, error, _message) -> None:
        if error is not None:
            self.failed += 1
            if self.failed == 1:
                print(f"Kafka delivery failed: {error}", file=sys.stderr)
            return
        self.acknowledged += 1
        self.last_ack_monotonic = time.monotonic()
        if self.stage_deadline is None or self.last_ack_monotonic < self.stage_deadline:
            self.acknowledged_before_deadline += 1

    def wait_ready(self, timeout: float = 10) -> None:
        self._producer.list_topics(timeout=timeout)

    def poll(self, timeout: float = 0) -> int:
        return self._producer.poll(timeout)

    def _submit_one(self, topic: str, payload: bytes, key: bytes | None, deadline: float | None) -> bool:
        while deadline is None or time.monotonic() < deadline:
            try:
                self._producer.produce(topic, value=payload, key=key, on_delivery=self._delivered)
                self.submitted += 1
                return True
            except BufferError:
                self._wait_for_queue(deadline)
        return False

    def _wait_for_queue(self, deadline: float | None) -> None:
        timeout = 0.01 if deadline is None else max(0, min(0.01, deadline - time.monotonic()))
        self.poll(timeout)

    def send(self, topic: str, item: dict, *, key: str | None = None) -> None:
        payload = json.dumps(item, separators=(",", ":")).encode("utf-8")
        self._submit_one(topic, payload, key.encode("utf-8") if key is not None else None, None)
        self.poll()

    def send_payloads(self, topic: str, payloads: list[bytes], *, deadline: float | None = None) -> int:
        """Enqueue a bounded chunk and retry only messages rejected by the local queue."""
        if deadline is None:
            deadline = self.stage_deadline
        start_count = self.submitted
        if self._produce_batch is None:
            # Older supported confluent-kafka versions lack the bulk API.
            for payload in payloads:
                if not self._submit_one(topic, payload, None, deadline):
                    break
            self.poll()
            return self.submitted - start_count
        pending = [{"value": payload} for payload in payloads]
        while pending and (deadline is None or time.monotonic() < deadline):
            try:
                submitted = self._produce_batch(topic, pending, on_delivery=self._delivered)
            except BufferError:
                self._wait_for_queue(deadline)
                continue
            self.submitted += submitted
            if submitted == len(pending):
                break
            rejected = []
            for message in pending:
                error = message.pop("_error", None)
                if error is None:
                    continue
                if error.code() != self._queue_full:
                    raise RuntimeError(f"Kafka enqueue failed: {error}")
                rejected.append(message)
            if len(rejected) != len(pending) - submitted:
                raise RuntimeError("Kafka batch returned inconsistent enqueue counts")
            pending = rejected
            self._wait_for_queue(deadline)
        self.poll()
        return self.submitted - start_count

    def close(self) -> None:
        self._producer.flush(30)
        self.pending = self.submitted - self.acknowledged - self.failed
        if self.pending or self.failed:
            raise RuntimeError(f"Kafka delivery incomplete: {self.pending} pending, {self.failed} failed")
