from dataclasses import dataclass, field
import json
import os

import pytest

from scripts import load_test
from worldcup.benchmark import Checkpoints, SparkMetrics, latency, offset_delta
from worldcup.contracts import TOPICS


def test_checkpoint_counts_cutoff_and_preserves_offsets_across_rolling_retention(tmp_path):
    topic = next(iter(TOPICS.values()))
    directory = tmp_path / "crowd"
    (directory / "sources" / "0").mkdir(parents=True)
    (directory / "commits").mkdir()
    (directory / "offsets").mkdir()

    def write_offsets(path, timestamp, value):
        path.write_text("v1\n" + json.dumps({topic: {"0": value}, "hotels": {"0": 333}}))
        os.utime(path, (timestamp, timestamp))

    write_offsets(directory / "sources" / "0" / "0", 10, 5)
    write_offsets(directory / "offsets" / "0", 11, 12)
    write_offsets(directory / "offsets" / "1", 12, 14)
    # A planned offset may be present before Spark commits the batch.
    checkpoints = Checkpoints(directory)
    checkpoints.update()
    assert checkpoints.latest() == (10_000, {(topic, 0): 5})
    (directory / "commits" / "0").touch()
    os.utime(directory / "commits" / "0", (13, 13))
    checkpoints.update()
    assert checkpoints.latest() == (13_000, {(topic, 0): 12})
    assert checkpoints.latest(12_999) == (10_000, {(topic, 0): 5})
    assert offset_delta(checkpoints.latest()[1], checkpoints.latest(12_999)[1]) == 7
    with pytest.raises(RuntimeError, match="backward"):
        offset_delta(checkpoints.latest(12_999)[1], checkpoints.latest()[1])
    (directory / "commits" / "0").unlink()
    (directory / "offsets" / "0").unlink()
    checkpoints.update()
    assert checkpoints.latest() == (13_000, {(topic, 0): 12})


def test_metrics_read_complete_lines_and_correlate_only_first_visible_city_frame(tmp_path):
    path = tmp_path / "spark.jsonl"
    commit = {
        "kind": "redis_commit", "stream_id": "test", "event_count": 3,
        "processed_total": 3, "committed_at_ms": 1000,
        "latency_samples_ms": [100, 50, 40],
        "latency_samples_by_city_ms": {"Rabat": [100, 50], "Casablanca": [40]},
    }
    line = json.dumps(commit)
    path.write_text(line[:25])
    metrics = SparkMetrics(path, "test")
    metrics.update()
    assert metrics.commits(0) == []
    with path.open("a") as output:
        output.write(line[25:] + "\n" + json.dumps({**commit, "stream_id": "other"}) + "\n")
    metrics.update()
    metrics.update()
    assert metrics.commits(0) == [commit]
    frame = {"city": "Rabat", "available": True, "processed_total": 4, "latest_update_ms": 1000}
    result = latency([commit], [{**frame, "available": False, "received_at_ms": 1050},
                                {**frame, "received_at_ms": 1100}, {**frame, "received_at_ms": 1200}])
    assert result["kafka_to_redis_ms"] == {"samples": 3, "p50": 50, "p95": 95, "p99": 99}
    assert result["kafka_to_dashboard_ws_ms"] == {"samples": 2, "p50": 175, "p95": 197.5, "p99": 199.5}


@dataclass
class Clock:
    at: float = 0.0

    def wait(self, timeout):
        self.at += timeout

    def is_set(self):
        return False


@dataclass
class Sink:
    clock: Clock
    write_cost: float
    admissions: list = field(default_factory=list)

    def send_payloads(self, _topic, payloads, *, deadline):
        if self.clock.at < deadline:
            self.admissions.extend((self.clock.at, payload) for payload in payloads)
            self.clock.wait(self.write_cost)

    def poll(self, _timeout):
        pass

    def close(self):
        self.clock.wait(1.0)


@pytest.mark.parametrize(("write_cost", "expected_count"), [(0, 100), (0.55, 56)])
def test_stage_admission_has_a_fixed_deadline_even_if_producer_falls_behind(monkeypatch, write_cost, expected_count):
    clock = Clock()
    sink = Sink(clock, write_cost)
    monkeypatch.setattr(load_test.time, "monotonic", lambda: clock.at)
    topic = next(iter(TOPICS.values()))
    stats = {}
    load_test.submit_stage(sink, iter((topic, number) for number in range(100)), 100, 100, 1, 2, clock, stats)
    assert all(1 <= at < 2 for at, _ in sink.admissions)
    assert len(sink.admissions) == expected_count
    assert stats["flush_finished"] - stats["admission_finished"] == 1
    assert "error" not in stats
