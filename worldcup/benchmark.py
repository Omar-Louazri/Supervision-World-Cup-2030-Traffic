"""Measurement helpers for the local streaming benchmark."""

from __future__ import annotations

from bisect import bisect_left
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
from threading import Event, Thread
import time

from .contracts import TOPICS


def utc_text(seconds: float) -> str:
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def percentile_summary(values) -> dict:
    values = sorted(values)
    if not values:
        return {"samples": 0, "p50": None, "p95": None, "p99": None}

    def percentile(fraction: float) -> float:
        position = (len(values) - 1) * fraction
        index = int(position)
        next_index = min(index + 1, len(values) - 1)
        return round(values[index] + (values[next_index] - values[index]) * (position - index), 3)

    return {"samples": len(values), "p50": percentile(0.50), "p95": percentile(0.95), "p99": percentile(0.99)}


def offset_delta(target: dict, previous: dict) -> int:
    if target.keys() != previous.keys():
        raise RuntimeError("Kafka partition set changed or Spark has not discovered all benchmark partitions")
    deltas = [end - previous[partition] for partition, end in target.items()]
    if any(delta < 0 for delta in deltas):
        raise RuntimeError("Kafka or Spark offsets moved backward")
    return sum(deltas)


def cover_offsets(consumed: dict, target: dict) -> bool:
    return consumed.keys() == target.keys() and all(consumed[p] >= end for p, end in target.items())


class KafkaOffsets:
    def __init__(self, bootstrap_servers: str):
        from confluent_kafka import TopicPartition
        from confluent_kafka.admin import AdminClient

        self.admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        topics = self.admin.list_topics(timeout=5).topics
        self.partitions = []
        for name in TOPICS.values():
            if name not in topics or topics[name].error:
                raise RuntimeError(f"Create Kafka topic {name} before running a benchmark")
            self.partitions.extend(TopicPartition(name, p) for p in topics[name].partitions)

    def latest(self) -> dict:
        from confluent_kafka.admin import OffsetSpec

        futures = self.admin.list_offsets(
            {partition: OffsetSpec.latest() for partition in self.partitions}, request_timeout=3
        )
        return {(p.topic, p.partition): future.result().offset for p, future in futures.items()}


def _offsets(path: Path) -> dict:
    values = json.loads(path.read_text(encoding="utf-8").splitlines()[-1])
    return {
        (topic, int(partition)): int(offset)
        for topic, partitions in values.items()
        if topic in TOPICS.values()
        for partition, offset in partitions.items()
    }


class Checkpoints:
    """Use Spark commit logs instead of Kafka consumer-group counters."""

    def __init__(self, path: Path):
        self.path = path
        self.entries: dict[int, tuple[int, dict]] = {}

    def update(self) -> None:
        directory = self.path / "commits"
        if directory.is_dir():
            for path in directory.iterdir():
                if not path.name.isdecimal():
                    continue
                self._load_commit(path)
        if -1 not in self.entries:
            initial = self.path / "sources" / "0" / "0"
            try:
                self.entries[-1] = (initial.stat().st_mtime_ns // 1_000_000, _offsets(initial))
            except FileNotFoundError:
                pass

    def _load_commit(self, path: Path) -> None:
        batch_id = int(path.name)
        if batch_id in self.entries:
            return
        try:
            # Spark renames an atomic temporary file after foreachBatch returns.
            committed_ms = path.stat().st_mtime_ns // 1_000_000
            self.entries[batch_id] = (committed_ms, _offsets(self.path / "offsets" / path.name))
        except FileNotFoundError:
            # Spark can concurrently delete old entries according to its retention.
            return

    def latest(self, cutoff_ms: int | None = None) -> tuple[int | None, dict]:
        eligible = ((batch_id, entry) for batch_id, entry in self.entries.items()
                    if cutoff_ms is None or entry[0] <= cutoff_ms)
        return max(eligible, default=(None, (None, {})))[1]


class SparkMetrics:
    def __init__(self, path: Path | None, stream_id: str):
        self.path = path
        self.stream_id = stream_id
        self.position = 0
        self.records: list[dict] = []

    def update(self) -> None:
        if self.path is None or not self.path.exists():
            return
        with self.path.open("rb") as source:
            if source.seek(0, os.SEEK_END) < self.position:
                raise RuntimeError("Spark metrics log was truncated during the benchmark")
            source.seek(self.position)
            while line := source.readline():
                if not line.endswith(b"\n"):
                    return
                self.position = source.tell()
                entry = json.loads(line)
                if entry.get("stream_id") in (None, self.stream_id):
                    self.records.append(entry)

    def commits(self, since_ms: int) -> list[dict]:
        return [entry for entry in self.records
                if entry.get("kind") == "redis_commit" and entry.get("committed_at_ms", 0) >= since_ms]

    def batches(self, since_ms: int) -> list[dict]:
        return [entry for entry in self.records if entry.get("kind") == "spark_batch"
                and entry.get("completed_at_ms", 0) >= since_ms]


def latency(commit_records: list[dict], frames: list[dict]) -> dict:
    redis_samples = []
    websocket_samples = []
    eligible_dashboard_samples = 0
    visible_frames = [frame for frame in frames if frame["processed_total"] is not None and frame["available"]]
    # Ignore retransmissions and frames referring to an equal or older snapshot.
    ordered = []
    for frame in visible_frames:
        if not ordered or frame["processed_total"] > ordered[-1]["processed_total"]:
            ordered.append(frame)
    totals = [frame["processed_total"] for frame in ordered]
    for commit in commit_records:
        city_samples = commit.get("latency_samples_by_city_ms", {})
        all_samples = commit.get("latency_samples_ms")
        if all_samples is None:
            all_samples = [value for values in city_samples.values() for value in values]
        redis_samples.extend(all_samples)
        if not city_samples:
            continue
        if frames:
            eligible_dashboard_samples += len(city_samples.get(frames[0]["city"], []))
        total = commit.get("processed_total")
        if total is None:
            continue
        index = bisect_left(totals, total)
        if index == len(ordered):
            continue
        frame = ordered[index]
        for value in city_samples.get(frame["city"], []):
            created_ms = commit["committed_at_ms"] - value
            update_ms = frame.get("latest_update_ms")
            # Dashboard views omit old aggregate windows. Exclude events older
            # than the 180 s window that may no longer be queryable at receipt.
            visible_age_ms = frame["received_at_ms"] - created_ms
            if update_ms is not None and created_ms <= update_ms + 1000 and value <= visible_age_ms <= 180_000:
                websocket_samples.append(visible_age_ms)
    return {
        "kafka_to_redis_ms": percentile_summary(redis_samples),
        "kafka_to_dashboard_ws_ms": percentile_summary(websocket_samples),
        "dashboard_city_samples_excluded": eligible_dashboard_samples - len(websocket_samples),
    }


class DashboardProbe:
    def __init__(self, url: str, city: str | None):
        self.url = url
        self.city = city
        self.frames: list[dict] = []
        self.errors: list[dict] = []
        self.ready = Event()
        self.stopped = Event()
        self.thread = Thread(target=self._listen, daemon=True)

    def __enter__(self):
        self.thread.start()
        if not self.ready.wait(7):
            self.__exit__()
            raise RuntimeError(f"No usable WebSocket view frame from {self.url}")
        return self

    def __exit__(self, *_args):
        self.stopped.set()
        self.thread.join(timeout=4)

    def _listen(self) -> None:
        from websockets.exceptions import WebSocketException
        from websockets.sync.client import connect

        while not self.stopped.is_set():
            try:
                with connect(self.url, open_timeout=3, close_timeout=1) as socket:
                    while not self.stopped.is_set():
                        try:
                            frame = json.loads(socket.recv(timeout=0.5))
                        except TimeoutError:
                            continue
                        if frame.get("type") == "config":
                            self.city = self.city or frame["city"]
                            socket.send(json.dumps({"type": "subscribe", "city": self.city}))
                        if frame.get("type") != "view" or frame["city"] != self.city:
                            continue
                        view = frame["view"]
                        updated = view.get("latest_update")
                        self.frames.append({
                            "received_at_ms": time.time_ns() // 1_000_000,
                            "available": frame["available"],
                            "city": frame["city"],
                            "processed_total": frame.get("benchmark_processed_total"),
                            "latest_update_ms": int(datetime.fromisoformat(updated.replace("Z", "+00:00")).timestamp() * 1000) if updated else None,
                            "recommendations": len(view["recommendations"]),
                        })
                        if frame["available"]:
                            self.ready.set()
            except (OSError, ValueError, WebSocketException) as exc:
                self.errors.append({"at_ms": time.time_ns() // 1_000_000, "error": str(exc)})
                self.stopped.wait(1)


class HostMonitor:
    """Measure actual host use and optional process trees on Linux."""

    def __init__(self, pids: dict[str, int], disk_paths: list[Path]):
        self.pids = pids
        self.disk_paths = disk_paths
        self.previous_cpu = self._cpu()
        self.previous_services = self._services()
        self.previous_time = time.monotonic()

    @staticmethod
    def _cpu() -> tuple[int, int, int] | None:
        try:
            ticks = [int(value) for value in Path("/proc/stat").read_text().splitlines()[0].split()[1:9]]
        except FileNotFoundError:
            return None
        return sum(ticks), ticks[3], ticks[4]

    @staticmethod
    def _proc_values(path: str, keys: tuple[str, ...]) -> dict:
        try:
            lines = Path(path).read_text().splitlines()
        except FileNotFoundError:
            return {}
        values = {}
        for line in lines:
            key, value, *_ = line.split()
            key = key.rstrip(":")
            if key in keys:
                values[key] = int(value)
        return values

    def _services(self) -> dict:
        if not self.pids:
            return {}
        if not Path("/proc").exists():
            return {}
        processes = {}
        children = {}
        output = subprocess.run(["ps", "-eo", "pid=,ppid=,rss="], capture_output=True, text=True, check=True)
        for row in output.stdout.splitlines():
            pid, parent, rss = (int(value) for value in row.split())
            processes[pid] = rss
            children.setdefault(parent, []).append(pid)
        services = {}
        for name, root in self.pids.items():
            active = [root]
            for pid in active:
                active.extend(children.get(pid, []))
            cpu = {}
            for pid in active:
                try:
                    stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
                    cpu[pid] = int(stat[11]) + int(stat[12])
                except (FileNotFoundError, ProcessLookupError):
                    continue
            services[name] = (sum(processes.get(pid, 0) for pid in cpu), cpu)
        return services

    def snapshot(self) -> dict:
        current = self._cpu()
        now = time.monotonic()
        cpu_percent = None
        iowait_percent = None
        if current and self.previous_cpu:
            total, idle, iowait = (new - old for new, old in zip(current, self.previous_cpu))
            if total:
                cpu_percent = round((total - idle - iowait) * 100 / total, 2)
                iowait_percent = round(iowait * 100 / total, 2)
        self.previous_cpu = current
        memory = self._proc_values("/proc/meminfo", ("MemTotal", "MemAvailable", "SwapTotal", "SwapFree"))
        swap = self._proc_values("/proc/vmstat", ("pswpin", "pswpout"))
        services = self._services()
        elapsed = now - self.previous_time
        clock_ticks = os.sysconf("SC_CLK_TCK") if services else None
        utilization = {}
        for name, (rss, cpu) in services.items():
            previous = self.previous_services[name][1]
            elapsed_ticks = sum(max(0, count - previous.get(pid, 0)) for pid, count in cpu.items())
            utilization[name] = {"rss_mib": round(rss / 1024, 2),
                                 "cpu_percent_of_one_thread": round(elapsed_ticks / clock_ticks / elapsed * 100, 2)}
        self.previous_services, self.previous_time = services, now
        return {
            "cpu_percent_all_logical_cores": cpu_percent,
            "iowait_percent_all_logical_cores": iowait_percent,
            "memory_available_mib": memory.get("MemAvailable", 0) // 1024 if memory else None,
            "memory_used_mib": (memory["MemTotal"] - memory["MemAvailable"]) // 1024 if memory else None,
            "swap_used_mib": (memory["SwapTotal"] - memory["SwapFree"]) // 1024 if memory else None,
            "swap_in_pages": swap.get("pswpin"),
            "swap_out_pages": swap.get("pswpout"),
            "disk_free_mib": {str(path): shutil.disk_usage(path).free // 2**20 for path in self.disk_paths},
            "services": utilization,
        }
