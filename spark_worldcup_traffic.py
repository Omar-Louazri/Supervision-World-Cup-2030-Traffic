"""Kafka -> Spark Structured Streaming -> H3 aggregates and Redis."""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
import threading
import time

from worldcup.config import Settings
from worldcup.contracts import HOTEL_TOPIC, INVALID_TOPIC, TOPICS
from worldcup.reference import capacity_for, load_reference
from worldcup.redis_store import index_key, mark_processed_batch, write_aggregates, write_hotel
from worldcup.spatial import latlon_to_h3


def crowd_schema():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("event_id", T.StringType()),
            T.StructField("schema_version", T.IntegerType()),
            T.StructField("source", T.StringType()),
            T.StructField("city", T.StringType()),
            T.StructField("venue", T.StringType()),
            T.StructField("lat", T.DoubleType()),
            T.StructField("lon", T.DoubleType()),
            T.StructField("timestamp", T.StringType()),
            T.StructField("action", T.StringType()),
            T.StructField("fan_id", T.StringType()),
        ]
    )


def crowd_rejection(event, event_time, reference):
    from pyspark.sql import functions as F

    e = event.getField
    return (
        F.when(event.isNull(), "invalid_json")
        .when(e("schema_version").isNull() | (e("schema_version") != 1), "schema_version")
        .when(e("event_id").isNull() | (F.length(F.trim(e("event_id"))) == 0), "event_id")
        .when(e("city").isNull() | ~e("city").isin(*[city["name"] for city in reference["cities"]]), "city")
        .when(e("source").isNull() | ~e("source").isin(*TOPICS.keys()), "source")
        .when(
            ((F.col("topic") == TOPICS["stadium_access"]) & (e("source") != "stadium_access"))
            | ((F.col("topic") == TOPICS["transit_validation"]) & (e("source") != "transit_validation")),
            "topic_source_mismatch",
        )
        .when(
            ((e("source") == "stadium_access") & (e("action") != "gate_checkin"))
            | ((e("source") == "transit_validation") & (e("action") != "transit_validation"))
            | e("action").isNull(),
            "action",
        )
        .when(
            (e("source") == "stadium_access")
            & (e("venue").isNull() | (F.length(F.trim(e("venue"))) == 0)),
            "venue",
        )
        .when(
            e("lat").isNull() | e("lon").isNull() | F.isnan(e("lat")) | F.isnan(e("lon"))
            | (e("lat") < -90) | (e("lat") > 90) | (e("lon") < -180) | (e("lon") > 180),
            "coordinates",
        )
        .when(
            e("timestamp").isNull()
            | ~e("timestamp").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
            | event_time.isNull(),
            "timestamp",
        )
    )


def _event_time(timestamp):
    from pyspark.sql import functions as F

    # ANSI mode is enabled by default in Spark 4. Invalid timestamp strings
    # need to enter the rejection stream, even when a parse throws in ANSI.
    return F.try_to_timestamp(timestamp, F.lit("yyyy-MM-dd'T'HH:mm:ssX"))


def crowd_frames(raw, reference: dict, h3_resolution: int, *, include_h3=True, monitor_latency=False):
    """Return validated crowd events and reason-coded invalid records."""
    from pyspark.sql import functions as F

    event = F.col("event")
    e = event.getField
    frame = (
        raw.filter(F.col("topic").isin(*TOPICS.values()))
        .withColumn("event", F.from_json(F.col("value").cast("string"), crowd_schema()))
        .withColumn("event_time", _event_time(e("timestamp")))
    )
    reason = crowd_rejection(event, F.col("event_time"), reference)
    frame = frame.withColumn("invalid_reason", reason)
    invalid = frame.filter(F.col("invalid_reason").isNotNull()).select(
        "topic", "partition", "offset", "invalid_reason", e("event_id").alias("event_id")
    )

    valid = frame.filter(F.col("invalid_reason").isNull()).select(
        e("event_id").alias("event_id"),
        e("city").alias("city"),
        e("venue").alias("venue"),
        e("source").alias("source"),
        e("lat").alias("lat"),
        e("lon").alias("lon"),
        "event_time",
        *([F.unix_millis("timestamp").alias("kafka_timestamp_ms")] if monitor_latency else []),
    )
    if include_h3:
        valid = _with_h3(valid, h3_resolution)
    return valid, invalid


def _with_h3(frame, resolution: int):
    from pyspark.sql import functions as F, types as T

    h3_udf = F.udf(lambda lat, lon: latlon_to_h3(lat, lon, resolution), T.StringType())
    return frame.withColumn("h3_cell", h3_udf("lat", "lon")).drop("lat", "lon")


def hotel_schema():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("snapshot_id", T.StringType()),
            T.StructField("schema_version", T.IntegerType()),
            T.StructField("source_id", T.StringType()),
            T.StructField("city", T.StringType()),
            T.StructField("area_id", T.StringType()),
            T.StructField("timestamp", T.StringType()),
            T.StructField("rooms_available", T.IntegerType()),
            T.StructField("rooms_occupied", T.IntegerType()),
        ]
    )


def hotel_rejection(event, reference):
    from pyspark.sql import functions as F

    e = event.getField
    required = ["snapshot_id", "source_id", "city", "area_id", "timestamp"]
    missing = F.lit(False)
    for field in required:
        missing = missing | e(field).isNull() | (F.length(F.trim(e(field))) == 0)
    return (
        F.when(event.isNull(), "invalid_json")
        .when(e("schema_version").isNull() | (e("schema_version") != 1), "schema_version")
        .when(missing, "required_field")
        .when(~e("city").isin(*[city["name"] for city in reference["cities"]]), "city")
        .when(
            ~e("timestamp").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
            | _event_time(e("timestamp")).isNull(),
            "timestamp",
        )
        .when(
            e("rooms_available").isNull() | e("rooms_occupied").isNull()
            | (e("rooms_available") < 0) | (e("rooms_occupied") < 0)
            | ((e("rooms_available").cast("long") + e("rooms_occupied")) <= 0),
            "rooms",
        )
    )


def hotel_frames(raw, reference: dict):
    from pyspark.sql import functions as F

    event = F.col("event")
    e = event.getField
    frame = raw.filter(F.col("topic") == HOTEL_TOPIC).withColumn(
        "event", F.from_json(F.col("value").cast("string"), hotel_schema())
    )
    reason = hotel_rejection(event, reference)
    frame = frame.withColumn("invalid_reason", reason)
    invalid = frame.filter(F.col("invalid_reason").isNotNull()).select(
        "topic", "partition", "offset", "invalid_reason", e("snapshot_id").alias("event_id")
    )
    valid = frame.filter(F.col("invalid_reason").isNull()).select(
        *[e(field).alias(field) for field in ("snapshot_id", "source_id", "city", "area_id", "timestamp", "rooms_available", "rooms_occupied")]
    )
    return valid, invalid


def reject_records(raw, reference: dict):
    """Classify both record types in one Kafka scan."""
    from pyspark.sql import functions as F

    value = F.col("value").cast("string")
    crowd = F.from_json(value, crowd_schema())
    hotel = F.from_json(value, hotel_schema())
    rejection = F.when(
        F.col("topic").isin(*TOPICS.values()),
        F.struct(
            crowd_rejection(crowd, _event_time(crowd["timestamp"]), reference).alias("invalid_reason"),
            crowd["event_id"].alias("event_id"),
        ),
    ).when(
        F.col("topic") == HOTEL_TOPIC,
        F.struct(hotel_rejection(hotel, reference).alias("invalid_reason"), hotel["snapshot_id"].alias("event_id")),
    )
    return (
        raw.select("topic", "partition", "offset", rejection.alias("rejection"))
        .filter(F.col("rejection.invalid_reason").isNotNull())
        .select("topic", "partition", "offset", "rejection.*")
    )


def crowd_aggregates(batch, settings: Settings):
    from pyspark.sql import functions as F

    if "h3_cell" not in batch.columns:
        batch = _with_h3(batch, settings.h3_resolution)
    dimensions = ["city", "venue", "source"]
    # The contract has one-second timestamps. Combine equal event times and
    # cells once before expanding sliding windows and rolling up the venues.
    counts = (
        batch.withColumn("venue", F.coalesce("venue", F.lit("")))
        .groupBy(*dimensions, "h3_cell", "event_time")
        .count()
        .withColumn("window", F.window("event_time", settings.window_duration, settings.window_slide))
    )
    return (
        counts.groupingSets(
            [(*dimensions, "window", "h3_cell"), (*dimensions, "window")],
            *dimensions, "window", "h3_cell",
        )
        .agg(F.sum("count").alias("event_count"))
        .withColumn("kind", F.when(F.col("h3_cell").isNull(), "venue").otherwise("cell"))
        .withColumn("h3_cell", F.coalesce("h3_cell", F.lit("")))
    )


def save_crowd_batch(batch, batch_id: int, settings: Settings, reference: dict, metrics=None) -> None:
    """Aggregate a static micro-batch, then apply each delta once in Redis."""
    batch.persist()
    try:
        _save_crowd_batch(batch, batch_id, settings, reference, metrics)
    finally:
        batch.unpersist()


def _save_crowd_batch(batch, batch_id: int, settings: Settings, reference: dict, metrics) -> None:
    import redis
    from pyspark.sql import functions as F

    started_ns = time.monotonic_ns()
    observations = [F.count(F.lit(1)).alias("events")]
    if metrics and "kafka_timestamp_ms" in batch.columns:
        observations.append(
            F.collect_list(
                F.when(
                    F.col("kafka_timestamp_ms").isNotNull() & (F.pmod(F.xxhash64("event_id"), F.lit(256)) == 0),
                    F.struct("city", "kafka_timestamp_ms"),
                )
            ).alias("latency_sample")
        )
    observed = batch.agg(*observations).first().asDict()
    event_count = observed["events"]
    if not event_count:
        return
    rows = crowd_aggregates(batch, settings)
    capacities = {
        (city["name"], city["venue"], source): capacity_for(reference, city["name"], source)
        for city in reference["cities"] for source in TOPICS
    }
    redis_url = settings.redis_url
    stream_id = settings.stream_id
    ttl_seconds = settings.redis_ttl_seconds

    def write_partition(partition) -> None:
        aggregates = (
            dict(
                stream_id=stream_id,
                batch_id=batch_id,
                kind=row.kind,
                city=row.city,
                venue=row.venue,
                source=row.source,
                h3_cell=row.h3_cell,
                window_start=row.window.start,
                window_end=row.window.end,
                delta=int(row.event_count),
                capacity=capacities.get((row.city, row.venue, row.source)) if row.kind == "venue" else None,
                ttl_seconds=ttl_seconds,
            )
            for row in partition
        )
        with redis.Redis.from_url(redis_url, decode_responses=True) as client:
            write_aggregates(client, aggregates)

    rows.foreachPartition(write_partition)
    with redis.Redis.from_url(redis_url, decode_responses=True) as client:
        with client.pipeline(transaction=False) as pipeline:
            cutoff = time.time() - ttl_seconds
            for kind in ("cell", "venue"):
                pipeline.zremrangebyscore(index_key(kind), "-inf", cutoff)
            pipeline.execute()
        processed_total = mark_processed_batch(
            client, stream_id=stream_id, batch_id=batch_id, count=event_count, ttl_seconds=ttl_seconds,
        )
    if metrics and processed_total:
        committed_at_ms = time.time_ns() // 1_000_000
        latencies_by_city: dict[str, list[int]] = {}
        for sample in observed.get("latency_sample", []):
            latencies_by_city.setdefault(sample.city, []).append(committed_at_ms - sample.kafka_timestamp_ms)
        metrics.write(
            kind="redis_commit",
            query_name="crowd",
            batch_id=batch_id,
            committed_at_ms=committed_at_ms,
            event_count=event_count,
            processed_total=processed_total,
            sink_ms=(time.monotonic_ns() - started_ns) // 1_000_000,
            latency_samples_ms=[latency for city_samples in latencies_by_city.values() for latency in city_samples],
            latency_samples_by_city_ms=latencies_by_city,
        )


def save_hotel_batch(batch, _batch_id: int, settings: Settings) -> None:
    def write_partition(partition) -> None:
        import redis

        with redis.Redis.from_url(settings.redis_url, decode_responses=True) as client:
            for row in partition:
                write_hotel(client, row.asDict(recursive=True), settings.redis_ttl_seconds)

    batch.foreachPartition(write_partition)


class SparkMetrics:
    def __init__(self, path: Path, stream_id: str) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.stream_id = stream_id
        self._lock = threading.Lock()

    def write(self, **record) -> None:
        try:
            with self._lock, self.path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({"stream_id": self.stream_id, **record}, separators=(",", ":")) + "\n")
        except OSError:
            logging.exception("Could not write Spark metrics to %s", self.path)

    def listener(self):
        from pyspark.sql.streaming import StreamingQueryListener

        metrics = self

        class Progress(StreamingQueryListener):
            def onQueryStarted(self, event):
                metrics.write(
                    kind="query_started", query_name=event.name, query_id=str(event.id),
                    run_id=str(event.runId), timestamp=event.timestamp,
                )

            def onQueryProgress(self, event):
                completed_at_ms = time.time_ns() // 1_000_000
                progress = json.loads(event.progress.json)
                metrics.write(
                    kind="spark_batch",
                    query_name=progress["name"],
                    batch_id=progress["batchId"],
                    query_id=progress["id"],
                    run_id=progress["runId"],
                    timestamp=progress["timestamp"],
                    completed_at_ms=completed_at_ms,
                    num_input_rows=progress["numInputRows"],
                    input_rows_per_second=progress["inputRowsPerSecond"],
                    processed_rows_per_second=progress["processedRowsPerSecond"],
                    duration_ms=progress["durationMs"],
                    state_operators=progress["stateOperators"],
                    sources=progress["sources"],
                )

            def onQueryIdle(self, _event):
                pass

            def onQueryTerminated(self, event):
                metrics.write(
                    kind="query_terminated", query_id=str(event.id), run_id=str(event.runId),
                    exception=event.exception,
                )

        return Progress()


def main() -> None:
    from pyspark.sql import SparkSession, functions as F

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--available-now", action="store_true", help="Process currently available Kafka offsets and exit")
    parser.add_argument(
        "--trigger-interval", default=os.getenv("SPARK_TRIGGER_INTERVAL", "5 seconds"),
        help="Micro-batch interval for continuous streaming",
    )
    args = parser.parse_args()
    max_offsets = int(os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "250000"))
    shuffle_partitions = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
    state_store = os.getenv("SPARK_STATE_STORE", "")
    if max_offsets < 1 or shuffle_partitions < 1:
        parser.error("Spark offsets per trigger and shuffle partitions must be positive")
    if state_store not in {"", "hdfs", "rocksdb"}:
        parser.error("SPARK_STATE_STORE must be hdfs or rocksdb")
    settings = Settings.from_env()
    reference = load_reference(settings.reference_path)
    builder = SparkSession.builder.appName("WorldCupTraffic").config("spark.sql.session.timeZone", "UTC")
    if state_store == "rocksdb":
        builder = (
            builder.config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
            .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
            .config("spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage", "true")
            .config("spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB", "512")
        )
    elif state_store == "hdfs":
        builder = builder.config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
    spark = builder.getOrCreate()
    shuffle_key = "spark.sql.shuffle.partitions"
    if "SPARK_SHUFFLE_PARTITIONS" in os.environ or not spark.sparkContext.getConf().contains(shuffle_key):
        spark.conf.set(shuffle_key, shuffle_partitions)
    spark.sparkContext.setLogLevel("WARN")
    metrics_path = os.getenv("SPARK_METRICS_PATH")
    metrics = SparkMetrics(Path(metrics_path), settings.stream_id) if metrics_path else None
    listener = metrics.listener() if metrics else None
    if listener:
        spark.streams.addListener(listener)
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", ",".join([*TOPICS.values(), HOTEL_TOPIC]))
        .option("startingOffsets", "earliest" if args.available_now else "latest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .option("failOnDataLoss", "true")
        .load()
    )
    crowd, _ = crowd_frames(
        raw, reference, settings.h3_resolution, include_h3=False, monitor_latency=bool(metrics),
    )
    hotels, _ = hotel_frames(raw, reference)
    deduplicated = crowd.withWatermark("event_time", settings.watermark_delay).dropDuplicatesWithinWatermark(["event_id"])
    invalid = reject_records(raw, reference)
    checkpoint_root = Path(settings.spark_checkpoint_root).resolve()
    trigger = {"availableNow": True} if args.available_now else {"processingTime": args.trigger_interval}
    queries = []
    try:
        queries.append(
            deduplicated.writeStream.outputMode("append")
            .queryName("crowd")
            .foreachBatch(lambda batch, batch_id: save_crowd_batch(batch, batch_id, settings, reference, metrics))
            .option("checkpointLocation", str(checkpoint_root / "crowd"))
            .trigger(**trigger)
            .start()
        )
        queries.append(
            hotels.writeStream.outputMode("append")
            .queryName("hotels")
            .foreachBatch(lambda batch, batch_id: save_hotel_batch(batch, batch_id, settings))
            .option("checkpointLocation", str(checkpoint_root / "hotels"))
            .trigger(**trigger)
            .start()
        )
        dead_letter = invalid.select(
            F.col("topic").cast("string").alias("key"),
            F.to_json(F.struct("topic", "partition", "offset", "invalid_reason", "event_id")).alias("value"),
        )
        queries.append(
            dead_letter.writeStream.format("kafka").outputMode("append")
            .queryName("invalid")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
            .option("topic", INVALID_TOPIC)
            .option("checkpointLocation", str(checkpoint_root / "invalid"))
            .trigger(**trigger)
            .start()
        )
        if args.available_now:
            for query in queries:
                query.awaitTermination()
        else:
            spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
