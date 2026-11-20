import json
import os
import sys
from datetime import datetime, timezone

import pytest

from worldcup.producer import crowd_events, hotel_snapshots
from worldcup.reference import load_reference


pyspark = pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[1]")
        .appName("WorldCupContractTests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def raw_frame(spark, topic_and_items):
    from pyspark.sql import functions as F

    frames = [
        spark.range(1).select(
            F.lit(topic).alias("topic"),
            F.lit(0).alias("partition"),
            F.lit(index).cast("long").alias("offset"),
            F.lit(json.dumps(item).encode("utf-8")).alias("value"),
        )
        for index, (topic, item) in enumerate(topic_and_items)
    ]
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def test_crowd_validation_h3_and_privacy(spark):
    from spark_worldcup_traffic import crowd_frames
    from worldcup.spatial import latlon_to_h3

    reference = load_reference()
    event = next(crowd_events(reference, count=1, seed=1, run_id="test", timestamp=datetime(2030, 6, 15, tzinfo=timezone.utc)))
    valid, invalid = crowd_frames(
        raw_frame(
            spark,
            [
                ("stadium-access", event.to_mapping() | {"source": "stadium_access", "action": "gate_checkin"}),
                ("stadium-access", event.to_mapping() | {"event_id": "bad", "source": "stadium_access", "action": "gate_checkin", "lat": 91}),
            ],
        ),
        reference,
        8,
    )
    good_rows = valid.select("event_id", "city", "event_time").collect()
    bad_rows = invalid.collect()
    assert len(good_rows) == 1
    assert latlon_to_h3(event.lat, event.lon, 8).startswith("8")
    assert "fan_id" not in valid.columns
    assert len(bad_rows) == 1 and bad_rows[0].invalid_reason == "coordinates"
    assert "fan_id" not in invalid.columns


def test_hotel_validation(spark):
    from spark_worldcup_traffic import hotel_frames

    reference = load_reference()
    hotel = next(hotel_snapshots(reference, seed=2, run_id="test", timestamp=datetime(2030, 6, 15, tzinfo=timezone.utc)))
    valid, invalid = hotel_frames(
        raw_frame(spark, [("hotel-occupancy", hotel.to_mapping()), ("hotel-occupancy", hotel.to_mapping() | {"snapshot_id": "bad", "rooms_available": -1})]),
        reference,
    )
    assert valid.count() == 1
    assert [row.invalid_reason for row in invalid.collect()] == ["rooms"]


def test_combined_rejections_include_invalid_calendar_dates(spark):
    from spark_worldcup_traffic import reject_records

    reference = load_reference()
    start = datetime(2030, 6, 15, tzinfo=timezone.utc)
    event = next(crowd_events(reference, count=1, seed=1, run_id="test", timestamp=start)).to_mapping()
    hotel = next(hotel_snapshots(reference, seed=2, run_id="test", timestamp=start)).to_mapping()
    raw = raw_frame(
        spark,
        [
            ("stadium-access", event | {"source": "stadium_access", "action": "gate_checkin", "timestamp": "2030-02-30T00:00:00Z"}),
            ("hotel-occupancy", hotel | {"timestamp": "2030-02-30T00:00:00Z"}),
            ("hotel-occupancy", hotel),
        ],
    )
    assert {(row.topic, row.invalid_reason) for row in reject_records(raw, reference).collect()} == {
        ("stadium-access", "timestamp"), ("hotel-occupancy", "timestamp"),
    }


def test_crowd_aggregation_retains_sliding_windows_and_venue_totals(spark):
    from spark_worldcup_traffic import crowd_aggregates
    from worldcup.config import Settings

    start = datetime(2030, 6, 15, 0, 0, 16, tzinfo=timezone.utc)
    batch = spark.createDataFrame(
        [
            ("Rabat", None, "transit_validation", "cell-1", start),
            ("Rabat", "", "transit_validation", "cell-1", start),
            ("Rabat", "", "transit_validation", "cell-2", start),
        ],
        "city string, venue string, source string, h3_cell string, event_time timestamp",
    )
    rows = crowd_aggregates(batch, Settings(window_duration="20 seconds", window_slide="10 seconds")).collect()
    counts = {(row.kind, row.h3_cell, row.window.start.second): row.event_count for row in rows}
    assert counts == {
        ("cell", "cell-1", 0): 2, ("cell", "cell-1", 10): 2,
        ("cell", "cell-2", 0): 1, ("cell", "cell-2", 10): 1,
        ("venue", "", 0): 3, ("venue", "", 10): 3,
    }
    assert all(row.venue == "" for row in rows)
