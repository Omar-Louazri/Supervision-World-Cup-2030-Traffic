"""Deterministic synthetic crowd and hotel data generation."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import random
from time import time_ns
from typing import Any, Iterator

from .contracts import CrowdEvent, HotelSnapshot, TOPICS, utc_text


def crowd_events(
    reference: dict[str, Any], *, count: int, seed: int, run_id: str, timestamp: datetime | None = None
) -> Iterator[CrowdEvent]:
    for item in _crowd_records(reference, count=count, seed=seed, run_id=run_id, timestamp=timestamp):
        yield CrowdEvent.from_mapping(item)


def crowd_payloads(
    reference: dict[str, Any], *, count: int, seed: int, run_id: str, timestamp: datetime | None = None
) -> Iterator[tuple[str, bytes]]:
    """Encode fresh synthetic events without repeated validation and dataclass copies."""
    encode = json.JSONEncoder(separators=(",", ":")).encode
    locations: dict[tuple[str, str | None, str], tuple[str, str | None]] = {}
    for item in _crowd_records(reference, count=count, seed=seed, run_id=run_id.lstrip(), timestamp=timestamp):
        source = item["source"]
        location_key = item["city"], item["venue"], source
        if location_key not in locations:
            # All other fields are generated with the same bounds and types on every record.
            validated = CrowdEvent.from_mapping(item)
            locations[location_key] = validated.city, validated.venue
        item["city"], item["venue"] = locations[location_key]
        yield TOPICS[source], encode(item).encode("utf-8")


def _crowd_records(
    reference: dict[str, Any], *, count: int, seed: int, run_id: str, timestamp: datetime | None
) -> Iterator[dict[str, Any]]:
    if count < 0:
        raise ValueError("count must be nonnegative")
    rng = random.Random(seed)
    cities = reference["cities"]
    fan_ids = max(count // 3, 1)
    previous_second = None
    event_time = utc_text(timestamp) if timestamp is not None else None
    for index in range(count):
        if timestamp is None:
            second = time_ns() // 1_000_000_000
            if second != previous_second:
                event_time = utc_text(datetime.fromtimestamp(second, timezone.utc))
                previous_second = second
        city = cities[rng.randrange(len(cities))]
        source = "stadium_access" if rng.random() < 0.55 else "transit_validation"
        center_lat, center_lon = city["center"]
        north, east = rng.gauss(0, 1), rng.gauss(0, 1)
        site = city.get("site")
        if site:
            # Place simulated scans in the gate plazas and at the transit stop.
            gate_scan = source == "stadium_access"
            center_lat, center_lon = site["gates"][index % len(site["gates"])] if gate_scan else site["transit"]
            spread_m = 15 if gate_scan else 23
            north = max(-2.5, min(2.5, north)) * spread_m / 111_320
            east = max(-2.5, min(2.5, east)) * spread_m / (111_320 * math.cos(math.radians(center_lat)))
        else:
            north *= 0.004
            east *= 0.004
        lat = max(-90, min(90, center_lat + north))
        lon = max(-180, min(180, center_lon + east))
        yield {
            "event_id": f"{run_id}-{index:012d}",
            "schema_version": 1,
            "source": source,
            "city": city["name"],
            "venue": city["venue"],
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "timestamp": event_time,
            "action": "gate_checkin" if source == "stadium_access" else "transit_validation",
            "fan_id": f"SYN-{rng.randrange(fan_ids):08d}",
        }


def hotel_snapshots(
    reference: dict[str, Any], *, seed: int, run_id: str, timestamp: datetime | None = None
) -> Iterator[HotelSnapshot]:
    rng = random.Random(seed)
    event_time = utc_text(timestamp or datetime.now(timezone.utc))
    for city in reference["cities"]:
        total = city["hotel_rooms_total"]
        occupied = round(total * rng.uniform(0.60, 0.95))
        yield HotelSnapshot.from_mapping(
            {
                "snapshot_id": f"{run_id}-hotel-{city['name'].lower()}",
                "schema_version": 1,
                "source_id": "synthetic-demo",
                "city": city["name"],
                "area_id": "citywide",
                "timestamp": event_time,
                "rooms_available": total - occupied,
                "rooms_occupied": occupied,
            }
        )
