from datetime import datetime, timezone
import json
from types import SimpleNamespace

import pytest

from worldcup.contracts import CrowdEvent, HotelSnapshot, TOPICS
from worldcup.producer import crowd_events, crowd_payloads, hotel_snapshots
from worldcup.reference import load_reference


NOW = datetime(2030, 6, 15, 16, 30, tzinfo=timezone.utc)


def test_reproducible_events_and_topic_contracts():
    reference = load_reference()
    first = list(crowd_events(reference, count=30, seed=42, run_id="test", timestamp=NOW))
    second = list(crowd_events(reference, count=30, seed=42, run_id="test", timestamp=NOW))
    assert first == second
    assert len({event.event_id for event in first}) == 30
    assert {event.source for event in first} == {"stadium_access", "transit_validation"}
    assert all(event.timestamp == "2030-06-15T16:30:00Z" for event in first)


def test_kafka_payloads_preserve_the_validated_seeded_events():
    reference = load_reference()
    reference["cities"][0]["site"] = {
        "gates": [[33.612, -7.135], [33.6121, -7.135]],
        "transit": [33.613, -7.135],
    }
    args = dict(count=128, seed=42, run_id=" validated", timestamp=NOW)
    expected = [(TOPICS[event.source], event.to_mapping()) for event in crowd_events(reference, **args)]
    assert [(topic, json.loads(payload)) for topic, payload in crowd_payloads(reference, **args)] == expected


def test_kafka_batch_retries_only_rejected_messages_and_separates_late_acks(monkeypatch):
    import confluent_kafka

    from worldcup import kafka

    now = 0.0
    accepted = []

    class Producer:
        def __init__(self, _configuration):
            self.callbacks = []

        def produce_batch(self, topic, messages, *, on_delivery):
            previous_attempt = bool(accepted)
            count = len(messages) if previous_attempt else 1
            for message in messages[:count]:
                assert "_error" not in message
                accepted.append((topic, message["value"]))
                self.callbacks.append(on_delivery)
            for message in messages[count:]:
                message["_error"] = confluent_kafka.KafkaError(confluent_kafka.KafkaError._QUEUE_FULL)
            return count

        def poll(self, _timeout):
            if self.callbacks:
                self.callbacks.pop(0)(None, None)
                return 1
            return 0

        def flush(self, timeout):
            while self.poll(timeout):
                pass
            return 0

    monkeypatch.setattr(confluent_kafka, "Producer", Producer)
    monkeypatch.setattr(kafka, "time", SimpleNamespace(monotonic=lambda: now))
    sink = kafka.KafkaSink("localhost:9092", stage_deadline=1.0)
    assert sink.send_payloads("topic", [b"a", b"b", b"c"]) == 3
    assert sink.acknowledged_before_deadline == 2
    now = 1.0
    assert sink.send_payloads("topic", [b"too late"]) == 0
    sink.close()
    assert accepted == [("topic", b"a"), ("topic", b"b"), ("topic", b"c")]
    assert sink.submitted == sink.acknowledged == 3
    assert sink.acknowledged_before_deadline == 2
    assert sink.last_ack_monotonic == 1.0
    assert sink.failed == sink.pending == 0


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": 2},
        {"lat": 91},
        {"lon": float("nan")},
        {"timestamp": "2030-06-15T16:30:00"},
        {"action": "transit_validation"},
        {"event_id": ""},
    ],
)
def test_invalid_crowd_event_is_rejected(change):
    event = next(crowd_events(load_reference(), count=1, seed=1, run_id="test", timestamp=NOW))
    item = event.to_mapping() | {"source": "stadium_access", "action": "gate_checkin"} | change
    with pytest.raises(ValueError):
        CrowdEvent.from_mapping(item)


def test_hotel_capacity_contract():
    snapshot = next(hotel_snapshots(load_reference(), seed=7, run_id="test", timestamp=NOW))
    assert snapshot.rooms_available + snapshot.rooms_occupied == 12000
    with pytest.raises(ValueError):
        HotelSnapshot.from_mapping(snapshot.to_mapping() | {"rooms_available": -1})


def test_longer_producer_run_refreshes_hotel_snapshots(capsys):
    from worldcup_event_producer import main

    assert main(["--sink", "stdout", "--count", "3", "--rate", "100", "--hotel-interval", "0.001", "--run-id", "refresh"]) == 0
    records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    hotel_records = [record for record in records if record["topic"] == "hotel-occupancy"]
    crowd_records = [record for record in records if record["topic"] != "hotel-occupancy"]
    assert len(crowd_records) == 3
    assert len(hotel_records) >= 4
    assert len({record["value"]["snapshot_id"] for record in hotel_records}) == len(hotel_records)
