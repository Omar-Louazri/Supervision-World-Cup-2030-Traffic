import json

import pytest

from worldcup import presentation
from worldcup.config import Settings
from worldcup.reference import load_reference


@pytest.mark.parametrize("count", [1, 500])
def test_confirmed_scene_uses_the_published_passages(tmp_path, monkeypatch, count):
    reference_path = tmp_path / "reference.json"
    presentation.prepare(reference_path)
    settings = Settings(reference_path=reference_path)
    site = load_reference(reference_path)["cities"][0]["site"]
    sent = []
    counters = iter([100, 100 + count])

    class Sink:
        def __init__(self, _servers):
            pass

        def send(self, _topic, event):
            sent.append(event)

        def close(self):
            assert not (tmp_path / "passages.json").exists()

    class Counter:
        def get(self, _key):
            return next(counters)

    monkeypatch.setattr(presentation, "KafkaSink", Sink)
    presentation.run_stage(settings, Counter(), tmp_path / "stage.json", count, 5)
    points = json.loads((tmp_path / "passages.json").read_text())["Rabat"]["features"]
    assert len(points) == len(sent) == count
    assert len(site["gates"]) == 3
    for feature, event in zip(points, sent):
        assert feature["geometry"] == {"type": "Point", "coordinates": [event["lon"], event["lat"]]}
        assert feature["properties"] == {"source": event["source"]}
        landmarks = site["gates"] if event["source"] == "stadium_access" else [site["transit"]]
        assert any(abs(event["lat"] - lat) < 0.0006 and abs(event["lon"] - lon) < 0.0007 for lat, lon in landmarks)
