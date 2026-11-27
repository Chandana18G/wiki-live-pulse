"""Tests for processor/transform.py using the 50 real sample events.

Run from the repo root:  python tests/test_transform.py   (or: pytest tests)
"""
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "processor"))

from transform import to_bronze, to_silver  # noqa: E402

SAMPLE = ROOT / "data" / "sample" / "sample_events.jsonl"
INGEST = "2026-11-27T08:00:00+00:00"


def load_sample():
    return [json.loads(line) for line in SAMPLE.read_text(encoding="utf-8").splitlines() if line]


def test_every_sample_edit_becomes_a_valid_silver_row():
    events = load_sample()
    rows = [to_silver(e, INGEST) for e in events]
    assert all(r is not None for r in rows), "every sample event is an edit"
    for r in rows:
        assert r["event_id"]
        datetime.fromisoformat(r["ts"])  # parseable timestamp
        assert r["ts"].endswith("+00:00")
        assert isinstance(r["is_bot"], bool)
        assert "user" not in r  # no personal data in silver


def test_bytes_changed_is_new_minus_old():
    e = load_sample()[0]
    r = to_silver(e, INGEST)
    assert r["bytes_changed"] == e["length"]["new"] - e["length"]["old"]


def test_non_edit_and_missing_id_are_skipped():
    assert to_silver({"type": "log", "meta": {"id": "x"}}, INGEST) is None
    assert to_silver({"type": "edit", "meta": {}}, INGEST) is None


def test_missing_length_gives_null_bytes_changed():
    r = to_silver({"type": "edit", "meta": {"id": "a", "dt": "2026-11-27T07:00:00Z"}}, INGEST)
    assert r["bytes_changed"] is None and r["length_old"] is None


def test_epoch_fallback_when_meta_dt_missing():
    r = to_silver({"type": "edit", "meta": {"id": "a"}, "timestamp": 1795765166}, INGEST)
    assert r["ts"].startswith("2026-11-27T")


def test_bronze_keeps_the_raw_event():
    e = load_sample()[0]
    b = to_bronze(e, INGEST)
    assert json.loads(b["payload"]) == e
    assert b["event_id"] == e["meta"]["id"]


if __name__ == "__main__":
    tests = [v for k, v in dict(globals()).items() if k.startswith("test_")]
    for t in tests:
        t()
        print("ok ", t.__name__)
    print(f"{len(tests)} tests passed")
