"""Tests for the processor's batching and ack logic (no GCP needed).

Run from the repo root:  python tests/test_processor.py   (or: pytest tests)
"""
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "processor"))
os.environ["DRY_RUN"] = "1"

import processor  # noqa: E402

SAMPLE = ROOT / "data" / "sample" / "sample_events.jsonl"


class FakeMessage:
    def __init__(self, data: bytes):
        self.data = data
        self.state = None

    def ack(self):
        self.state = "ack"

    def nack(self):
        self.state = "nack"


class FakeSink:
    def __init__(self, ok=True):
        self.ok = ok
        self.calls = []

    def write(self, bronze, silver):
        self.calls.append((list(bronze), list(silver)))
        return self.ok


def sample_lines():
    return [l.encode("utf-8") for l in SAMPLE.read_text(encoding="utf-8").splitlines() if l]


def test_messages_are_acked_only_after_a_successful_write():
    sink = FakeSink(ok=True)
    b = processor.Batcher(sink)
    msgs = [FakeMessage(raw) for raw in sample_lines()[:5]]
    for m in msgs:
        processor.handle(m.data, b, m)
    assert all(m.state is None for m in msgs), "nothing acked before the flush"
    b.flush()
    assert all(m.state == "ack" for m in msgs)
    bronze, silver = sink.calls[0]
    assert len(bronze) == 5 and len(silver) == 5


def test_failed_write_nacks_so_pubsub_redelivers():
    b = processor.Batcher(FakeSink(ok=False))
    m = FakeMessage(sample_lines()[0])
    processor.handle(m.data, b, m)
    b.flush()
    assert m.state == "nack"


def test_malformed_message_is_acked_and_not_written():
    sink = FakeSink()
    b = processor.Batcher(sink)
    m = FakeMessage(b"{not json")
    processor.handle(m.data, b, m)
    b.flush()
    assert m.state == "ack" and sink.calls == []


def test_non_edit_goes_to_bronze_only():
    sink = FakeSink()
    b = processor.Batcher(sink)
    m = FakeMessage(json.dumps({"type": "log", "meta": {"id": "x1"}}).encode())
    processor.handle(m.data, b, m)
    b.flush()
    bronze, silver = sink.calls[0]
    assert len(bronze) == 1 and silver == [] and m.state == "ack"


def test_batch_flushes_automatically_when_full():
    sink = FakeSink()
    b = processor.Batcher(sink)
    old = processor.BATCH_SIZE
    processor.BATCH_SIZE = 10
    try:
        for raw in sample_lines()[:25]:
            processor.handle(raw, b, FakeMessage(raw))
    finally:
        processor.BATCH_SIZE = old
    assert [len(c[0]) for c in sink.calls] == [10, 10]  # 5 still waiting for the timer


if __name__ == "__main__":
    tests = [v for k, v in dict(globals()).items() if k.startswith("test_")]
    for t in tests:
        t()
        print("ok ", t.__name__)
    print(f"{len(tests)} tests passed")
