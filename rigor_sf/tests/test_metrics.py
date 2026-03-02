"""Tests for metrics writer."""

from __future__ import annotations

import json
from pathlib import Path

from rigor_sf.metrics import MetricsWriter


def test_metrics_writer_creates_jsonl(tmp_path: Path):
    writer = MetricsWriter(output_dir=tmp_path / "metrics", run_id="testrun")
    writer.write_event(phase="infer", event="start", status="running")
    writer.write_event(
        phase="infer",
        event="end",
        status="success",
        duration_ms=123,
        counts={"edges": 5},
        exit_code=0,
    )

    lines = writer.path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2

    first = json.loads(lines[0])
    second = json.loads(lines[1])
    assert first["run_id"] == "testrun"
    assert first["phase"] == "infer"
    assert first["event"] == "start"
    assert second["duration_ms"] == 123
    assert second["counts"]["edges"] == 5
    assert second["exit_code"] == 0
