"""Lightweight JSONL metrics for pipeline phase observability."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class MetricsWriter:
    """Write one JSON object per line under artifacts/metrics."""

    output_dir: Path = Path("artifacts/metrics")
    run_id: str = ""

    def __post_init__(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if not self.run_id:
            self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.path = self.output_dir / f"run_{self.run_id}.jsonl"

    def write_event(
        self,
        *,
        phase: str,
        event: str,
        status: str | None = None,
        duration_ms: int | None = None,
        counts: dict[str, Any] | None = None,
        error: str | None = None,
        exit_code: int | None = None,
    ) -> None:
        payload = {
            "run_id": self.run_id,
            "timestamp": _utc_now_iso(),
            "phase": phase,
            "event": event,
            "status": status,
            "duration_ms": duration_ms,
            "counts": counts or {},
            "error": error,
            "exit_code": exit_code,
        }
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
