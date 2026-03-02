"""Tests for pipeline metrics hooks."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from rigor_sf.exit_codes import ExitCode
from rigor_sf.pipeline import run


def _write_config(base: Path) -> Path:
    (base / "data").mkdir(parents=True, exist_ok=True)
    (base / "golden").mkdir(parents=True, exist_ok=True)
    (base / "metadata").mkdir(parents=True, exist_ok=True)

    (base / "metadata" / "tables.csv").write_text("table_name,comment\n", encoding="utf-8")
    (base / "metadata" / "columns.csv").write_text("table_name,column_name,comment\n", encoding="utf-8")
    (base / "golden" / "overrides.yaml").write_text(
        yaml.safe_dump({"approve": [], "reject": [], "table_classification": {}}),
        encoding="utf-8",
    )
    (base / "data" / "core.owl").write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:owl="http://www.w3.org/2002/07/owl#">
  <owl:Ontology rdf:about="http://example.org/rigor"/>
</rdf:RDF>
""",
        encoding="utf-8",
    )
    pd.DataFrame(
        columns=[
            "from_table",
            "from_column",
            "to_table",
            "to_column",
            "confidence_sql",
            "status",
            "evidence",
            "match_rate",
            "pk_unique_rate",
            "fk_null_rate",
        ]
    ).to_csv(base / "data" / "inferred_relationships.csv", index=False)

    cfg = {
        "db": {"url": "sqlite:///:memory:"},
        "paths": {
            "core_in": str(base / "data" / "core.owl"),
            "core_out": str(base / "data" / "core.owl"),
            "provenance_jsonl": str(base / "data" / "provenance.jsonl"),
            "fragments_dir": str(base / "data" / "fragments"),
            "inferred_relationships_csv": str(base / "data" / "inferred_relationships.csv"),
            "overrides_yaml": str(base / "golden" / "overrides.yaml"),
            "runs_dir": str(base / "runs"),
            "data_quality_report": str(base / "data" / "data_quality_report.json"),
            "validation_report": str(base / "data" / "validation_report.json"),
        },
        "metadata": {
            "tables_csv": str(base / "metadata" / "tables.csv"),
            "columns_csv": str(base / "metadata" / "columns.csv"),
        },
        "llm": {"provider": "cursor", "interactive_on_failure": False, "max_retries": 1},
    }
    cfg_path = base / "config.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return cfg_path


def test_run_emits_phase_metrics(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cfg_path = _write_config(tmp_path)

    result = run(str(cfg_path), phase="validate")
    assert result == ExitCode.SUCCESS

    metrics_dir = tmp_path / "artifacts" / "metrics"
    files = sorted(metrics_dir.glob("run_*.jsonl"))
    assert files
    events = [json.loads(line) for line in files[-1].read_text(encoding="utf-8").splitlines()]

    assert any(e["phase"] == "validate" and e["event"] == "start" for e in events)
    assert any(e["phase"] == "validate" and e["event"] == "end" and e["status"] == "success" for e in events)
    assert any(e["phase"] == "run" and e["event"] == "summary" and e["exit_code"] == 0 for e in events)
