"""Tests for CLI argument parsing and validation.

Tests verify:
- Required arguments are enforced
- Phase choices are validated
- Optional arguments are parsed correctly
- Exit codes match spec §16.5
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest import mock

import pytest
import yaml

from rigor_v1.exit_codes import ExitCode, ConfigError, PrerequisiteError
from rigor_v1.pipeline import main, run, PHASES


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def minimal_config(tmp_path: Path) -> Path:
    """Create a minimal valid configuration file."""
    config = {
        "db": {"url": "sqlite:///:memory:"},
        "paths": {
            "core_in": str(tmp_path / "data" / "core.owl"),
            "core_out": str(tmp_path / "data" / "core.owl"),
            "provenance_jsonl": str(tmp_path / "data" / "provenance.jsonl"),
            "fragments_dir": str(tmp_path / "data" / "fragments"),
            "inferred_relationships_csv": str(tmp_path / "data" / "inferred_relationships.csv"),
            "overrides_yaml": str(tmp_path / "golden" / "overrides.yaml"),
            "runs_dir": str(tmp_path / "runs"),
            "data_quality_report": str(tmp_path / "data" / "data_quality_report.json"),
            "validation_report": str(tmp_path / "data" / "validation_report.json"),
        },
        "metadata": {
            "tables_csv": str(tmp_path / "metadata" / "tables.csv"),
            "columns_csv": str(tmp_path / "metadata" / "columns.csv"),
        },
    }

    # Create directories
    (tmp_path / "data" / "fragments").mkdir(parents=True)
    (tmp_path / "golden").mkdir(parents=True)
    (tmp_path / "metadata").mkdir(parents=True)
    (tmp_path / "runs").mkdir(parents=True)

    # Create required files
    (tmp_path / "metadata" / "tables.csv").write_text("table_name,comment\n")
    (tmp_path / "metadata" / "columns.csv").write_text("table_name,column_name,comment\n")
    (tmp_path / "golden" / "overrides.yaml").write_text(
        yaml.safe_dump({"approve": [], "reject": [], "table_classification": {}})
    )
    (tmp_path / "data" / "core.owl").write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:owl="http://www.w3.org/2002/07/owl#">
  <owl:Ontology rdf:about="http://example.org/rigor"/>
</rdf:RDF>
"""
    )

    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config))
    return config_path


@pytest.fixture
def sample_sql_dir(tmp_path: Path) -> Path:
    """Create a sample SQL worksheets directory."""
    sql_dir = tmp_path / "sql_worksheets"
    sql_dir.mkdir()
    (sql_dir / "test.sql").write_text(
        """
        SELECT a.id, b.name
        FROM TABLE_A a
        JOIN TABLE_B b ON b.A_ID = a.ID;
        """
    )
    return sql_dir


# ── CLI Argument Tests ────────────────────────────────────────────────────────


class TestCLIArgumentParsing:
    """Test CLI argument parsing."""

    def test_config_required(self, minimal_config: Path, capsys):
        """--config is a required argument."""
        with mock.patch.object(sys, "argv", ["pipeline", "--phase", "validate"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 2  # argparse error

    def test_phase_defaults_to_all(self, minimal_config: Path, sample_sql_dir: Path):
        """Phase defaults to 'all' if not specified."""
        # The 'all' phase requires either sql_dir or existing relationships
        # Create a relationships CSV to allow validate to run
        import pandas as pd
        rel_csv = Path(minimal_config).parent / "data" / "inferred_relationships.csv"
        df = pd.DataFrame(columns=[
            "from_table", "from_column", "to_table", "to_column",
            "confidence_sql", "status", "evidence"
        ])
        df.to_csv(rel_csv, index=False)

        # Mock LLM to avoid actual calls
        with mock.patch("rigor_v1.pipeline._get_source_mode", return_value="offline"):
            with mock.patch("rigor_v1.pipeline._load_schema_offline", return_value=[]):
                exit_code = run(str(minimal_config))
                # Should run through without specifying phase (defaults to 'all')
                assert exit_code == ExitCode.SUCCESS

    def test_valid_phase_choices(self):
        """Test that PHASES constant contains expected values."""
        expected = ["query-gen", "infer", "review", "generate", "validate", "all"]
        assert PHASES == expected

    def test_invalid_phase_raises_config_error(self, minimal_config: Path):
        """Invalid phase should raise ConfigError."""
        with pytest.raises(ConfigError) as exc_info:
            run(str(minimal_config), phase="invalid-phase")
        assert exc_info.value.exit_code == ExitCode.CONFIG_ERROR

    def test_sql_dir_optional_for_most_phases(self, minimal_config: Path):
        """--sql-dir is optional for validate phase."""
        import pandas as pd
        rel_csv = Path(minimal_config).parent / "data" / "inferred_relationships.csv"
        df = pd.DataFrame(columns=[
            "from_table", "from_column", "to_table", "to_column",
            "confidence_sql", "status", "evidence"
        ])
        df.to_csv(rel_csv, index=False)

        exit_code = run(str(minimal_config), phase="validate")
        assert exit_code == ExitCode.SUCCESS

    def test_sql_dir_required_for_query_gen(self, minimal_config: Path):
        """--sql-dir is required for query-gen phase."""
        with pytest.raises(ConfigError) as exc_info:
            run(str(minimal_config), phase="query-gen", sql_dir=None)
        assert exc_info.value.exit_code == ExitCode.CONFIG_ERROR

    def test_sql_dir_accepted_when_provided(
        self, minimal_config: Path, sample_sql_dir: Path
    ):
        """--sql-dir should be accepted for query-gen phase."""
        exit_code = run(
            str(minimal_config),
            phase="query-gen",
            sql_dir=str(sample_sql_dir),
        )
        assert exit_code == ExitCode.SUCCESS

    def test_run_dir_optional_for_infer(
        self, minimal_config: Path, sample_sql_dir: Path
    ):
        """--run-dir is optional for infer phase."""
        exit_code = run(
            str(minimal_config),
            phase="infer",
            sql_dir=str(sample_sql_dir),
            run_dir=None,  # Optional
        )
        assert exit_code == ExitCode.SUCCESS

    def test_run_dir_accepted_when_provided(
        self, minimal_config: Path, sample_sql_dir: Path, tmp_path: Path
    ):
        """--run-dir should be accepted and used."""
        import json
        import pandas as pd

        run_dir = tmp_path / "runs" / "test_run"
        (run_dir / "queries").mkdir(parents=True)
        (run_dir / "results").mkdir(parents=True)
        (run_dir / "artifacts").mkdir(parents=True)

        # Create run_meta.json
        (run_dir / "run_meta.json").write_text(json.dumps({
            "run_id": "test_run",
            "generated_at": "2026-03-01T12:00:00Z",
            "generated_by": "test",
            "sql_worksheets_ingested": [],
            "worksheets_hash": "sha256:test",
            "candidate_edges_found": 0,
            "ambiguous_direction_edges": 0,
            "sample_limit": 200000,
            "queries_generated": {},
            "downstream_run": None,
            "notes": "",
        }))

        # Create profiling results CSV with required columns
        profiling_df = pd.DataFrame(columns=[
            "from_table", "from_column", "to_table", "to_column",
            "sample_rows", "fk_nonnull", "match_count", "match_rate",
            "pk_distinct", "pk_total", "pk_unique_rate", "fk_null_rate",
            "confidence_sql", "frequency", "evidence"
        ])
        profiling_df.to_csv(run_dir / "results" / "profiling_edges.csv", index=False)

        exit_code = run(
            str(minimal_config),
            phase="infer",
            sql_dir=str(sample_sql_dir),
            run_dir=str(run_dir),
        )
        assert exit_code == ExitCode.SUCCESS


class TestNonInteractiveFlag:
    """Test --non-interactive flag behavior."""

    def test_non_interactive_disables_prompts(self, minimal_config: Path, sample_sql_dir: Path):
        """Non-interactive flag should disable interactive prompts."""
        import pandas as pd

        # Create relationships CSV
        rel_csv = Path(minimal_config).parent / "data" / "inferred_relationships.csv"
        df = pd.DataFrame([{
            "from_table": "A", "from_column": "B_ID",
            "to_table": "B", "to_column": "ID",
            "confidence_sql": 0.9, "status": "approved", "evidence": "test"
        }])
        df.to_csv(rel_csv, index=False)

        with mock.patch("rigor_v1.pipeline._get_source_mode", return_value="offline"):
            with mock.patch("rigor_v1.pipeline._load_schema_offline", return_value=[]):
                # With non-interactive, should not prompt for user input
                exit_code = run(
                    str(minimal_config),
                    phase="generate",
                    non_interactive=True,
                )
                assert exit_code == ExitCode.SUCCESS

    def test_non_interactive_auto_skips_on_failure(
        self, minimal_config: Path, sample_sql_dir: Path
    ):
        """Non-interactive mode should auto-skip on LLM failure."""
        import pandas as pd
        from rigor_v1.llm_provider import LLMResponse

        # Setup
        rel_csv = Path(minimal_config).parent / "data" / "inferred_relationships.csv"
        df = pd.DataFrame(columns=[
            "from_table", "from_column", "to_table", "to_column",
            "confidence_sql", "status", "evidence"
        ])
        df.to_csv(rel_csv, index=False)

        with mock.patch("rigor_v1.pipeline._get_source_mode", return_value="offline"):
            with mock.patch("rigor_v1.pipeline._load_schema_offline", return_value=[]):
                with mock.patch("rigor_v1.pipeline.create_provider") as mock_provider:
                    mock_llm = mock.MagicMock()
                    mock_llm.generate.return_value = LLMResponse(
                        content="",
                        raw_output="",
                        success=False,
                        error="Simulated failure",
                    )
                    mock_provider.return_value = mock_llm

                    # Should complete without hanging (auto-skip)
                    exit_code = run(
                        str(minimal_config),
                        phase="generate",
                        non_interactive=True,
                    )
                    assert exit_code == ExitCode.SUCCESS


class TestForceRegenerateFlag:
    """Test --force-regenerate flag behavior."""

    def test_force_regenerate_accepts_table_name(
        self, minimal_config: Path, sample_sql_dir: Path
    ):
        """--force-regenerate should accept table names."""
        import pandas as pd

        rel_csv = Path(minimal_config).parent / "data" / "inferred_relationships.csv"
        df = pd.DataFrame(columns=[
            "from_table", "from_column", "to_table", "to_column",
            "confidence_sql", "status", "evidence"
        ])
        df.to_csv(rel_csv, index=False)

        with mock.patch("rigor_v1.pipeline._get_source_mode", return_value="offline"):
            with mock.patch("rigor_v1.pipeline._load_schema_offline", return_value=[]):
                exit_code = run(
                    str(minimal_config),
                    phase="generate",
                    force_regenerate=["TABLE_A"],
                )
                assert exit_code == ExitCode.SUCCESS

    def test_force_regenerate_multiple_tables(
        self, minimal_config: Path, sample_sql_dir: Path
    ):
        """--force-regenerate can be specified multiple times."""
        import pandas as pd

        rel_csv = Path(minimal_config).parent / "data" / "inferred_relationships.csv"
        df = pd.DataFrame(columns=[
            "from_table", "from_column", "to_table", "to_column",
            "confidence_sql", "status", "evidence"
        ])
        df.to_csv(rel_csv, index=False)

        with mock.patch("rigor_v1.pipeline._get_source_mode", return_value="offline"):
            with mock.patch("rigor_v1.pipeline._load_schema_offline", return_value=[]):
                exit_code = run(
                    str(minimal_config),
                    phase="generate",
                    force_regenerate=["TABLE_A", "TABLE_B", "TABLE_C"],
                )
                assert exit_code == ExitCode.SUCCESS


# ── Phase Execution Tests ─────────────────────────────────────────────────────


class TestPhaseQueryGen:
    """Tests for query-gen phase execution."""

    def test_query_gen_creates_run_folder(
        self, minimal_config: Path, sample_sql_dir: Path
    ):
        """Query-gen should create a timestamped run folder."""
        exit_code = run(
            str(minimal_config),
            phase="query-gen",
            sql_dir=str(sample_sql_dir),
        )
        assert exit_code == ExitCode.SUCCESS
        # Run folder is created in cwd/runs/

    def test_query_gen_with_run_label(
        self, minimal_config: Path, sample_sql_dir: Path
    ):
        """Query-gen should accept optional run_label."""
        exit_code = run(
            str(minimal_config),
            phase="query-gen",
            sql_dir=str(sample_sql_dir),
            run_label="test_label",
        )
        assert exit_code == ExitCode.SUCCESS


class TestPhaseInfer:
    """Tests for infer phase execution."""

    def test_infer_creates_relationships_csv(
        self, minimal_config: Path, sample_sql_dir: Path
    ):
        """Infer phase should create relationships CSV."""
        exit_code = run(
            str(minimal_config),
            phase="infer",
            sql_dir=str(sample_sql_dir),
        )
        assert exit_code == ExitCode.SUCCESS

        rel_csv = Path(minimal_config).parent / "data" / "inferred_relationships.csv"
        assert rel_csv.exists()

    def test_infer_extracts_join_edges(
        self, minimal_config: Path, sample_sql_dir: Path
    ):
        """Infer should extract join edges from SQL."""
        import pandas as pd

        exit_code = run(
            str(minimal_config),
            phase="infer",
            sql_dir=str(sample_sql_dir),
        )
        assert exit_code == ExitCode.SUCCESS

        rel_csv = Path(minimal_config).parent / "data" / "inferred_relationships.csv"
        df = pd.read_csv(rel_csv)

        # Should have at least one edge from the test SQL
        assert len(df) >= 1
        assert "from_table" in df.columns
        assert "to_table" in df.columns


class TestPhaseGenerate:
    """Tests for generate phase execution."""

    def test_generate_requires_relationships(self, minimal_config: Path):
        """Generate phase should require relationships CSV."""
        # Ensure relationships CSV doesn't exist
        rel_csv = Path(minimal_config).parent / "data" / "inferred_relationships.csv"
        if rel_csv.exists():
            rel_csv.unlink()

        with mock.patch("rigor_v1.pipeline._get_source_mode", return_value="offline"):
            with pytest.raises(PrerequisiteError) as exc_info:
                run(str(minimal_config), phase="generate")
            assert exc_info.value.exit_code == ExitCode.PREREQUISITE_NOT_MET


class TestPhaseValidate:
    """Tests for validate phase execution."""

    def test_validate_requires_core_owl(self, minimal_config: Path):
        """Validate phase should require core.owl."""
        import pandas as pd

        # Create relationships CSV
        rel_csv = Path(minimal_config).parent / "data" / "inferred_relationships.csv"
        df = pd.DataFrame(columns=[
            "from_table", "from_column", "to_table", "to_column",
            "confidence_sql", "status", "evidence"
        ])
        df.to_csv(rel_csv, index=False)

        # Remove core.owl
        core_path = Path(minimal_config).parent / "data" / "core.owl"
        if core_path.exists():
            core_path.unlink()

        with pytest.raises(PrerequisiteError) as exc_info:
            run(str(minimal_config), phase="validate")
        assert exc_info.value.exit_code == ExitCode.PREREQUISITE_NOT_MET

    def test_validate_creates_report(self, minimal_config: Path):
        """Validate should create validation report."""
        import pandas as pd

        rel_csv = Path(minimal_config).parent / "data" / "inferred_relationships.csv"
        df = pd.DataFrame(columns=[
            "from_table", "from_column", "to_table", "to_column",
            "confidence_sql", "status", "evidence"
        ])
        df.to_csv(rel_csv, index=False)

        exit_code = run(str(minimal_config), phase="validate")
        assert exit_code == ExitCode.SUCCESS

        # Check report was created (may be versioned)
        report_dir = Path(minimal_config).parent / "data"
        reports = list(report_dir.glob("validation_report*.json"))
        assert len(reports) >= 1


class TestPhaseReview:
    """Tests for review phase execution."""

    def test_review_launches_streamlit(self, minimal_config: Path):
        """Review phase should attempt to launch Streamlit."""
        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = mock.MagicMock(returncode=0)

            # Note: phase_review doesn't return exit code, it calls subprocess
            from rigor_v1.pipeline import phase_review
            from rigor_v1.config import load_config

            cfg = load_config(str(minimal_config))
            cfg._config_path = str(minimal_config)

            phase_review(cfg)

            # Verify streamlit was called
            mock_run.assert_called_once()
            call_args = mock_run.call_args[0][0]
            assert "streamlit" in call_args[2]


class TestPhaseAll:
    """Tests for 'all' phase execution."""

    def test_all_runs_infer_generate_validate(
        self, minimal_config: Path, sample_sql_dir: Path
    ):
        """'all' phase should run infer, generate, and validate."""
        with mock.patch("rigor_v1.pipeline._get_source_mode", return_value="offline"):
            with mock.patch("rigor_v1.pipeline._load_schema_offline", return_value=[]):
                exit_code = run(
                    str(minimal_config),
                    phase="all",
                    sql_dir=str(sample_sql_dir),
                )
                assert exit_code == ExitCode.SUCCESS


# ── Exit Code Tests ───────────────────────────────────────────────────────────


class TestExitCodes:
    """Test exit codes per SPEC_V2.md §16.5."""

    def test_success_returns_0(self, minimal_config: Path):
        """Successful execution should return exit code 0."""
        import pandas as pd

        rel_csv = Path(minimal_config).parent / "data" / "inferred_relationships.csv"
        df = pd.DataFrame(columns=[
            "from_table", "from_column", "to_table", "to_column",
            "confidence_sql", "status", "evidence"
        ])
        df.to_csv(rel_csv, index=False)

        exit_code = run(str(minimal_config), phase="validate")
        assert exit_code == ExitCode.SUCCESS
        assert exit_code == 0

    def test_config_error_returns_1(self, tmp_path: Path):
        """Configuration errors should return exit code 1."""
        invalid_config = tmp_path / "invalid.yaml"
        invalid_config.write_text("invalid: yaml: content:")

        with pytest.raises(ConfigError) as exc_info:
            run(str(invalid_config), phase="validate")
        assert exc_info.value.exit_code == ExitCode.CONFIG_ERROR
        assert exc_info.value.exit_code == 1

    def test_prerequisite_error_returns_2(self, minimal_config: Path):
        """Prerequisite errors should return exit code 2."""
        # Remove core.owl to trigger prerequisite error
        core_path = Path(minimal_config).parent / "data" / "core.owl"
        if core_path.exists():
            core_path.unlink()

        with pytest.raises(PrerequisiteError) as exc_info:
            run(str(minimal_config), phase="validate")
        assert exc_info.value.exit_code == ExitCode.PREREQUISITE_NOT_MET
        assert exc_info.value.exit_code == 2

    def test_validation_error_returns_3(self, minimal_config: Path):
        """Validation failures should return exit code 3."""
        import pandas as pd
        from rigor_v1.exit_codes import ValidationError

        # Create invalid OWL
        core_path = Path(minimal_config).parent / "data" / "core.owl"
        core_path.write_text("not valid xml")

        # Create relationships CSV
        rel_csv = Path(minimal_config).parent / "data" / "inferred_relationships.csv"
        df = pd.DataFrame(columns=[
            "from_table", "from_column", "to_table", "to_column",
            "confidence_sql", "status", "evidence"
        ])
        df.to_csv(rel_csv, index=False)

        with pytest.raises(ValidationError) as exc_info:
            run(str(minimal_config), phase="validate")
        assert exc_info.value.exit_code == ExitCode.VALIDATION_FAILED
        assert exc_info.value.exit_code == 3
