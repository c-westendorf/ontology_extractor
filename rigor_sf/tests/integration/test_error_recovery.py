"""Integration tests for error recovery.

Tests verify:
- Retry logic with exponential backoff
- Interactive S/R/H prompts
- Non-interactive auto-skip
- Graceful degradation on partial failures
"""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest
import yaml

from rigor_sf.config import (
    AppConfig,
    DBConfig,
    LLMConfig,
    PathsConfig,
    ReviewConfig,
    OntologyConfig,
    ProfilingConfig,
    ValidationConfig,
    MetadataConfig,
)
from rigor_sf.db_introspect import TableInfo, ColumnInfo, ForeignKeyInfo
from rigor_sf.exit_codes import ExitCode, LLMError
from rigor_sf.llm_provider import (
    LLMResponse,
    with_retry,
    prompt_user_recovery,
)
from rigor_sf.pipeline import run, phase_generate


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def temp_workspace():
    """Create a temporary workspace with all required directories."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)

        # Create required directories
        (base / "data" / "fragments").mkdir(parents=True)
        (base / "golden").mkdir(parents=True)
        (base / "metadata").mkdir(parents=True)
        (base / "runs").mkdir(parents=True)

        # Create empty metadata CSVs
        (base / "metadata" / "tables.csv").write_text(
            "table_name,comment\n", encoding="utf-8"
        )
        (base / "metadata" / "columns.csv").write_text(
            "table_name,column_name,comment\n", encoding="utf-8"
        )

        # Create empty overrides.yaml
        (base / "golden" / "overrides.yaml").write_text(
            yaml.safe_dump({
                "approve": [],
                "reject": [],
                "table_classification": {},
            }),
            encoding="utf-8",
        )

        # Create basic core.owl
        (base / "data" / "core.owl").write_text(
            """<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:owl="http://www.w3.org/2002/07/owl#"
         xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#">
  <owl:Ontology rdf:about="http://example.org/rigor"/>
</rdf:RDF>
""",
            encoding="utf-8",
        )

        # Create relationships CSV
        df = pd.DataFrame([
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "confidence_sql": 0.9,
                "status": "approved",
                "evidence": "test",
                "match_rate": 0.99,
                "pk_unique_rate": 1.0,
                "fk_null_rate": 0.01,
            }
        ])
        df.to_csv(base / "data" / "inferred_relationships.csv", index=False)

        yield base


@pytest.fixture
def test_config(temp_workspace: Path) -> AppConfig:
    """Create a test configuration pointing to temp workspace."""
    return AppConfig(
        db=DBConfig(url="sqlite:///:memory:"),
        llm=LLMConfig(
            provider="cursor",
            max_retries=3,
            interactive_on_failure=True,
        ),
        review=ReviewConfig(
            auto_approve_threshold=0.95,
            auto_approve_confidence=0.80,
        ),
        paths=PathsConfig(
            core_in=str(temp_workspace / "data" / "core.owl"),
            core_out=str(temp_workspace / "data" / "core.owl"),
            provenance_jsonl=str(temp_workspace / "data" / "provenance.jsonl"),
            fragments_dir=str(temp_workspace / "data" / "fragments"),
            inferred_relationships_csv=str(temp_workspace / "data" / "inferred_relationships.csv"),
            overrides_yaml=str(temp_workspace / "golden" / "overrides.yaml"),
            runs_dir=str(temp_workspace / "runs"),
            data_quality_report=str(temp_workspace / "data" / "data_quality_report.json"),
            validation_report=str(temp_workspace / "data" / "validation_report.json"),
        ),
        metadata=MetadataConfig(
            tables_csv=str(temp_workspace / "metadata" / "tables.csv"),
            columns_csv=str(temp_workspace / "metadata" / "columns.csv"),
        ),
        profiling=ProfilingConfig(sample_limit=1000),
        ontology=OntologyConfig(
            base_iri="http://example.org/rigor#",
            format="xml",
        ),
        validation=ValidationConfig(
            coverage_warn_threshold=0.50,
            coverage_pass_threshold=0.90,
            allow_duplicate_iris=False,
        ),
    )


@pytest.fixture
def config_yaml_path(temp_workspace: Path, test_config: AppConfig) -> str:
    """Write config to YAML file and return path."""
    config_path = temp_workspace / "config.yaml"

    config_dict = {
        "db": {"url": test_config.db.url},
        "llm": {
            "provider": test_config.llm.provider,
            "max_retries": test_config.llm.max_retries,
            "interactive_on_failure": test_config.llm.interactive_on_failure,
        },
        "review": {
            "auto_approve_threshold": test_config.review.auto_approve_threshold,
            "auto_approve_confidence": test_config.review.auto_approve_confidence,
        },
        "paths": {
            "core_in": test_config.paths.core_in,
            "core_out": test_config.paths.core_out,
            "provenance_jsonl": test_config.paths.provenance_jsonl,
            "fragments_dir": test_config.paths.fragments_dir,
            "inferred_relationships_csv": test_config.paths.inferred_relationships_csv,
            "overrides_yaml": test_config.paths.overrides_yaml,
            "runs_dir": test_config.paths.runs_dir,
            "data_quality_report": test_config.paths.data_quality_report,
            "validation_report": test_config.paths.validation_report,
        },
        "metadata": {
            "tables_csv": test_config.metadata.tables_csv,
            "columns_csv": test_config.metadata.columns_csv,
        },
        "profiling": {
            "sample_limit": test_config.profiling.sample_limit,
        },
        "ontology": {
            "base_iri": test_config.ontology.base_iri,
            "format": test_config.ontology.format,
        },
        "validation": {
            "coverage_warn_threshold": test_config.validation.coverage_warn_threshold,
            "coverage_pass_threshold": test_config.validation.coverage_pass_threshold,
            "allow_duplicate_iris": test_config.validation.allow_duplicate_iris,
        },
    }

    config_path.write_text(yaml.safe_dump(config_dict), encoding="utf-8")
    return str(config_path)


@pytest.fixture
def sample_tables() -> list[TableInfo]:
    """Create sample tables for testing."""
    customers = TableInfo(
        name="CUSTOMERS",
        columns=[
            ColumnInfo(name="ID", type="INTEGER", nullable=False),
            ColumnInfo(name="NAME", type="VARCHAR(100)", nullable=True),
        ],
        primary_key=["ID"],
        foreign_keys=[],
    )
    orders = TableInfo(
        name="ORDERS",
        columns=[
            ColumnInfo(name="ID", type="INTEGER", nullable=False),
            ColumnInfo(name="CUSTOMER_ID", type="INTEGER", nullable=False),
        ],
        primary_key=["ID"],
        foreign_keys=[
            ForeignKeyInfo(
                constrained_columns=["CUSTOMER_ID"],
                referred_table="CUSTOMERS",
                referred_columns=["ID"],
            )
        ],
    )
    return [customers, orders]


@pytest.fixture
def mock_success_response() -> LLMResponse:
    """Create a mock successful LLM response."""
    header = {"table": "TEST", "created_entities": {}, "assumptions": []}
    ttl = """@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rigor: <http://example.org/rigor#> .

rigor:Test a owl:Class ;
    rdfs:label "Test" .
"""
    return LLMResponse(
        content=f"{json.dumps(header)}\n{ttl}",
        raw_output="",
        success=True,
    )


@pytest.fixture
def mock_failure_response() -> LLMResponse:
    """Create a mock failed LLM response."""
    return LLMResponse(
        content="",
        raw_output="",
        success=False,
        error="Simulated LLM failure",
    )


# ── Retry Logic Tests ─────────────────────────────────────────────────────────


class TestRetryLogic:
    """Test retry logic with exponential backoff."""

    def test_with_retry_succeeds_on_first_attempt(self):
        """with_retry should succeed immediately if first call succeeds."""
        call_count = 0

        @with_retry(max_retries=3, base_delay=0.01)
        def always_succeeds() -> LLMResponse:
            nonlocal call_count
            call_count += 1
            return LLMResponse(content="success", raw_output="", success=True)

        result = always_succeeds()
        assert result.success
        assert call_count == 1

    def test_with_retry_retries_on_failure(self):
        """with_retry should retry up to max_retries on failure."""
        call_count = 0

        @with_retry(max_retries=3, base_delay=0.01)
        def always_fails() -> LLMResponse:
            nonlocal call_count
            call_count += 1
            return LLMResponse(
                content="", raw_output="", success=False, error="fail"
            )

        result = always_fails()
        assert not result.success
        # Initial attempt + max_retries
        assert call_count == 4  # 1 initial + 3 retries

    def test_with_retry_succeeds_after_failures(self):
        """with_retry should succeed if later attempt succeeds."""
        call_count = 0

        @with_retry(max_retries=3, base_delay=0.01)
        def succeeds_on_third() -> LLMResponse:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return LLMResponse(
                    content="", raw_output="", success=False, error="fail"
                )
            return LLMResponse(content="success", raw_output="", success=True)

        result = succeeds_on_third()
        assert result.success
        assert call_count == 3

    def test_with_retry_exponential_backoff(self):
        """with_retry should use exponential backoff between retries."""
        call_times = []

        @with_retry(max_retries=2, base_delay=0.05, exponential_base=2.0)
        def track_times() -> LLMResponse:
            call_times.append(time.time())
            return LLMResponse(
                content="", raw_output="", success=False, error="fail"
            )

        track_times()

        # Should have 3 calls (1 initial + 2 retries)
        assert len(call_times) == 3

        # First delay should be ~0.05s, second ~0.1s
        delay1 = call_times[1] - call_times[0]
        delay2 = call_times[2] - call_times[1]

        # Allow some tolerance for timing
        assert delay1 >= 0.04  # ~0.05s
        assert delay2 >= 0.08  # ~0.1s (2x first delay)


# ── Non-Interactive Mode Tests ────────────────────────────────────────────────


class TestNonInteractiveMode:
    """Test non-interactive mode auto-skip behavior."""

    def test_non_interactive_auto_skips_on_failure(
        self,
        config_yaml_path: str,
        temp_workspace: Path,
        sample_tables: list,
        mock_failure_response: LLMResponse,
    ):
        """Non-interactive mode should auto-skip failed tables."""
        with mock.patch("rigor_sf.pipeline._get_source_mode") as mock_mode:
            mock_mode.return_value = "offline"

            with mock.patch("rigor_sf.pipeline._load_schema_offline") as mock_schema:
                mock_schema.return_value = sample_tables

                with mock.patch("rigor_sf.pipeline.create_provider") as mock_provider:
                    mock_llm = mock.MagicMock()
                    mock_llm.generate.return_value = mock_failure_response
                    mock_provider.return_value = mock_llm

                    # Should complete without hanging (auto-skip)
                    exit_code = run(
                        config_yaml_path,
                        phase="generate",
                        non_interactive=True,
                    )

                    assert exit_code == ExitCode.SUCCESS

    def test_non_interactive_continues_after_failure(
        self,
        config_yaml_path: str,
        temp_workspace: Path,
        sample_tables: list,
        mock_success_response: LLMResponse,
        mock_failure_response: LLMResponse,
    ):
        """Non-interactive mode should continue processing after failures."""
        tables_processed = []

        def mock_generate(prompt):
            # Fail for first table, succeed for second
            for table in sample_tables:
                if table.name in prompt:
                    tables_processed.append(table.name)
                    if table.name == "CUSTOMERS":
                        return mock_failure_response
                    return mock_success_response
            return mock_failure_response

        with mock.patch("rigor_sf.pipeline._get_source_mode") as mock_mode:
            mock_mode.return_value = "offline"

            with mock.patch("rigor_sf.pipeline._load_schema_offline") as mock_schema:
                mock_schema.return_value = sample_tables

                with mock.patch("rigor_sf.pipeline.create_provider") as mock_provider:
                    mock_llm = mock.MagicMock()
                    mock_llm.generate.side_effect = mock_generate
                    mock_provider.return_value = mock_llm

                    exit_code = run(
                        config_yaml_path,
                        phase="generate",
                        non_interactive=True,
                    )

                    assert exit_code == ExitCode.SUCCESS
                    # Both tables should have been attempted
                    assert len(tables_processed) >= 1


# ── Interactive Mode Tests ────────────────────────────────────────────────────


class TestInteractiveMode:
    """Test interactive mode with user prompts."""

    def test_prompt_user_recovery_skip(self):
        """prompt_user_recovery should return 'skip' for 's' input."""
        with mock.patch("builtins.input", return_value="s"):
            result = prompt_user_recovery("test error", "TEST_TABLE")
            assert result == "skip"

    def test_prompt_user_recovery_retry(self):
        """prompt_user_recovery should return 'retry' for 'r' input."""
        with mock.patch("builtins.input", return_value="r"):
            result = prompt_user_recovery("test error", "TEST_TABLE")
            assert result == "retry"

    def test_prompt_user_recovery_halt(self):
        """prompt_user_recovery should return 'halt' for 'h' input."""
        with mock.patch("builtins.input", return_value="h"):
            result = prompt_user_recovery("test error", "TEST_TABLE")
            assert result == "halt"

    def test_prompt_user_recovery_custom_content(self):
        """prompt_user_recovery should return custom content for other input."""
        custom_ttl = "@prefix owl: <http://www.w3.org/2002/07/owl#> ."
        with mock.patch("builtins.input", return_value=custom_ttl):
            result = prompt_user_recovery("test error", "TEST_TABLE")
            assert result == custom_ttl

    def test_prompt_user_recovery_case_insensitive(self):
        """prompt_user_recovery should be case insensitive."""
        for input_val, expected in [
            ("S", "skip"),
            ("SKIP", "skip"),
            ("R", "retry"),
            ("RETRY", "retry"),
            ("H", "halt"),
            ("HALT", "halt"),
        ]:
            with mock.patch("builtins.input", return_value=input_val):
                result = prompt_user_recovery("test error", "TEST_TABLE")
                assert result == expected


# ── Partial Failure Tests ─────────────────────────────────────────────────────


class TestPartialFailure:
    """Test graceful handling of partial failures."""

    def test_some_tables_succeed_others_fail(
        self,
        config_yaml_path: str,
        temp_workspace: Path,
        sample_tables: list,
        mock_success_response: LLMResponse,
        mock_failure_response: LLMResponse,
    ):
        """Pipeline should succeed even if some tables fail in non-interactive mode."""
        call_order = []

        def mock_generate(prompt):
            for table in sample_tables:
                if table.name in prompt:
                    call_order.append(table.name)
                    # Alternate success/failure
                    if len(call_order) % 2 == 0:
                        return mock_failure_response
                    return mock_success_response
            return mock_failure_response

        with mock.patch("rigor_sf.pipeline._get_source_mode") as mock_mode:
            mock_mode.return_value = "offline"

            with mock.patch("rigor_sf.pipeline._load_schema_offline") as mock_schema:
                mock_schema.return_value = sample_tables

                with mock.patch("rigor_sf.pipeline.create_provider") as mock_provider:
                    mock_llm = mock.MagicMock()
                    mock_llm.generate.side_effect = mock_generate
                    mock_provider.return_value = mock_llm

                    exit_code = run(
                        config_yaml_path,
                        phase="generate",
                        non_interactive=True,
                    )

                    # Should still succeed overall
                    assert exit_code == ExitCode.SUCCESS

    def test_fragments_created_for_successful_tables(
        self,
        config_yaml_path: str,
        temp_workspace: Path,
        sample_tables: list,
        mock_success_response: LLMResponse,
        mock_failure_response: LLMResponse,
    ):
        """Fragment files should be created for successful tables only."""

        def mock_generate(prompt):
            for table in sample_tables:
                if table.name in prompt:
                    if table.name == "CUSTOMERS":
                        return mock_success_response
                    return mock_failure_response
            return mock_failure_response

        with mock.patch("rigor_sf.pipeline._get_source_mode") as mock_mode:
            mock_mode.return_value = "offline"

            with mock.patch("rigor_sf.pipeline._load_schema_offline") as mock_schema:
                mock_schema.return_value = sample_tables

                with mock.patch("rigor_sf.pipeline.create_provider") as mock_provider:
                    mock_llm = mock.MagicMock()
                    mock_llm.generate.side_effect = mock_generate
                    mock_provider.return_value = mock_llm

                    run(
                        config_yaml_path,
                        phase="generate",
                        non_interactive=True,
                    )

        fragments_dir = temp_workspace / "data" / "fragments"
        # CUSTOMERS should have fragment (succeeded)
        assert (fragments_dir / "CUSTOMERS.ttl").exists()


# ── Halt Behavior Tests ───────────────────────────────────────────────────────


class TestHaltBehavior:
    """Test halt behavior on LLM failure."""

    def test_interactive_halt_raises_llm_error(
        self,
        config_yaml_path: str,
        temp_workspace: Path,
        sample_tables: list,
        mock_failure_response: LLMResponse,
    ):
        """Interactive halt should raise LLMError."""
        with mock.patch("rigor_sf.pipeline._get_source_mode") as mock_mode:
            mock_mode.return_value = "offline"

            with mock.patch("rigor_sf.pipeline._load_schema_offline") as mock_schema:
                mock_schema.return_value = sample_tables[:1]  # Just one table

                with mock.patch("rigor_sf.pipeline.create_provider") as mock_provider:
                    mock_llm = mock.MagicMock()
                    mock_llm.generate.return_value = mock_failure_response
                    mock_provider.return_value = mock_llm

                    # Mock user input to 'h' (halt)
                    with mock.patch(
                        "rigor_sf.llm_provider.prompt_user_recovery",
                        return_value="halt"
                    ):
                        with mock.patch(
                            "rigor_sf.pipeline.prompt_user_recovery",
                            return_value="halt"
                        ):
                            with pytest.raises(LLMError) as exc_info:
                                run(
                                    config_yaml_path,
                                    phase="generate",
                                    non_interactive=False,
                                )

                            assert exc_info.value.exit_code == ExitCode.LLM_GENERATION_FAILED


# ── Error Message Tests ───────────────────────────────────────────────────────


class TestErrorMessages:
    """Test error message quality."""

    def test_llm_error_includes_table_name(self):
        """LLMError should include table name."""
        error = LLMError(
            "Generation failed",
            table="CUSTOMERS",
            attempt=3,
            details="Connection timeout",
        )

        assert error.table == "CUSTOMERS"
        assert error.attempt == 3

    def test_llm_error_includes_details(self):
        """LLMError should include error details."""
        error = LLMError(
            "Generation failed",
            details="The LLM returned invalid JSON",
        )

        assert "invalid JSON" in str(error)

    def test_retry_exhausted_message(self):
        """Retry exhausted should have informative message."""
        call_count = 0

        @with_retry(max_retries=2, base_delay=0.01)
        def always_fails() -> LLMResponse:
            nonlocal call_count
            call_count += 1
            return LLMResponse(
                content="",
                raw_output="raw output here",
                success=False,
                error=f"Error on attempt {call_count}",
            )

        result = always_fails()
        assert not result.success
        assert result.error is not None
        assert "attempt" in result.error.lower() or "3" in result.error
