"""Integration tests for incremental generation.

Tests verify:
- Generation cache correctly identifies unchanged tables
- Changed tables are regenerated
- --force-regenerate bypasses cache
- Cache persists across runs
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest
import yaml

from rigor_v1.config import (
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
from rigor_v1.db_introspect import TableInfo, ColumnInfo, ForeignKeyInfo
from rigor_v1.exit_codes import ExitCode
from rigor_v1.generation_cache import GenerationCache, compute_fingerprint, create_cache
from rigor_v1.llm_provider import LLMResponse
from rigor_v1.pipeline import run, phase_generate


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
            max_retries=1,
            interactive_on_failure=False,
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
            ColumnInfo(name="EMAIL", type="VARCHAR(255)", nullable=True),
        ],
        primary_key=["ID"],
        foreign_keys=[],
        comment="Customer master table",
    )
    orders = TableInfo(
        name="ORDERS",
        columns=[
            ColumnInfo(name="ID", type="INTEGER", nullable=False),
            ColumnInfo(name="CUSTOMER_ID", type="INTEGER", nullable=False),
            ColumnInfo(name="ORDER_DATE", type="DATE", nullable=False),
        ],
        primary_key=["ID"],
        foreign_keys=[
            ForeignKeyInfo(
                constrained_columns=["CUSTOMER_ID"],
                referred_table="CUSTOMERS",
                referred_columns=["ID"],
            )
        ],
        comment="Order transactions",
    )
    return [customers, orders]


@pytest.fixture
def mock_llm_response() -> LLMResponse:
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


# ── Cache Behavior Tests ──────────────────────────────────────────────────────


class TestCacheBehavior:
    """Test generation cache behavior."""

    def test_cache_saves_and_loads(self, temp_workspace: Path, sample_tables: list):
        """Cache should persist across invocations."""
        cache_dir = temp_workspace / "data" / "fragments"
        cache = create_cache(str(cache_dir))

        table = sample_tables[0]
        fingerprint = compute_fingerprint(table)

        # Save entry
        cache.put(
            table_name=table.name,
            fingerprint=fingerprint,
            ttl_content="test content",
            header={"table": table.name},
            llm_model="test-model",
        )
        cache.save()

        # Load new cache instance
        cache2 = create_cache(str(cache_dir))
        assert cache2.is_valid(table.name, fingerprint)

    def test_cache_invalidated_on_schema_change(
        self, temp_workspace: Path, sample_tables: list
    ):
        """Cache should be invalidated when schema changes."""
        cache_dir = temp_workspace / "data" / "fragments"
        cache = create_cache(str(cache_dir))

        table = sample_tables[0]
        fingerprint = compute_fingerprint(table)

        # Save entry
        cache.put(
            table_name=table.name,
            fingerprint=fingerprint,
            ttl_content="test content",
            header={"table": table.name},
            llm_model="test-model",
        )
        cache.save()

        # Modify table schema
        modified_table = TableInfo(
            name=table.name,
            columns=table.columns + [
                ColumnInfo(name="NEW_COLUMN", type="VARCHAR(50)", nullable=True)
            ],
            primary_key=table.primary_key,
            foreign_keys=table.foreign_keys,
            comment=table.comment,
        )
        new_fingerprint = compute_fingerprint(modified_table)

        # Old fingerprint should be different from new
        assert fingerprint != new_fingerprint

        # Cache should not be valid with new fingerprint
        assert not cache.is_valid(table.name, new_fingerprint)

    def test_cache_invalidated_on_classification_change(
        self, temp_workspace: Path, sample_tables: list
    ):
        """Cache should be invalidated when classification changes."""
        cache_dir = temp_workspace / "data" / "fragments"
        cache = create_cache(str(cache_dir))

        table = sample_tables[0]
        fingerprint = compute_fingerprint(table, classification="dimension")

        cache.put(
            table_name=table.name,
            fingerprint=fingerprint,
            ttl_content="test content",
            header={"table": table.name},
            llm_model="test-model",
        )
        cache.save()

        # Change classification
        new_fingerprint = compute_fingerprint(table, classification="fact")

        assert fingerprint != new_fingerprint
        assert not cache.is_valid(table.name, new_fingerprint)


# ── Incremental Generation Tests ──────────────────────────────────────────────


class TestIncrementalGeneration:
    """Test incremental generation with cache."""

    def test_unchanged_tables_skipped(
        self,
        config_yaml_path: str,
        temp_workspace: Path,
        sample_tables: list,
        mock_llm_response: LLMResponse,
    ):
        """Unchanged tables should be skipped on second run."""
        call_count = 0

        def mock_generate(prompt):
            nonlocal call_count
            call_count += 1
            return mock_llm_response

        with mock.patch("rigor_v1.pipeline._get_source_mode") as mock_mode:
            mock_mode.return_value = "offline"

            with mock.patch("rigor_v1.pipeline._load_schema_offline") as mock_schema:
                mock_schema.return_value = sample_tables

                with mock.patch("rigor_v1.pipeline.create_provider") as mock_provider:
                    mock_llm = mock.MagicMock()
                    mock_llm.generate.side_effect = mock_generate
                    mock_provider.return_value = mock_llm

                    # First run - should call LLM for both tables
                    exit_code = run(config_yaml_path, phase="generate", non_interactive=True)
                    assert exit_code == ExitCode.SUCCESS

                    first_run_calls = call_count

                    # Second run - should skip cached tables
                    call_count = 0
                    exit_code = run(config_yaml_path, phase="generate", non_interactive=True)
                    assert exit_code == ExitCode.SUCCESS

                    # Second run should have fewer LLM calls (cached)
                    assert call_count < first_run_calls or call_count == 0

    def test_force_regenerate_bypasses_cache(
        self,
        config_yaml_path: str,
        temp_workspace: Path,
        sample_tables: list,
        mock_llm_response: LLMResponse,
    ):
        """--force-regenerate should bypass cache for specified tables."""
        call_count = 0
        regenerated_tables = []

        def mock_generate(prompt):
            nonlocal call_count
            call_count += 1
            # Extract table name from prompt (simplified)
            for table in sample_tables:
                if table.name in prompt:
                    regenerated_tables.append(table.name)
                    break
            return mock_llm_response

        with mock.patch("rigor_v1.pipeline._get_source_mode") as mock_mode:
            mock_mode.return_value = "offline"

            with mock.patch("rigor_v1.pipeline._load_schema_offline") as mock_schema:
                mock_schema.return_value = sample_tables

                with mock.patch("rigor_v1.pipeline.create_provider") as mock_provider:
                    mock_llm = mock.MagicMock()
                    mock_llm.generate.side_effect = mock_generate
                    mock_provider.return_value = mock_llm

                    # First run to populate cache
                    run(config_yaml_path, phase="generate", non_interactive=True)

                    # Second run with --force-regenerate for CUSTOMERS
                    call_count = 0
                    regenerated_tables.clear()
                    exit_code = run(
                        config_yaml_path,
                        phase="generate",
                        non_interactive=True,
                        force_regenerate=["CUSTOMERS"],
                    )
                    assert exit_code == ExitCode.SUCCESS

                    # Should have regenerated CUSTOMERS
                    assert "CUSTOMERS" in regenerated_tables or call_count > 0

    def test_cache_persists_across_runs(
        self,
        temp_workspace: Path,
        sample_tables: list,
    ):
        """Cache should persist across multiple generate runs."""
        cache_dir = temp_workspace / "data" / "fragments"
        cache_file = Path(cache_dir) / ".generation_cache.json"

        # Simulate first run
        cache = create_cache(str(cache_dir))
        for table in sample_tables:
            fingerprint = compute_fingerprint(table)
            cache.put(
                table_name=table.name,
                fingerprint=fingerprint,
                ttl_content="test",
                header={},
                llm_model="test",
            )
        cache.save()

        # Verify cache file exists
        assert cache_file.exists()

        # Simulate second run
        cache2 = create_cache(str(cache_dir))
        for table in sample_tables:
            fingerprint = compute_fingerprint(table)
            assert cache2.is_valid(table.name, fingerprint)


# ── Fragment File Tests ───────────────────────────────────────────────────────


class TestFragmentFiles:
    """Test fragment file creation and caching."""

    def test_fragments_created_on_first_run(
        self,
        config_yaml_path: str,
        temp_workspace: Path,
        sample_tables: list,
        mock_llm_response: LLMResponse,
    ):
        """Fragment files should be created on first run."""
        with mock.patch("rigor_v1.pipeline._get_source_mode") as mock_mode:
            mock_mode.return_value = "offline"

            with mock.patch("rigor_v1.pipeline._load_schema_offline") as mock_schema:
                mock_schema.return_value = sample_tables

                with mock.patch("rigor_v1.pipeline.create_provider") as mock_provider:
                    mock_llm = mock.MagicMock()
                    mock_llm.generate.return_value = mock_llm_response
                    mock_provider.return_value = mock_llm

                    run(config_yaml_path, phase="generate", non_interactive=True)

        # Check fragment files exist
        fragments_dir = temp_workspace / "data" / "fragments"
        for table in sample_tables:
            frag_path = fragments_dir / f"{table.name}.ttl"
            assert frag_path.exists(), f"Fragment file missing for {table.name}"

    def test_cached_fragments_reused(
        self,
        config_yaml_path: str,
        temp_workspace: Path,
        mock_llm_response: LLMResponse,
    ):
        """Cached fragments should be reused without regeneration."""
        # Use simple tables without FKs for cleaner cache testing
        simple_tables = [
            TableInfo(
                name="SIMPLE_A",
                columns=[ColumnInfo(name="ID", type="INTEGER", nullable=False)],
                primary_key=["ID"],
                foreign_keys=[],
            ),
            TableInfo(
                name="SIMPLE_B",
                columns=[ColumnInfo(name="ID", type="INTEGER", nullable=False)],
                primary_key=["ID"],
                foreign_keys=[],
            ),
        ]

        fragments_dir = temp_workspace / "data" / "fragments"

        # Pre-create fragment files and cache with correct fingerprints
        cache = create_cache(str(fragments_dir))
        for table in simple_tables:
            frag_path = fragments_dir / f"{table.name}.ttl"
            frag_path.write_text(
                f"# Cached fragment for {table.name}\n",
                encoding="utf-8",
            )
            fingerprint = compute_fingerprint(table)
            cache.put(
                table_name=table.name,
                fingerprint=fingerprint,
                ttl_content=f"# Cached fragment for {table.name}\n",
                header={},
                llm_model="test",
            )
        cache.save()

        # Track LLM calls
        call_count = 0

        def mock_generate(prompt):
            nonlocal call_count
            call_count += 1
            return mock_llm_response

        # Create empty relationships CSV to avoid FK injection
        rel_csv = temp_workspace / "data" / "inferred_relationships.csv"
        pd.DataFrame(columns=[
            "from_table", "from_column", "to_table", "to_column",
            "confidence_sql", "status", "evidence", "match_rate"
        ]).to_csv(rel_csv, index=False)

        with mock.patch("rigor_v1.pipeline._get_source_mode") as mock_mode:
            mock_mode.return_value = "offline"

            with mock.patch("rigor_v1.pipeline._load_schema_offline") as mock_schema:
                mock_schema.return_value = simple_tables

                with mock.patch("rigor_v1.pipeline.create_provider") as mock_provider:
                    mock_llm = mock.MagicMock()
                    mock_llm.generate.side_effect = mock_generate
                    mock_provider.return_value = mock_llm

                    run(config_yaml_path, phase="generate", non_interactive=True)

        # Should not have called LLM (all cached)
        assert call_count == 0


# ── Fingerprint Tests ─────────────────────────────────────────────────────────


class TestFingerprint:
    """Test fingerprint computation."""

    def test_fingerprint_deterministic(self, sample_tables: list):
        """Fingerprint should be deterministic for same input."""
        table = sample_tables[0]
        fp1 = compute_fingerprint(table)
        fp2 = compute_fingerprint(table)
        assert fp1 == fp2

    def test_fingerprint_changes_with_column_add(self, sample_tables: list):
        """Fingerprint should change when column is added."""
        table = sample_tables[0]
        fp1 = compute_fingerprint(table)

        modified = TableInfo(
            name=table.name,
            columns=table.columns + [
                ColumnInfo(name="NEW_COL", type="INTEGER", nullable=True)
            ],
            primary_key=table.primary_key,
            foreign_keys=table.foreign_keys,
        )
        fp2 = compute_fingerprint(modified)

        assert fp1 != fp2

    def test_fingerprint_changes_with_column_type(self, sample_tables: list):
        """Fingerprint should change when column type changes."""
        table = sample_tables[0]
        fp1 = compute_fingerprint(table)

        modified_cols = [
            ColumnInfo(
                name=c.name,
                type="BIGINT" if c.name == "ID" else c.type,  # Change ID type
                nullable=c.nullable,
            )
            for c in table.columns
        ]
        modified = TableInfo(
            name=table.name,
            columns=modified_cols,
            primary_key=table.primary_key,
            foreign_keys=table.foreign_keys,
        )
        fp2 = compute_fingerprint(modified)

        assert fp1 != fp2

    def test_fingerprint_changes_with_fk(self, sample_tables: list):
        """Fingerprint should change when FK is added."""
        table = sample_tables[0]  # CUSTOMERS (no FKs)
        fp1 = compute_fingerprint(table)

        modified = TableInfo(
            name=table.name,
            columns=table.columns,
            primary_key=table.primary_key,
            foreign_keys=[
                ForeignKeyInfo(
                    constrained_columns=["SOME_COL"],
                    referred_table="OTHER_TABLE",
                    referred_columns=["ID"],
                )
            ],
        )
        fp2 = compute_fingerprint(modified)

        assert fp1 != fp2

    def test_fingerprint_includes_classification(self, sample_tables: list):
        """Fingerprint should include classification."""
        table = sample_tables[0]
        fp1 = compute_fingerprint(table, classification=None)
        fp2 = compute_fingerprint(table, classification="dimension")
        fp3 = compute_fingerprint(table, classification="fact")

        assert fp1 != fp2
        assert fp2 != fp3
        assert fp1 != fp3
