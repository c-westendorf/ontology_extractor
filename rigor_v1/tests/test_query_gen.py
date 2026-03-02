"""
Tests for rigor_v1/query_gen.py

Target coverage: 85%

Tests cover:
- Run folder generation
- Edge normalization and direction inference
- SQL file generation (profiling_edges, column_profiles, value_overlap)
- run_meta.json generation
- README generation
- File hashing
- Run ID generation with counters
- Edge frequency boosting
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from rigor_v1.query_gen import (
    _EdgeSpec,
    _build_directed_edges,
    _build_readme,
    _build_run_meta,
    _hash_files,
    _make_run_id,
    _write_column_profiles_sql,
    _write_profiling_edges_sql,
    _write_value_overlap_sql,
    generate_run,
)
from rigor_v1.sql_ingest import JoinEdge


# ── Edge Normalization Tests ──────────────────────────────────────────────────


class TestBuildDirectedEdges:
    """Tests for _build_directed_edges function."""

    def test_basic_direction_inference(self):
        """FK_ID -> ID pattern should set direction correctly."""
        edges = [
            JoinEdge(
                left_table="ORDERS",
                left_column="CUSTOMER_ID",
                right_table="CUSTOMERS",
                right_column="ID",
                confidence=0.85,
                evidence="test.sql",
            )
        ]
        directed = _build_directed_edges(edges)
        assert len(directed) == 1
        spec = directed[0]
        # Direction should be ORDERS.CUSTOMER_ID -> CUSTOMERS.ID
        assert spec.from_table == "ORDERS"
        assert spec.from_column == "CUSTOMER_ID"
        assert spec.to_table == "CUSTOMERS"
        assert spec.to_column == "ID"
        assert not spec.ambiguous_direction

    def test_reverse_direction_inference(self):
        """ID -> FK_ID pattern should flip direction."""
        edges = [
            JoinEdge(
                left_table="CUSTOMERS",
                left_column="ID",
                right_table="ORDERS",
                right_column="CUSTOMER_ID",
                confidence=0.85,
                evidence="test.sql",
            )
        ]
        directed = _build_directed_edges(edges)
        assert len(directed) == 1
        spec = directed[0]
        # Direction should be flipped to ORDERS.CUSTOMER_ID -> CUSTOMERS.ID
        assert spec.from_table == "ORDERS"
        assert spec.from_column == "CUSTOMER_ID"
        assert spec.to_table == "CUSTOMERS"
        assert spec.to_column == "ID"

    def test_ambiguous_direction(self):
        """Non-ID patterns should be marked ambiguous."""
        edges = [
            JoinEdge(
                left_table="TABLE_A",
                left_column="CODE",
                right_table="TABLE_B",
                right_column="CODE",
                confidence=0.6,
                evidence="test.sql",
            )
        ]
        directed = _build_directed_edges(edges)
        assert len(directed) == 1
        spec = directed[0]
        assert spec.ambiguous_direction

    def test_frequency_accumulation(self):
        """Multiple appearances should increase frequency."""
        edges = [
            JoinEdge(
                left_table="ORDERS",
                left_column="CUSTOMER_ID",
                right_table="CUSTOMERS",
                right_column="ID",
                confidence=0.85,
                evidence="file1.sql | ON clause",
            ),
            JoinEdge(
                left_table="ORDERS",
                left_column="CUSTOMER_ID",
                right_table="CUSTOMERS",
                right_column="ID",
                confidence=0.85,
                evidence="file2.sql | ON clause",
            ),
            JoinEdge(
                left_table="ORDERS",
                left_column="CUSTOMER_ID",
                right_table="CUSTOMERS",
                right_column="ID",
                confidence=0.85,
                evidence="file3.sql | ON clause",
            ),
        ]
        directed = _build_directed_edges(edges)
        assert len(directed) == 1
        assert directed[0].frequency == 3

    def test_frequency_boost_5(self):
        """5+ appearances should get confidence boost."""
        edges = [
            JoinEdge(
                left_table="A",
                left_column="B_ID",
                right_table="B",
                right_column="ID",
                confidence=0.80,
                evidence=f"file{i}.sql | ON clause",
            )
            for i in range(5)
        ]
        directed = _build_directed_edges(edges)
        assert len(directed) == 1
        # 0.80 + 0.05 (freq 5 boost) = 0.85
        assert directed[0].confidence_sql == 0.85

    def test_frequency_boost_10(self):
        """10+ appearances should get larger confidence boost."""
        edges = [
            JoinEdge(
                left_table="A",
                left_column="B_ID",
                right_table="B",
                right_column="ID",
                confidence=0.80,
                evidence=f"file{i}.sql | ON clause",
            )
            for i in range(10)
        ]
        directed = _build_directed_edges(edges)
        assert len(directed) == 1
        # 0.80 + 0.10 (freq 10 boost) = 0.90
        assert directed[0].confidence_sql == 0.90

    def test_deduplication(self):
        """Duplicate edges should be deduplicated."""
        edges = [
            JoinEdge(
                left_table="A",
                left_column="B_ID",
                right_table="B",
                right_column="ID",
                confidence=0.85,
                evidence="file1.sql | ON clause",
            ),
            JoinEdge(
                left_table="B",
                left_column="ID",
                right_table="A",
                right_column="B_ID",
                confidence=0.80,
                evidence="file2.sql | ON clause",
            ),
        ]
        directed = _build_directed_edges(edges)
        # Both edges are the same relationship, should dedupe to 1
        assert len(directed) == 1

    def test_evidence_consolidation(self):
        """Evidence from multiple files should be consolidated."""
        edges = [
            JoinEdge(
                left_table="A",
                left_column="B_ID",
                right_table="B",
                right_column="ID",
                confidence=0.85,
                evidence="file1.sql | ON clause",
            ),
            JoinEdge(
                left_table="A",
                left_column="B_ID",
                right_table="B",
                right_column="ID",
                confidence=0.85,
                evidence="file2.sql | ON clause",
            ),
        ]
        directed = _build_directed_edges(edges)
        assert len(directed) == 1
        # Evidence should contain both file paths
        assert "file1.sql" in directed[0].evidence
        assert "file2.sql" in directed[0].evidence

    def test_sorting(self):
        """Results should be sorted by frequency and confidence."""
        edges = [
            JoinEdge("A", "B_ID", "B", "ID", 0.70, "file1.sql | ON clause"),
            JoinEdge("C", "D_ID", "D", "ID", 0.90, "file1.sql | ON clause"),
            JoinEdge("C", "D_ID", "D", "ID", 0.90, "file2.sql | ON clause"),
        ]
        directed = _build_directed_edges(edges)
        # C->D should come first (higher frequency)
        assert directed[0].from_table == "C"

    def test_id_only_column_direction(self):
        """When one side is just ID (not FK), that's the referred side."""
        edges = [
            JoinEdge(
                left_table="DETAIL",
                left_column="CODE",
                right_table="MASTER",
                right_column="ID",
                confidence=0.70,
                evidence="test.sql",
            )
        ]
        directed = _build_directed_edges(edges)
        spec = directed[0]
        # MASTER.ID is the referred side
        assert spec.to_table == "MASTER"
        assert spec.to_column == "ID"


# ── SQL File Generation Tests ─────────────────────────────────────────────────


class TestWriteProfilingEdgesSql:
    """Tests for _write_profiling_edges_sql function."""

    def test_generates_sql_file(self, temp_dir):
        """Should generate a valid SQL file."""
        run_dir = temp_dir / "test_run"
        (run_dir / "queries").mkdir(parents=True)

        edges = [
            _EdgeSpec(
                from_table="ORDERS",
                from_column="CUSTOMER_ID",
                to_table="CUSTOMERS",
                to_column="ID",
                confidence_sql=0.85,
                frequency=3,
                evidence="test.sql",
                ambiguous_direction=False,
            )
        ]
        _write_profiling_edges_sql(edges, run_dir, sample_limit=200000)

        sql_path = run_dir / "queries" / "01_profiling_edges.sql"
        assert sql_path.exists()
        content = sql_path.read_text()

        # Check header
        assert "RIGOR-SF" in content
        assert "01_profiling_edges.sql" in content
        assert "1 candidate join edges" in content

        # Check SQL structure
        assert "SELECT" in content
        assert "FROM ORDERS" in content
        assert "CUSTOMER_ID" in content
        assert "match_rate" in content
        assert "pk_unique_rate" in content
        assert "fk_null_rate" in content

    def test_multiple_edges_union_all(self, temp_dir):
        """Multiple edges should be joined with UNION ALL."""
        run_dir = temp_dir / "test_run"
        (run_dir / "queries").mkdir(parents=True)

        edges = [
            _EdgeSpec("A", "B_ID", "B", "ID", 0.85, 1, "test", False),
            _EdgeSpec("C", "D_ID", "D", "ID", 0.90, 2, "test", False),
        ]
        _write_profiling_edges_sql(edges, run_dir, sample_limit=100000)

        content = (run_dir / "queries" / "01_profiling_edges.sql").read_text()
        # There's one UNION ALL per edge boundary (N-1 for N edges)
        # The template adds "UNION ALL\n\n" between blocks
        assert "UNION ALL" in content

    def test_ambiguous_direction_note(self, temp_dir):
        """Ambiguous edges should have a note in the SQL."""
        run_dir = temp_dir / "test_run"
        (run_dir / "queries").mkdir(parents=True)

        edges = [
            _EdgeSpec("A", "CODE", "B", "CODE", 0.60, 1, "test", True),
        ]
        _write_profiling_edges_sql(edges, run_dir, sample_limit=100000)

        content = (run_dir / "queries" / "01_profiling_edges.sql").read_text()
        assert "AMBIGUOUS DIRECTION" in content


class TestWriteColumnProfilesSql:
    """Tests for _write_column_profiles_sql function."""

    def test_generates_unique_columns(self, temp_dir):
        """Should profile each unique (table, column) pair once."""
        run_dir = temp_dir / "test_run"
        (run_dir / "queries").mkdir(parents=True)

        edges = [
            _EdgeSpec("A", "B_ID", "B", "ID", 0.85, 1, "test", False),
            _EdgeSpec("A", "C_ID", "C", "ID", 0.85, 1, "test", False),
        ]
        _write_column_profiles_sql(edges, run_dir, sample_limit=100000)

        content = (run_dir / "queries" / "02_column_profiles.sql").read_text()
        # Should have 4 unique columns: A.B_ID, B.ID, A.C_ID, C.ID
        assert "A.B_ID" in content or "'B_ID'" in content
        assert "column_profiles.sql" in content

    def test_empty_edges(self, temp_dir):
        """Should handle empty edge list."""
        run_dir = temp_dir / "test_run"
        (run_dir / "queries").mkdir(parents=True)

        _write_column_profiles_sql([], run_dir, sample_limit=100000)

        content = (run_dir / "queries" / "02_column_profiles.sql").read_text()
        assert "0 join columns" in content


class TestWriteValueOverlapSql:
    """Tests for _write_value_overlap_sql function."""

    def test_generates_for_ambiguous_only(self, temp_dir):
        """Should only generate queries for ambiguous edges."""
        run_dir = temp_dir / "test_run"
        (run_dir / "queries").mkdir(parents=True)

        edges = [
            _EdgeSpec("A", "B_ID", "B", "ID", 0.85, 1, "test", False),  # Not ambiguous
            _EdgeSpec("C", "CODE", "D", "CODE", 0.60, 1, "test", True),  # Ambiguous
        ]
        _write_value_overlap_sql(edges, run_dir, sample_limit=100000)

        content = (run_dir / "queries" / "03_value_overlap.sql").read_text()
        # Should only have 1 ambiguous edge
        assert "1 ambiguous edge" in content
        assert "C" in content
        assert "D" in content

    def test_no_ambiguous_edges(self, temp_dir):
        """Should generate placeholder when no ambiguous edges."""
        run_dir = temp_dir / "test_run"
        (run_dir / "queries").mkdir(parents=True)

        edges = [
            _EdgeSpec("A", "B_ID", "B", "ID", 0.85, 1, "test", False),
        ]
        _write_value_overlap_sql(edges, run_dir, sample_limit=100000)

        content = (run_dir / "queries" / "03_value_overlap.sql").read_text()
        assert "0 ambiguous edge" in content
        assert "no_ambiguous_edges" in content


# ── Run Meta Tests ────────────────────────────────────────────────────────────


class TestBuildRunMeta:
    """Tests for _build_run_meta function."""

    def test_meta_structure(self, temp_dir):
        """Should generate correct metadata structure."""
        files = [temp_dir / "test.sql"]
        files[0].write_text("SELECT 1")

        edges = [
            _EdgeSpec("A", "B_ID", "B", "ID", 0.85, 1, "test", False),
            _EdgeSpec("C", "CODE", "D", "CODE", 0.60, 1, "test", True),
        ]

        meta = _build_run_meta(
            run_id="2026-03-01_001",
            worksheet_files=files,
            directed=edges,
            worksheets_hash="sha256:abc123",
            sample_limit=200000,
        )

        assert meta["run_id"] == "2026-03-01_001"
        assert "generated_at" in meta
        assert meta["worksheets_hash"] == "sha256:abc123"
        assert meta["candidate_edges_found"] == 2
        assert meta["ambiguous_direction_edges"] == 1
        assert meta["sample_limit"] == 200000
        assert "queries_generated" in meta
        assert meta["downstream_run"] is None

    def test_queries_metadata(self, temp_dir):
        """Should include metadata for each query file."""
        files = [temp_dir / "test.sql"]
        files[0].write_text("SELECT 1")

        edges = [_EdgeSpec("A", "B_ID", "B", "ID", 0.85, 1, "test", False)]

        meta = _build_run_meta("test_run", files, edges, "hash", 100000)

        assert "01_profiling_edges.sql" in meta["queries_generated"]
        assert "02_column_profiles.sql" in meta["queries_generated"]
        assert "03_value_overlap.sql" in meta["queries_generated"]


# ── README Tests ──────────────────────────────────────────────────────────────


class TestBuildReadme:
    """Tests for _build_readme function."""

    def test_readme_structure(self, temp_dir):
        """Should generate a well-structured README."""
        run_dir = temp_dir / "test_run"
        run_dir.mkdir()

        files = [temp_dir / "test.sql"]
        files[0].write_text("SELECT 1")

        edges = [
            _EdgeSpec("ORDERS", "CUSTOMER_ID", "CUSTOMERS", "ID", 0.85, 5, "test", False),
        ]

        readme = _build_readme("2026-03-01_001", files, edges, run_dir)

        # Check structure
        assert "# RIGOR-SF Query Package" in readme
        assert "2026-03-01_001" in readme
        assert "Step 1" in readme
        assert "Step 2" in readme
        assert "Step 3" in readme
        assert "Step 4" in readme
        assert "profiling_edges.csv" in readme
        assert "column_profiles.csv" in readme
        assert "value_overlap.csv" in readme

    def test_readme_includes_top_edges(self, temp_dir):
        """Should include top edges by frequency."""
        run_dir = temp_dir / "test_run"
        run_dir.mkdir()

        files = [temp_dir / "test.sql"]
        files[0].write_text("SELECT 1")

        edges = [
            _EdgeSpec("ORDERS", "CUSTOMER_ID", "CUSTOMERS", "ID", 0.85, 10, "test", False),
            _EdgeSpec("ITEMS", "ORDER_ID", "ORDERS", "ID", 0.90, 5, "test", False),
        ]

        readme = _build_readme("test_run", files, edges, run_dir)

        # Should show top edges
        assert "ORDERS.CUSTOMER_ID" in readme
        assert "frequency: 10" in readme


# ── Helper Function Tests ─────────────────────────────────────────────────────


class TestHashFiles:
    """Tests for _hash_files function."""

    def test_consistent_hash(self, temp_dir):
        """Same content should produce same hash."""
        f1 = temp_dir / "test1.sql"
        f1.write_text("SELECT 1")

        hash1 = _hash_files([f1])
        hash2 = _hash_files([f1])

        assert hash1 == hash2

    def test_different_content_different_hash(self, temp_dir):
        """Different content should produce different hash."""
        f1 = temp_dir / "test1.sql"
        f2 = temp_dir / "test2.sql"
        f1.write_text("SELECT 1")
        f2.write_text("SELECT 2")

        hash1 = _hash_files([f1])
        hash2 = _hash_files([f2])

        assert hash1 != hash2

    def test_hash_format(self, temp_dir):
        """Hash should have proper format."""
        f1 = temp_dir / "test1.sql"
        f1.write_text("SELECT 1")

        result = _hash_files([f1])

        assert result.startswith("sha256:")
        assert len(result) == len("sha256:") + 16


class TestMakeRunId:
    """Tests for _make_run_id function."""

    def test_basic_run_id(self, temp_dir):
        """Should generate date-based run ID."""
        runs_dir = temp_dir / "runs"

        run_id = _make_run_id(str(runs_dir), None)

        # Should start with a date pattern YYYY-MM-DD
        import re
        assert re.match(r"\d{4}-\d{2}-\d{2}_\d{3}", run_id)
        assert "_001" in run_id

    def test_run_id_with_label(self, temp_dir):
        """Should include label in run ID."""
        runs_dir = temp_dir / "runs"

        run_id = _make_run_id(str(runs_dir), "test_label")

        assert "test_label" in run_id

    def test_run_id_counter_increment(self, temp_dir):
        """Counter should increment with existing runs."""
        from datetime import timezone
        runs_dir = temp_dir / "runs"
        runs_dir.mkdir()

        # Create existing run using UTC date (what the function uses)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        (runs_dir / f"{today}_001").mkdir()

        run_id = _make_run_id(str(runs_dir), None)

        assert "_002" in run_id


# ── Generate Run Integration Tests ────────────────────────────────────────────


class TestGenerateRun:
    """Integration tests for generate_run function."""

    def test_full_run_generation(self, sql_worksheet_dir, temp_dir):
        """Should generate complete run folder structure."""
        runs_dir = temp_dir / "runs"

        run_path = generate_run(
            sql_dir=str(sql_worksheet_dir),
            runs_dir=str(runs_dir),
            run_label="test",
            sample_limit=100000,
        )

        run_dir = Path(run_path)
        assert run_dir.exists()

        # Check directory structure
        assert (run_dir / "queries").exists()
        assert (run_dir / "results").exists()
        assert (run_dir / "artifacts").exists()

        # Check files
        assert (run_dir / "run_meta.json").exists()
        assert (run_dir / "README.md").exists()
        assert (run_dir / "queries" / "01_profiling_edges.sql").exists()
        assert (run_dir / "queries" / "02_column_profiles.sql").exists()
        assert (run_dir / "queries" / "03_value_overlap.sql").exists()

    def test_nonexistent_sql_dir(self, temp_dir):
        """Should raise error for non-existent SQL directory."""
        with pytest.raises(FileNotFoundError):
            generate_run(
                sql_dir=str(temp_dir / "nonexistent"),
                runs_dir=str(temp_dir / "runs"),
            )

    def test_empty_sql_dir(self, temp_dir):
        """Should raise error for empty SQL directory."""
        empty_dir = temp_dir / "empty"
        empty_dir.mkdir()

        with pytest.raises(ValueError, match="No .sql files found"):
            generate_run(
                sql_dir=str(empty_dir),
                runs_dir=str(temp_dir / "runs"),
            )

    def test_run_meta_content(self, sql_worksheet_dir, temp_dir):
        """run_meta.json should have correct content."""
        runs_dir = temp_dir / "runs"

        run_path = generate_run(
            sql_dir=str(sql_worksheet_dir),
            runs_dir=str(runs_dir),
            sample_limit=50000,
        )

        meta_path = Path(run_path) / "run_meta.json"
        meta = json.loads(meta_path.read_text())

        assert "run_id" in meta
        assert "generated_at" in meta
        assert meta["sample_limit"] == 50000
        assert len(meta["sql_worksheets_ingested"]) == 2  # Two SQL files in fixture
