"""
Tests for rigor_sf/run_loader.py

Target coverage: 85%

Tests cover:
- RunLoader initialization and validation
- Edge profile loading from CSV
- Column profile loading from CSV
- Direction hints loading from CSV
- Merge relationships with profiling stats
- Direction correction application
- Override status application
- Meta stamping
- Data quality report generation
- Error handling for missing files
"""

from __future__ import annotations

import json
import warnings
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from rigor_sf.run_loader import (
    ColumnProfile,
    DirectionHint,
    EdgeProfile,
    RunLoader,
)


# ── RunLoader Initialization Tests ────────────────────────────────────────────


class TestRunLoaderInit:
    """Tests for RunLoader initialization."""

    def test_init_with_valid_dir(self, run_dir_with_results):
        """Should initialize with valid run directory."""
        loader = RunLoader(str(run_dir_with_results))
        assert loader.run_dir == run_dir_with_results

    def test_init_with_nonexistent_dir(self):
        """Should raise error for non-existent directory."""
        with pytest.raises(FileNotFoundError):
            RunLoader("/nonexistent/path")

    def test_meta_property(self, run_dir_with_results):
        """Should load meta from run_meta.json."""
        loader = RunLoader(str(run_dir_with_results))
        meta = loader.meta

        assert meta["run_id"] == "2026-03-01_001_test"
        assert "generated_at" in meta

    def test_meta_missing(self, temp_dir):
        """Should raise error when run_meta.json is missing."""
        run_dir = temp_dir / "runs" / "no_meta"
        run_dir.mkdir(parents=True)

        loader = RunLoader(str(run_dir))
        with pytest.raises(FileNotFoundError, match="run_meta.json not found"):
            _ = loader.meta


# ── Edge Profile Loading Tests ────────────────────────────────────────────────


class TestEdgeProfiles:
    """Tests for edge profile loading."""

    def test_load_edge_profiles(self, run_dir_with_results):
        """Should load edge profiles from CSV."""
        loader = RunLoader(str(run_dir_with_results))
        profiles = loader.edge_profiles

        assert len(profiles) == 2
        # Check key format (uppercase)
        key = ("ORDERS", "CUSTOMER_ID", "CUSTOMERS", "ID")
        assert key in profiles

        profile = profiles[key]
        assert profile.match_rate == 0.99
        assert profile.pk_unique_rate == 1.0
        assert profile.fk_null_rate == 0.02
        assert profile.profiled is True

    def test_edge_profiles_missing_csv(self, run_dir_no_results):
        """Should warn and return empty dict when CSV is missing."""
        loader = RunLoader(str(run_dir_no_results))

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            profiles = loader.edge_profiles

            assert len(w) == 1
            assert "profiling_edges.csv not found" in str(w[0].message)
            assert profiles == {}

    def test_edge_profile_dataclass(self, run_dir_with_results):
        """EdgeProfile should have all expected fields."""
        loader = RunLoader(str(run_dir_with_results))
        profiles = loader.edge_profiles

        key = ("ORDERS", "CUSTOMER_ID", "CUSTOMERS", "ID")
        profile = profiles[key]

        assert isinstance(profile, EdgeProfile)
        assert profile.from_table == "ORDERS"
        assert profile.from_column == "CUSTOMER_ID"
        assert profile.to_table == "CUSTOMERS"
        assert profile.to_column == "ID"
        assert profile.sample_rows == 10000
        assert profile.fk_nonnull == 9800
        assert profile.match_count == 9700
        assert profile.confidence_sql == 0.85
        assert profile.frequency == 5


# ── Column Profile Loading Tests ──────────────────────────────────────────────


class TestColumnProfiles:
    """Tests for column profile loading."""

    def test_load_column_profiles(self, run_dir_with_results):
        """Should load column profiles from CSV."""
        loader = RunLoader(str(run_dir_with_results))
        profiles = loader.column_profiles

        assert len(profiles) == 2
        key = ("ORDERS", "CUSTOMER_ID")
        assert key in profiles

        profile = profiles[key]
        assert profile.null_rate == 0.02
        assert profile.distinct_count == 5000

    def test_column_profiles_missing_csv(self, run_dir_no_results):
        """Should warn and return empty dict when CSV is missing."""
        loader = RunLoader(str(run_dir_no_results))

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            profiles = loader.column_profiles

            assert len(w) == 1
            assert "column_profiles.csv not found" in str(w[0].message)
            assert profiles == {}

    def test_column_profile_dataclass(self, run_dir_with_results):
        """ColumnProfile should have all expected fields."""
        loader = RunLoader(str(run_dir_with_results))
        profiles = loader.column_profiles

        key = ("CUSTOMERS", "ID")
        profile = profiles[key]

        assert isinstance(profile, ColumnProfile)
        assert profile.table_name == "CUSTOMERS"
        assert profile.column_name == "ID"
        assert profile.total_rows == 5000
        assert profile.non_null_count == 5000
        assert profile.null_rate == 0.0
        assert profile.cardinality_ratio == 1.0


# ── Direction Hints Loading Tests ─────────────────────────────────────────────


class TestDirectionHints:
    """Tests for direction hint loading."""

    def test_load_direction_hints(self, run_dir_with_results):
        """Should load direction hints when value_overlap.csv exists."""
        # Create value_overlap.csv
        overlap_df = pd.DataFrame([{
            "table_a": "TABLE_A",
            "col_a": "REF_CODE",
            "table_b": "TABLE_B",
            "col_b": "REF_CODE",
            "a_distinct": 1000,
            "b_distinct": 100,
            "a_in_b_count": 950,
            "b_in_a_count": 100,
            "a_coverage": 0.95,
            "b_coverage": 1.0,
            "direction_suggestion": "TABLE_B is likely the PARENT (referred) table",
        }])
        overlap_df.to_csv(
            run_dir_with_results / "results" / "value_overlap.csv",
            index=False
        )

        loader = RunLoader(str(run_dir_with_results))
        hints = loader.direction_hints

        # Should have both directions as keys
        key1 = ("TABLE_A", "REF_CODE", "TABLE_B", "REF_CODE")
        key2 = ("TABLE_B", "REF_CODE", "TABLE_A", "REF_CODE")
        assert key1 in hints
        assert key2 in hints

    def test_direction_hints_missing_csv(self, run_dir_with_results):
        """Should return empty dict when value_overlap.csv is missing."""
        loader = RunLoader(str(run_dir_with_results))
        hints = loader.direction_hints

        # No value_overlap.csv in fixture by default
        assert hints == {}


# ── Merge Relationships Tests ─────────────────────────────────────────────────


class TestMergeRelationships:
    """Tests for merge_relationships method."""

    def test_merge_with_profiling(self, run_dir_with_results, raw_relationships_df):
        """Should merge profiling stats into relationships DataFrame."""
        loader = RunLoader(str(run_dir_with_results))
        result = loader.merge_relationships(raw_relationships_df)

        # Check profiling stats were merged
        orders_row = result[result["from_table"] == "ORDERS"].iloc[0]
        assert orders_row["match_rate"] == 0.99
        assert orders_row["pk_unique_rate"] == 1.0
        assert orders_row["fk_null_rate"] == 0.02
        assert orders_row["frequency"] == 5

    def test_merge_marks_not_profiled(self, run_dir_no_results):
        """Should mark edges as not_profiled when no profiling data."""
        loader = RunLoader(str(run_dir_no_results))
        df = pd.DataFrame([{
            "from_table": "UNKNOWN",
            "from_column": "REF_ID",
            "to_table": "OTHER",
            "to_column": "ID",
            "confidence_sql": 0.7,
            "evidence": "test",
            "status": "proposed",
            "match_rate": None,
            "pk_unique_rate": None,
            "fk_null_rate": None,
        }])

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = loader.merge_relationships(df)

        assert result.iloc[0]["data_quality_flag"] == "not_profiled"

    def test_merge_adds_missing_columns(self, run_dir_with_results):
        """Should add missing columns to DataFrame."""
        loader = RunLoader(str(run_dir_with_results))
        df = pd.DataFrame([{
            "from_table": "ORDERS",
            "from_column": "CUSTOMER_ID",
            "to_table": "CUSTOMERS",
            "to_column": "ID",
            "confidence_sql": 0.85,
            "evidence": "test",
            "status": "proposed",
        }])

        result = loader.merge_relationships(df)

        # Should have all v1 columns
        expected_cols = [
            "from_table", "from_columns", "from_column",
            "to_table", "to_columns", "to_column",
            "confidence_sql", "frequency",
            "match_rate", "pk_unique_rate", "fk_null_rate",
            "status", "evidence", "data_quality_flag",
        ]
        for col in expected_cols:
            assert col in result.columns

    def test_merge_writes_artifact(self, run_dir_with_results, raw_relationships_df):
        """Should write merged CSV to artifacts directory."""
        loader = RunLoader(str(run_dir_with_results))
        loader.merge_relationships(raw_relationships_df)

        artifact_path = run_dir_with_results / "artifacts" / "inferred_relationships.csv"
        assert artifact_path.exists()

        # Verify content
        df = pd.read_csv(artifact_path)
        assert len(df) == 2


# ── Override Application Tests ────────────────────────────────────────────────


class TestApplyStatus:
    """Tests for _apply_status method."""

    def test_apply_approved_status(self, run_dir_with_results, raw_relationships_df):
        """Should apply approved status from overrides."""
        loader = RunLoader(str(run_dir_with_results))
        overrides = [{
            "from": {"table": "ORDERS", "columns": ["CUSTOMER_ID"]},
            "to": {"table": "CUSTOMERS", "columns": ["ID"]},
        }]

        result = loader.merge_relationships(
            raw_relationships_df,
            overrides_approved=overrides
        )

        orders_row = result[result["from_table"] == "ORDERS"].iloc[0]
        assert orders_row["status"] == "approved"

    def test_apply_rejected_status(self, run_dir_with_results, raw_relationships_df):
        """Should apply rejected status from overrides."""
        loader = RunLoader(str(run_dir_with_results))
        overrides = [{
            "from": {"table": "ORDER_ITEMS", "columns": ["ORDER_ID"]},
            "to": {"table": "ORDERS", "columns": ["ID"]},
        }]

        result = loader.merge_relationships(
            raw_relationships_df,
            overrides_rejected=overrides
        )

        items_row = result[result["from_table"] == "ORDER_ITEMS"].iloc[0]
        assert items_row["status"] == "rejected"

    def test_static_apply_status_method(self):
        """Test _apply_status static method directly."""
        df = pd.DataFrame([{
            "from_table": "A",
            "from_column": "B_ID",
            "to_table": "B",
            "to_column": "ID",
            "status": "proposed",
        }])
        overrides = [{
            "from": {"table": "A", "columns": ["B_ID"]},
            "to": {"table": "B", "columns": ["ID"]},
        }]

        result = RunLoader._apply_status(df, overrides, "approved")
        assert result.iloc[0]["status"] == "approved"


# ── Direction Correction Tests ────────────────────────────────────────────────


class TestDirectionCorrection:
    """Tests for direction correction application."""

    def test_direction_correction_applied(self, run_dir_with_results):
        """Should flip direction when value_overlap suggests correction."""
        # Create value_overlap.csv with direction hint
        overlap_df = pd.DataFrame([{
            "table_a": "WRONG_FROM",
            "col_a": "REF_ID",
            "table_b": "CORRECT_TO",
            "col_b": "ID",
            "a_distinct": 1000,
            "b_distinct": 100,
            "a_in_b_count": 950,
            "b_in_a_count": 100,
            "a_coverage": 0.95,
            "b_coverage": 1.0,
            "direction_suggestion": "wrong_from is likely the parent (referred) table",
        }])
        overlap_df.to_csv(
            run_dir_with_results / "results" / "value_overlap.csv",
            index=False
        )

        loader = RunLoader(str(run_dir_with_results))

        # Create a DataFrame with wrong direction
        df = pd.DataFrame([{
            "from_table": "WRONG_FROM",
            "from_column": "REF_ID",
            "to_table": "CORRECT_TO",
            "to_column": "ID",
            "confidence_sql": 0.6,
            "evidence": "test",
            "status": "proposed",
            "match_rate": None,
            "pk_unique_rate": None,
            "fk_null_rate": None,
        }])

        result = loader.merge_relationships(df)

        # Direction should be flipped
        row = result.iloc[0]
        assert row["from_table"] == "CORRECT_TO"
        assert row["to_table"] == "WRONG_FROM"
        assert "direction_corrected" in str(row["data_quality_flag"])


# ── Meta Stamping Tests ───────────────────────────────────────────────────────


class TestMetaStamping:
    """Tests for downstream run stamping."""

    def test_stamp_downstream_run(self, run_dir_with_results, raw_relationships_df):
        """Should update run_meta.json with downstream_run timestamp."""
        loader = RunLoader(str(run_dir_with_results))
        loader.merge_relationships(raw_relationships_df)

        # Re-read meta to check stamping
        meta_path = run_dir_with_results / "run_meta.json"
        meta = json.loads(meta_path.read_text())

        assert meta["downstream_run"] is not None
        # Should be a valid ISO timestamp
        datetime.fromisoformat(meta["downstream_run"].replace("Z", "+00:00"))


# ── Summary Tests ─────────────────────────────────────────────────────────────


class TestSummary:
    """Tests for summary method."""

    def test_summary_output(self, run_dir_with_results):
        """Should return human-readable summary."""
        loader = RunLoader(str(run_dir_with_results))
        summary = loader.summary()

        assert "2026-03-01_001_test" in summary
        assert "Edge profiles loaded" in summary
        assert "Column profiles" in summary


# ── Data Quality Report Tests ─────────────────────────────────────────────────


class TestDataQualityReport:
    """Tests for data_quality_report method."""

    def test_report_structure(self, run_dir_with_results):
        """Should return proper report structure."""
        loader = RunLoader(str(run_dir_with_results))
        report = loader.data_quality_report()

        assert "run_id" in report
        assert "generated_at" in report
        assert "issues" in report
        assert "total_issues" in report
        assert "warnings" in report
        assert "errors" in report

    def test_high_null_rate_detection(self, run_dir_with_results):
        """Should detect columns with high null rates."""
        # Add a column with high null rate
        cols_df = pd.read_csv(run_dir_with_results / "results" / "column_profiles.csv")
        new_row = pd.DataFrame([{
            "table_name": "TEST_TABLE",
            "column_name": "BAD_COLUMN",
            "total_rows": 1000,
            "non_null_count": 500,
            "null_rate": 0.50,  # 50% null
            "distinct_count": 100,
            "cardinality_ratio": 0.2,
            "min_val": "1",
            "max_val": "100",
            "inferred_type": "INTEGER",
        }])
        cols_df = pd.concat([cols_df, new_row], ignore_index=True)
        cols_df.to_csv(run_dir_with_results / "results" / "column_profiles.csv", index=False)

        loader = RunLoader(str(run_dir_with_results))
        report = loader.data_quality_report()

        # Should have a high_null_rate issue
        null_issues = [i for i in report["issues"] if i["type"] == "high_null_rate"]
        assert len(null_issues) >= 1
        assert any(i["table"] == "TEST_TABLE" for i in null_issues)

    def test_low_match_rate_detection(self, run_dir_with_results):
        """Should detect edges with low match rates."""
        # Add an edge with low match rate
        edges_df = pd.read_csv(run_dir_with_results / "results" / "profiling_edges.csv")
        new_row = pd.DataFrame([{
            "from_table": "BAD_TABLE",
            "from_column": "REF_ID",
            "to_table": "OTHER",
            "to_column": "ID",
            "sample_rows": 1000,
            "fk_nonnull": 1000,
            "match_count": 300,
            "match_rate": 0.30,  # Only 30% match
            "pk_distinct": 500,
            "pk_total": 500,
            "pk_unique_rate": 1.0,
            "fk_null_rate": 0.0,
            "confidence_sql": 0.6,
            "frequency": 1,
            "evidence": "test",
        }])
        edges_df = pd.concat([edges_df, new_row], ignore_index=True)
        edges_df.to_csv(run_dir_with_results / "results" / "profiling_edges.csv", index=False)

        loader = RunLoader(str(run_dir_with_results))
        report = loader.data_quality_report()

        # Should have a low_match_rate issue
        match_issues = [i for i in report["issues"] if i["type"] == "low_match_rate"]
        assert len(match_issues) >= 1


# ── Dataclass Tests ───────────────────────────────────────────────────────────


class TestDataclasses:
    """Tests for result dataclasses."""

    def test_edge_profile_defaults(self):
        """EdgeProfile should have correct defaults."""
        profile = EdgeProfile(
            from_table="A",
            from_column="B_ID",
            to_table="B",
            to_column="ID",
            sample_rows=1000,
            fk_nonnull=900,
            match_count=850,
            match_rate=0.85,
            pk_distinct=500,
            pk_total=500,
            pk_unique_rate=1.0,
            fk_null_rate=0.10,
            confidence_sql=0.85,
            frequency=1,
            evidence="test",
        )
        assert profile.profiled is True

    def test_column_profile_optional_fields(self):
        """ColumnProfile should handle optional fields."""
        profile = ColumnProfile(
            table_name="TEST",
            column_name="COL",
            total_rows=100,
            non_null_count=100,
            null_rate=0.0,
            distinct_count=50,
            cardinality_ratio=0.5,
            min_val=None,
            max_val=None,
            inferred_type=None,
        )
        assert profile.min_val is None
        assert profile.max_val is None
        assert profile.inferred_type is None

    def test_direction_hint_structure(self):
        """DirectionHint should hold all direction data."""
        hint = DirectionHint(
            table_a="A",
            col_a="CODE",
            table_b="B",
            col_b="CODE",
            a_coverage=0.95,
            b_coverage=1.0,
            direction_suggestion="B is likely the PARENT",
        )
        assert hint.a_coverage == 0.95
        assert "PARENT" in hint.direction_suggestion


# ── Edge Cases ────────────────────────────────────────────────────────────────


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_case_insensitive_key_lookup(self, run_dir_with_results):
        """Should handle case differences in table/column names."""
        loader = RunLoader(str(run_dir_with_results))

        # Create DataFrame with lowercase names
        df = pd.DataFrame([{
            "from_table": "orders",  # lowercase
            "from_column": "customer_id",
            "to_table": "customers",
            "to_column": "id",
            "confidence_sql": 0.85,
            "evidence": "test",
            "status": "proposed",
            "match_rate": None,
            "pk_unique_rate": None,
            "fk_null_rate": None,
        }])

        result = loader.merge_relationships(df)

        # Should still find the profile (uppercase in CSV)
        assert result.iloc[0]["match_rate"] == 0.99

    def test_reverse_direction_lookup(self, run_dir_with_results):
        """Should find profile even if direction is reversed in DataFrame."""
        loader = RunLoader(str(run_dir_with_results))

        # Create DataFrame with reversed direction
        df = pd.DataFrame([{
            "from_table": "CUSTOMERS",  # reversed
            "from_column": "ID",
            "to_table": "ORDERS",
            "to_column": "CUSTOMER_ID",
            "confidence_sql": 0.85,
            "evidence": "test",
            "status": "proposed",
            "match_rate": None,
            "pk_unique_rate": None,
            "fk_null_rate": None,
        }])

        result = loader.merge_relationships(df)

        # Should still find the profile via reverse lookup
        assert result.iloc[0]["match_rate"] == 0.99

    def test_empty_dataframe(self, run_dir_with_results):
        """Should handle empty input DataFrame."""
        loader = RunLoader(str(run_dir_with_results))
        df = pd.DataFrame(columns=[
            "from_table", "from_column", "to_table", "to_column",
            "confidence_sql", "evidence", "status",
            "match_rate", "pk_unique_rate", "fk_null_rate",
        ])

        result = loader.merge_relationships(df)
        assert len(result) == 0
