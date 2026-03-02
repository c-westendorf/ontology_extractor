"""
Tests for rigor_v1/overrides.py

Target coverage: 90%

Tests cover:
- OverrideEdge dataclass
- Normalization helpers (_norm, _norm_cols)
- Loading overrides from YAML
- Saving overrides to YAML
- Upserting edge overrides (add/update)
- Checking if edge is approved
- Checking if edge is rejected
- Handling missing files
- Column matching (single vs list)
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
import yaml

from rigor_v1.overrides import (
    OverrideEdge,
    _norm,
    _norm_cols,
    is_approved,
    is_rejected,
    load_overrides,
    save_overrides,
    upsert_edge_override,
)


# ── Normalization Tests ───────────────────────────────────────────────────────


class TestNormalization:
    """Tests for normalization helper functions."""

    def test_norm_simple(self):
        """Should uppercase and strip."""
        assert _norm("customers") == "CUSTOMERS"

    def test_norm_with_spaces(self):
        """Should strip leading/trailing spaces."""
        assert _norm("  customers  ") == "CUSTOMERS"

    def test_norm_quoted(self):
        """Should strip quotes."""
        assert _norm('"CUSTOMERS"') == "CUSTOMERS"

    def test_norm_quoted_with_spaces(self):
        """Should handle quotes and spaces."""
        assert _norm('  "customers"  ') == "CUSTOMERS"

    def test_norm_cols_none(self):
        """Should return empty list for None."""
        assert _norm_cols(None) == []

    def test_norm_cols_single_string(self):
        """Should handle single string."""
        assert _norm_cols("customer_id") == ["CUSTOMER_ID"]

    def test_norm_cols_list(self):
        """Should handle list of columns."""
        assert _norm_cols(["col1", "col2"]) == ["COL1", "COL2"]

    def test_norm_cols_list_with_empty(self):
        """Should filter out empty strings."""
        assert _norm_cols(["col1", "", "col2"]) == ["COL1", "COL2"]

    def test_norm_cols_list_with_spaces(self):
        """Should filter out whitespace-only strings."""
        assert _norm_cols(["col1", "   ", "col2"]) == ["COL1", "COL2"]


# ── OverrideEdge Dataclass Tests ──────────────────────────────────────────────


class TestOverrideEdge:
    """Tests for OverrideEdge dataclass."""

    def test_default_status(self):
        """Should default to approved status."""
        edge = OverrideEdge(
            from_table="ORDERS",
            from_column="CUSTOMER_ID",
            to_table="CUSTOMERS",
            to_column="ID",
        )
        assert edge.status == "approved"

    def test_rejected_status(self):
        """Should allow rejected status."""
        edge = OverrideEdge(
            from_table="TEMP",
            from_column="REF",
            to_table="OTHER",
            to_column="ID",
            status="rejected",
        )
        assert edge.status == "rejected"

    def test_optional_relation_name(self):
        """Should allow optional relation name."""
        edge = OverrideEdge(
            from_table="ORDERS",
            from_column="CUSTOMER_ID",
            to_table="CUSTOMERS",
            to_column="ID",
            relation_name="hasCustomer",
        )
        assert edge.relation_name == "hasCustomer"

    def test_relation_name_default_none(self):
        """Should default relation name to None."""
        edge = OverrideEdge(
            from_table="A",
            from_column="B_ID",
            to_table="B",
            to_column="ID",
        )
        assert edge.relation_name is None


# ── Load Overrides Tests ──────────────────────────────────────────────────────


class TestLoadOverrides:
    """Tests for load_overrides function."""

    def test_load_existing_file(self, overrides_yaml_file):
        """Should load overrides from existing YAML file."""
        data = load_overrides(str(overrides_yaml_file))

        assert "approve" in data
        assert "reject" in data
        assert len(data["approve"]) == 2
        assert len(data["reject"]) == 1

    def test_load_nonexistent_file(self, temp_dir):
        """Should return empty structure for non-existent file."""
        data = load_overrides(str(temp_dir / "nonexistent.yaml"))

        assert data == {"approve": [], "reject": [], "rename": []}

    def test_load_empty_file(self, temp_dir):
        """Should handle empty YAML file."""
        empty_file = temp_dir / "empty.yaml"
        empty_file.write_text("")

        data = load_overrides(str(empty_file))

        assert data == {"approve": [], "reject": [], "rename": []}

    def test_load_partial_file(self, temp_dir):
        """Should handle YAML with only some sections."""
        partial_file = temp_dir / "partial.yaml"
        partial_file.write_text(yaml.safe_dump({"approve": []}))

        data = load_overrides(str(partial_file))

        assert data["approve"] == []
        assert data["reject"] == []
        assert data["rename"] == []


# ── Save Overrides Tests ──────────────────────────────────────────────────────


class TestSaveOverrides:
    """Tests for save_overrides function."""

    def test_save_creates_file(self, temp_dir):
        """Should create YAML file."""
        path = temp_dir / "new_overrides.yaml"
        data = {"approve": [], "reject": [], "rename": []}

        save_overrides(str(path), data)

        assert path.exists()

    def test_save_creates_parent_dirs(self, temp_dir):
        """Should create parent directories if needed."""
        path = temp_dir / "nested" / "dir" / "overrides.yaml"
        data = {"approve": [], "reject": [], "rename": []}

        save_overrides(str(path), data)

        assert path.exists()

    def test_save_content(self, temp_dir):
        """Should save correct content."""
        path = temp_dir / "overrides.yaml"
        data = {
            "approve": [
                {
                    "from": {"table": "A", "columns": ["B_ID"]},
                    "to": {"table": "B", "columns": ["ID"]},
                }
            ],
            "reject": [],
            "rename": [],
        }

        save_overrides(str(path), data)

        loaded = yaml.safe_load(path.read_text())
        assert loaded["approve"][0]["from"]["table"] == "A"

    def test_save_overwrites_existing(self, temp_dir):
        """Should overwrite existing file."""
        path = temp_dir / "overrides.yaml"
        path.write_text("old content")

        data = {"approve": [{"new": "data"}], "reject": [], "rename": []}
        save_overrides(str(path), data)

        content = path.read_text()
        assert "old content" not in content
        assert "new" in content


# ── Upsert Edge Override Tests ────────────────────────────────────────────────


class TestUpsertEdgeOverride:
    """Tests for upsert_edge_override function."""

    def test_add_approved_edge(self):
        """Should add new approved edge."""
        data = {"approve": [], "reject": [], "rename": []}
        edge = OverrideEdge(
            from_table="ORDERS",
            from_column="CUSTOMER_ID",
            to_table="CUSTOMERS",
            to_column="ID",
            status="approved",
        )

        result = upsert_edge_override(data, edge)

        assert len(result["approve"]) == 1
        assert result["approve"][0]["from"]["table"] == "ORDERS"

    def test_add_rejected_edge(self):
        """Should add new rejected edge."""
        data = {"approve": [], "reject": [], "rename": []}
        edge = OverrideEdge(
            from_table="TEMP",
            from_column="REF",
            to_table="OTHER",
            to_column="ID",
            status="rejected",
        )

        result = upsert_edge_override(data, edge)

        assert len(result["reject"]) == 1
        assert result["reject"][0]["from"]["table"] == "TEMP"

    def test_add_with_relation_name(self):
        """Should include relation name when provided."""
        data = {"approve": [], "reject": [], "rename": []}
        edge = OverrideEdge(
            from_table="ORDERS",
            from_column="CUSTOMER_ID",
            to_table="CUSTOMERS",
            to_column="ID",
            relation_name="hasCustomer",
        )

        result = upsert_edge_override(data, edge)

        assert result["approve"][0]["relation"] == "hasCustomer"

    def test_upsert_removes_from_both_lists(self):
        """Should remove existing edge from both approve and reject."""
        data = {
            "approve": [
                {
                    "from": {"table": "ORDERS", "columns": ["CUSTOMER_ID"]},
                    "to": {"table": "CUSTOMERS", "columns": ["ID"]},
                }
            ],
            "reject": [],
            "rename": [],
        }
        edge = OverrideEdge(
            from_table="ORDERS",
            from_column="CUSTOMER_ID",
            to_table="CUSTOMERS",
            to_column="ID",
            status="rejected",  # Changing from approved to rejected
        )

        result = upsert_edge_override(data, edge)

        assert len(result["approve"]) == 0
        assert len(result["reject"]) == 1

    def test_upsert_updates_existing(self):
        """Should update existing edge (not duplicate)."""
        data = {
            "approve": [
                {
                    "from": {"table": "A", "columns": ["B_ID"]},
                    "to": {"table": "B", "columns": ["ID"]},
                }
            ],
            "reject": [],
            "rename": [],
        }
        edge = OverrideEdge(
            from_table="A",
            from_column="B_ID",
            to_table="B",
            to_column="ID",
            relation_name="newRelation",  # Adding relation name
        )

        result = upsert_edge_override(data, edge)

        # Should still have only 1 approved edge
        assert len(result["approve"]) == 1
        assert result["approve"][0]["relation"] == "newRelation"

    def test_upsert_case_insensitive(self):
        """Should match edges case-insensitively."""
        data = {
            "approve": [
                {
                    "from": {"table": "orders", "columns": ["customer_id"]},
                    "to": {"table": "customers", "columns": ["id"]},
                }
            ],
            "reject": [],
            "rename": [],
        }
        edge = OverrideEdge(
            from_table="ORDERS",  # Different case
            from_column="CUSTOMER_ID",
            to_table="CUSTOMERS",
            to_column="ID",
            status="rejected",
        )

        result = upsert_edge_override(data, edge)

        # Should have removed the old entry
        assert len(result["approve"]) == 0
        assert len(result["reject"]) == 1


# ── Is Approved Tests ─────────────────────────────────────────────────────────


class TestIsApproved:
    """Tests for is_approved function."""

    def test_approved_edge(self, sample_overrides_data):
        """Should return True for approved edge."""
        result = is_approved(
            sample_overrides_data,
            "ORDERS",
            "CUSTOMER_ID",
            "CUSTOMERS",
            "ID",
        )
        assert result is True

    def test_not_approved_edge(self, sample_overrides_data):
        """Should return False for non-approved edge."""
        result = is_approved(
            sample_overrides_data,
            "UNKNOWN",
            "REF_ID",
            "OTHER",
            "ID",
        )
        assert result is False

    def test_rejected_not_approved(self, sample_overrides_data):
        """Should return False for rejected edge."""
        result = is_approved(
            sample_overrides_data,
            "TEMP_TABLE",
            "REF_ID",
            "OTHER_TABLE",
            "ID",
        )
        assert result is False

    def test_case_insensitive_check(self, sample_overrides_data):
        """Should match case-insensitively."""
        result = is_approved(
            sample_overrides_data,
            "orders",  # lowercase
            "customer_id",
            "customers",
            "id",
        )
        assert result is True


# ── Is Rejected Tests ─────────────────────────────────────────────────────────


class TestIsRejected:
    """Tests for is_rejected function."""

    def test_rejected_edge(self, sample_overrides_data):
        """Should return True for rejected edge."""
        result = is_rejected(
            sample_overrides_data,
            "TEMP_TABLE",
            "REF_ID",
            "OTHER_TABLE",
            "ID",
        )
        assert result is True

    def test_not_rejected_edge(self, sample_overrides_data):
        """Should return False for non-rejected edge."""
        result = is_rejected(
            sample_overrides_data,
            "ORDERS",
            "CUSTOMER_ID",
            "CUSTOMERS",
            "ID",
        )
        assert result is False

    def test_unknown_edge(self, sample_overrides_data):
        """Should return False for unknown edge."""
        result = is_rejected(
            sample_overrides_data,
            "UNKNOWN",
            "REF_ID",
            "OTHER",
            "ID",
        )
        assert result is False

    def test_case_insensitive_check(self, sample_overrides_data):
        """Should match case-insensitively."""
        result = is_rejected(
            sample_overrides_data,
            "temp_table",  # lowercase
            "ref_id",
            "other_table",
            "id",
        )
        assert result is True


# ── Edge Cases ────────────────────────────────────────────────────────────────


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_empty_data(self):
        """Should handle empty override data."""
        data = {"approve": [], "reject": [], "rename": []}

        assert is_approved(data, "A", "B", "C", "D") is False
        assert is_rejected(data, "A", "B", "C", "D") is False

    def test_column_field_as_string(self):
        """Should handle 'column' field (singular) as well as 'columns'."""
        data = {
            "approve": [
                {
                    "from": {"table": "A", "column": "B_ID"},  # singular
                    "to": {"table": "B", "column": "ID"},
                }
            ],
            "reject": [],
            "rename": [],
        }

        result = is_approved(data, "A", "B_ID", "B", "ID")
        assert result is True

    def test_missing_columns_field(self):
        """Should handle missing columns field."""
        data = {
            "approve": [
                {
                    "from": {"table": "A"},  # no columns
                    "to": {"table": "B"},
                }
            ],
            "reject": [],
            "rename": [],
        }

        # Should not match since columns don't match
        result = is_approved(data, "A", "X", "B", "Y")
        assert result is False

    def test_quoted_identifiers(self):
        """Should handle quoted identifiers in data."""
        data = {
            "approve": [
                {
                    "from": {"table": '"ORDERS"', "columns": ['"CUSTOMER_ID"']},
                    "to": {"table": '"CUSTOMERS"', "columns": ['"ID"']},
                }
            ],
            "reject": [],
            "rename": [],
        }

        result = is_approved(data, "ORDERS", "CUSTOMER_ID", "CUSTOMERS", "ID")
        assert result is True

    def test_roundtrip_save_load(self, temp_dir, sample_overrides_data):
        """Should maintain data integrity through save/load cycle."""
        path = temp_dir / "roundtrip.yaml"

        save_overrides(str(path), sample_overrides_data)
        loaded = load_overrides(str(path))

        assert len(loaded["approve"]) == len(sample_overrides_data["approve"])
        assert len(loaded["reject"]) == len(sample_overrides_data["reject"])

    def test_upsert_multiple_edges(self):
        """Should handle multiple upsert operations."""
        data = {"approve": [], "reject": [], "rename": []}

        edge1 = OverrideEdge("A", "B_ID", "B", "ID")
        edge2 = OverrideEdge("C", "D_ID", "D", "ID")
        edge3 = OverrideEdge("E", "F_ID", "F", "ID", status="rejected")

        data = upsert_edge_override(data, edge1)
        data = upsert_edge_override(data, edge2)
        data = upsert_edge_override(data, edge3)

        assert len(data["approve"]) == 2
        assert len(data["reject"]) == 1


# ── Integration Tests ─────────────────────────────────────────────────────────


class TestIntegration:
    """Integration tests for override workflow."""

    def test_full_workflow(self, temp_dir):
        """Test complete workflow: create, save, load, modify, save."""
        path = temp_dir / "workflow_test.yaml"

        # Create initial data
        data = {"approve": [], "reject": [], "rename": []}

        # Add an approved edge
        edge1 = OverrideEdge("ORDERS", "CUSTOMER_ID", "CUSTOMERS", "ID", relation_name="hasCustomer")
        data = upsert_edge_override(data, edge1)

        # Save
        save_overrides(str(path), data)

        # Load
        loaded = load_overrides(str(path))

        # Verify
        assert is_approved(loaded, "ORDERS", "CUSTOMER_ID", "CUSTOMERS", "ID")
        assert not is_rejected(loaded, "ORDERS", "CUSTOMER_ID", "CUSTOMERS", "ID")

        # Change to rejected
        edge2 = OverrideEdge("ORDERS", "CUSTOMER_ID", "CUSTOMERS", "ID", status="rejected")
        loaded = upsert_edge_override(loaded, edge2)

        # Save again
        save_overrides(str(path), loaded)

        # Load again
        final = load_overrides(str(path))

        # Verify change
        assert not is_approved(final, "ORDERS", "CUSTOMER_ID", "CUSTOMERS", "ID")
        assert is_rejected(final, "ORDERS", "CUSTOMER_ID", "CUSTOMERS", "ID")

    def test_with_fixture_file(self):
        """Test with actual fixture file if available."""
        fixtures_dir = Path(__file__).parent / "fixtures" / "schemas"
        overrides_file = fixtures_dir / "sample_overrides.yaml"

        if not overrides_file.exists():
            pytest.skip("Fixture file not found")

        data = load_overrides(str(overrides_file))

        # Check expected content
        assert len(data["approve"]) >= 1
        assert "table_classification" in data
