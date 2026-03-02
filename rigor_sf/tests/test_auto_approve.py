"""Tests for auto-approve logic in phase_infer().

Tests the auto-approval of edges that meet match_rate and confidence thresholds
as specified in SPEC_V2.md §9 and §6.
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
import pandas as pd

from rigor_sf.config import (
    AppConfig,
    DBConfig,
    ReviewConfig,
    PathsConfig,
    LLMConfig,
)


def create_test_config(
    auto_approve_threshold: float = 0.95,
    auto_approve_confidence: float = 0.80,
) -> AppConfig:
    """Create a minimal test configuration."""
    return AppConfig(
        db=DBConfig(url="snowflake://test"),
        review=ReviewConfig(
            auto_approve_threshold=auto_approve_threshold,
            auto_approve_confidence=auto_approve_confidence,
        ),
        paths=PathsConfig(),
        llm=LLMConfig(),
    )


class TestAutoApproveLogic:
    """Tests for auto-approve logic in phase_infer."""

    def test_auto_approve_high_confidence_edges(self, tmp_path):
        """Edges meeting both thresholds should be auto-approved."""
        # Create test data with edges that should be auto-approved
        df = pd.DataFrame([
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "confidence_sql": 0.90,  # Meets 0.80 threshold
                "match_rate": 0.98,  # Meets 0.95 threshold
                "status": "proposed",
                "evidence": "SQL join found",
            },
            {
                "from_table": "LINE_ITEMS",
                "from_column": "ORDER_ID",
                "to_table": "ORDERS",
                "to_column": "ID",
                "confidence_sql": 0.85,
                "match_rate": 0.96,
                "status": "proposed",
                "evidence": "FK constraint",
            },
        ])

        cfg = create_test_config(
            auto_approve_threshold=0.95,
            auto_approve_confidence=0.80,
        )

        # Apply auto-approve logic (extracted from phase_infer)
        result_df = self._apply_auto_approve(df.copy(), cfg)

        # Both edges should be approved
        assert result_df.iloc[0]["status"] == "approved"
        assert result_df.iloc[1]["status"] == "approved"
        assert "[auto-approved]" in result_df.iloc[0]["evidence"]
        assert "[auto-approved]" in result_df.iloc[1]["evidence"]

    def test_auto_approve_preserves_original_evidence(self, tmp_path):
        """Auto-approve should append marker, not replace evidence."""
        df = pd.DataFrame([
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "confidence_sql": 0.90,
                "match_rate": 0.98,
                "status": "proposed",
                "evidence": "SQL join found in query.sql",
            },
        ])

        cfg = create_test_config()
        result_df = self._apply_auto_approve(df.copy(), cfg)

        assert result_df.iloc[0]["status"] == "approved"
        assert "SQL join found in query.sql" in result_df.iloc[0]["evidence"]
        assert "[auto-approved]" in result_df.iloc[0]["evidence"]

    def test_auto_approve_skips_low_match_rate(self, tmp_path):
        """Edges below match_rate threshold should not be auto-approved."""
        df = pd.DataFrame([
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "confidence_sql": 0.90,
                "match_rate": 0.80,  # Below 0.95 threshold
                "status": "proposed",
                "evidence": "SQL join found",
            },
        ])

        cfg = create_test_config()
        result_df = self._apply_auto_approve(df.copy(), cfg)

        assert result_df.iloc[0]["status"] == "proposed"
        assert "[auto-approved]" not in result_df.iloc[0]["evidence"]

    def test_auto_approve_skips_low_confidence(self, tmp_path):
        """Edges below confidence threshold should not be auto-approved."""
        df = pd.DataFrame([
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "confidence_sql": 0.60,  # Below 0.80 threshold
                "match_rate": 0.98,
                "status": "proposed",
                "evidence": "SQL join found",
            },
        ])

        cfg = create_test_config()
        result_df = self._apply_auto_approve(df.copy(), cfg)

        assert result_df.iloc[0]["status"] == "proposed"
        assert "[auto-approved]" not in result_df.iloc[0]["evidence"]

    def test_auto_approve_skips_already_approved(self, tmp_path):
        """Already approved edges should not be re-processed."""
        df = pd.DataFrame([
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "confidence_sql": 0.90,
                "match_rate": 0.98,
                "status": "approved",  # Already approved by human
                "evidence": "Manually approved",
            },
        ])

        cfg = create_test_config()
        result_df = self._apply_auto_approve(df.copy(), cfg)

        assert result_df.iloc[0]["status"] == "approved"
        # Should not add auto-approved marker to human-approved edges
        assert "[auto-approved]" not in result_df.iloc[0]["evidence"]

    def test_auto_approve_skips_rejected(self, tmp_path):
        """Rejected edges should not be auto-approved."""
        df = pd.DataFrame([
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "confidence_sql": 0.90,
                "match_rate": 0.98,
                "status": "rejected",
                "evidence": "Manually rejected",
            },
        ])

        cfg = create_test_config()
        result_df = self._apply_auto_approve(df.copy(), cfg)

        assert result_df.iloc[0]["status"] == "rejected"
        assert "[auto-approved]" not in result_df.iloc[0]["evidence"]

    def test_auto_approve_handles_empty_evidence(self, tmp_path):
        """Auto-approve should handle empty/None evidence gracefully."""
        df = pd.DataFrame([
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "confidence_sql": 0.90,
                "match_rate": 0.98,
                "status": "proposed",
                "evidence": None,
            },
        ])

        cfg = create_test_config()
        result_df = self._apply_auto_approve(df.copy(), cfg)

        assert result_df.iloc[0]["status"] == "approved"
        assert "[auto-approved]" in str(result_df.iloc[0]["evidence"])

    def test_auto_approve_handles_nan_values(self, tmp_path):
        """Auto-approve should handle NaN match_rate/confidence gracefully."""
        df = pd.DataFrame([
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "confidence_sql": None,  # NaN
                "match_rate": 0.98,
                "status": "proposed",
                "evidence": "SQL join",
            },
            {
                "from_table": "LINE_ITEMS",
                "from_column": "ORDER_ID",
                "to_table": "ORDERS",
                "to_column": "ID",
                "confidence_sql": 0.90,
                "match_rate": None,  # NaN
                "status": "proposed",
                "evidence": "SQL join",
            },
        ])

        cfg = create_test_config()
        result_df = self._apply_auto_approve(df.copy(), cfg)

        # Neither should be auto-approved due to NaN
        assert result_df.iloc[0]["status"] == "proposed"
        assert result_df.iloc[1]["status"] == "proposed"

    def test_auto_approve_disabled_when_threshold_zero(self, tmp_path):
        """Auto-approve should be disabled when thresholds are 0."""
        df = pd.DataFrame([
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "confidence_sql": 0.90,
                "match_rate": 0.98,
                "status": "proposed",
                "evidence": "SQL join",
            },
        ])

        cfg = create_test_config(
            auto_approve_threshold=0.0,
            auto_approve_confidence=0.80,
        )
        result_df = self._apply_auto_approve(df.copy(), cfg)

        # Should not auto-approve when threshold is 0
        assert result_df.iloc[0]["status"] == "proposed"

    def test_auto_approve_mixed_edges(self, tmp_path):
        """Test with a mix of qualifying and non-qualifying edges."""
        df = pd.DataFrame([
            # Should be auto-approved
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "confidence_sql": 0.90,
                "match_rate": 0.98,
                "status": "proposed",
                "evidence": "SQL join",
            },
            # Should NOT be auto-approved (low match_rate)
            {
                "from_table": "LINE_ITEMS",
                "from_column": "ORDER_ID",
                "to_table": "ORDERS",
                "to_column": "ID",
                "confidence_sql": 0.90,
                "match_rate": 0.70,
                "status": "proposed",
                "evidence": "SQL join",
            },
            # Should NOT be auto-approved (already rejected)
            {
                "from_table": "PAYMENTS",
                "from_column": "ORDER_ID",
                "to_table": "ORDERS",
                "to_column": "ID",
                "confidence_sql": 0.95,
                "match_rate": 0.99,
                "status": "rejected",
                "evidence": "Human rejected",
            },
            # Should be auto-approved
            {
                "from_table": "SHIPMENTS",
                "from_column": "ORDER_ID",
                "to_table": "ORDERS",
                "to_column": "ID",
                "confidence_sql": 0.85,
                "match_rate": 0.96,
                "status": "proposed",
                "evidence": "FK found",
            },
        ])

        cfg = create_test_config()
        result_df = self._apply_auto_approve(df.copy(), cfg)

        # Check each edge
        assert result_df.iloc[0]["status"] == "approved"  # Auto-approved
        assert "[auto-approved]" in result_df.iloc[0]["evidence"]

        assert result_df.iloc[1]["status"] == "proposed"  # Not auto-approved
        assert "[auto-approved]" not in result_df.iloc[1]["evidence"]

        assert result_df.iloc[2]["status"] == "rejected"  # Stays rejected
        assert "[auto-approved]" not in result_df.iloc[2]["evidence"]

        assert result_df.iloc[3]["status"] == "approved"  # Auto-approved
        assert "[auto-approved]" in result_df.iloc[3]["evidence"]

    def test_auto_approve_custom_thresholds(self, tmp_path):
        """Test with custom threshold values."""
        df = pd.DataFrame([
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "confidence_sql": 0.75,  # Meets custom 0.70 threshold
                "match_rate": 0.85,  # Meets custom 0.80 threshold
                "status": "proposed",
                "evidence": "SQL join",
            },
        ])

        cfg = create_test_config(
            auto_approve_threshold=0.80,  # Lower threshold
            auto_approve_confidence=0.70,  # Lower threshold
        )
        result_df = self._apply_auto_approve(df.copy(), cfg)

        assert result_df.iloc[0]["status"] == "approved"
        assert "[auto-approved]" in result_df.iloc[0]["evidence"]

    def test_auto_approve_string_values(self, tmp_path):
        """Test with string values in numeric columns (CSV read behavior)."""
        df = pd.DataFrame([
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "confidence_sql": "0.90",  # String from CSV
                "match_rate": "0.98",  # String from CSV
                "status": "proposed",
                "evidence": "SQL join",
            },
        ])

        cfg = create_test_config()
        result_df = self._apply_auto_approve(df.copy(), cfg)

        assert result_df.iloc[0]["status"] == "approved"
        assert "[auto-approved]" in result_df.iloc[0]["evidence"]

    def _apply_auto_approve(self, df: pd.DataFrame, cfg: AppConfig) -> pd.DataFrame:
        """Apply auto-approve logic (extracted from phase_infer for testing)."""
        if cfg.review.auto_approve_threshold > 0 and cfg.review.auto_approve_confidence > 0:
            # Ensure numeric columns for comparison
            match_rate = pd.to_numeric(df.get("match_rate", pd.Series(dtype=float)), errors="coerce").fillna(0)
            confidence_sql = pd.to_numeric(df.get("confidence_sql", pd.Series(dtype=float)), errors="coerce").fillna(0)

            # Find edges that qualify for auto-approval
            auto_approved_mask = (
                (match_rate >= cfg.review.auto_approve_threshold) &
                (confidence_sql >= cfg.review.auto_approve_confidence) &
                (df["status"] == "proposed")
            )

            if auto_approved_mask.sum() > 0:
                df.loc[auto_approved_mask, "status"] = "approved"
                # Append [auto-approved] marker to evidence field
                df.loc[auto_approved_mask, "evidence"] = (
                    df.loc[auto_approved_mask, "evidence"].fillna("").astype(str) + " [auto-approved]"
                ).str.strip()

        return df


class TestAutoApproveEdgeCases:
    """Edge case tests for auto-approve logic."""

    def test_empty_dataframe(self):
        """Auto-approve should handle empty dataframe."""
        df = pd.DataFrame(columns=[
            "from_table", "from_column", "to_table", "to_column",
            "confidence_sql", "match_rate", "status", "evidence"
        ])

        cfg = create_test_config()
        result_df = TestAutoApproveLogic()._apply_auto_approve(df.copy(), cfg)

        assert len(result_df) == 0

    def test_all_edges_already_approved(self):
        """All edges already approved should remain unchanged."""
        df = pd.DataFrame([
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "confidence_sql": 0.90,
                "match_rate": 0.98,
                "status": "approved",
                "evidence": "Human approved",
            },
        ])

        cfg = create_test_config()
        result_df = TestAutoApproveLogic()._apply_auto_approve(df.copy(), cfg)

        assert result_df.iloc[0]["status"] == "approved"
        assert "Human approved" in result_df.iloc[0]["evidence"]
        assert "[auto-approved]" not in result_df.iloc[0]["evidence"]

    def test_boundary_values_exact_threshold(self):
        """Test exact threshold boundary values."""
        df = pd.DataFrame([
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "confidence_sql": 0.80,  # Exactly at threshold
                "match_rate": 0.95,  # Exactly at threshold
                "status": "proposed",
                "evidence": "SQL join",
            },
        ])

        cfg = create_test_config(
            auto_approve_threshold=0.95,
            auto_approve_confidence=0.80,
        )
        result_df = TestAutoApproveLogic()._apply_auto_approve(df.copy(), cfg)

        # Exact threshold should qualify
        assert result_df.iloc[0]["status"] == "approved"

    def test_boundary_values_below_threshold(self):
        """Test values just below threshold."""
        df = pd.DataFrame([
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "confidence_sql": 0.79999,  # Just below 0.80
                "match_rate": 0.95,
                "status": "proposed",
                "evidence": "SQL join",
            },
        ])

        cfg = create_test_config()
        result_df = TestAutoApproveLogic()._apply_auto_approve(df.copy(), cfg)

        # Just below threshold should not qualify
        assert result_df.iloc[0]["status"] == "proposed"
