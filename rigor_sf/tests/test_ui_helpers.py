"""Tests for pure helper functions in ui/app.py."""

from __future__ import annotations

import pandas as pd

from rigor_sf.ui.app import (
    compute_quality_flag,
    is_auto_approved,
    suggest_classification,
    summarize_classification_progress,
    summarize_relationship_progress,
)


class TestAutoApprovedHelper:
    def test_detects_marker(self):
        assert is_auto_approved("some evidence [auto-approved]") is True

    def test_handles_missing_marker(self):
        assert is_auto_approved("manual") is False


class TestQualityFlagHelper:
    def test_ok_row(self):
        row = pd.Series({"match_rate": 0.98, "pk_unique_rate": 1.0, "fk_null_rate": 0.01})
        assert compute_quality_flag(row) == "ok"

    def test_warning_row(self):
        row = pd.Series({"match_rate": 0.88, "pk_unique_rate": 0.99, "fk_null_rate": 0.05})
        assert compute_quality_flag(row) == "warning"

    def test_critical_row(self):
        row = pd.Series({"match_rate": 0.40, "pk_unique_rate": 1.0, "fk_null_rate": 0.10})
        assert compute_quality_flag(row) == "critical"


class TestClassificationSuggestion:
    def test_bridge_suggestion(self):
        assert suggest_classification(3, 3, 6, 0.50) == "bridge"

    def test_fact_suggestion(self):
        assert suggest_classification(4, 1, 5, 0.20) == "fact"

    def test_dimension_suggestion(self):
        assert suggest_classification(1, 4, 5, 0.20) == "dimension"

    def test_entity_suggestion(self):
        assert suggest_classification(1, 1, 2, 0.50) == "entity"

    def test_empty_suggestion(self):
        assert suggest_classification(0, 0, 0, 0.0) == ""

    def test_fallback_blank_suggestion(self):
        assert suggest_classification(2, 2, 4, 0.2) == ""


class TestProgressSummaries:
    def test_relationship_progress(self):
        df = pd.DataFrame({"status": ["approved", "proposed", "rejected", "approved"]})
        got = summarize_relationship_progress(df)
        assert got["total"] == 4
        assert got["approved"] == 2
        assert got["proposed"] == 1
        assert got["rejected"] == 1

    def test_relationship_progress_empty(self):
        got = summarize_relationship_progress(pd.DataFrame())
        assert got["total"] == 0

    def test_relationship_progress_without_status_column(self):
        got = summarize_relationship_progress(pd.DataFrame({"from_table": ["A", "B"]}))
        assert got["total"] == 2
        assert got["proposed"] == 2

    def test_classification_progress(self):
        df = pd.DataFrame({"current_class": ["fact", "", "dimension", "fact"]})
        got = summarize_classification_progress(df)
        assert got["total"] == 4
        assert got["classified"] == 3
        assert got["unclassified"] == 1
        assert got["class_mix"] == {"fact": 2, "dimension": 1}

    def test_classification_progress_empty(self):
        got = summarize_classification_progress(pd.DataFrame())
        assert got["total"] == 0
