"""Tests for profiling.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from rigor_sf.profiling import profile_edge


def _mock_conn_rows(sample_rows, fk_nonnull, match_count, pk_total, pk_distinct):
    conn = MagicMock()
    conn.execute.side_effect = [
        MagicMock(mappings=lambda: MagicMock(one=lambda: {"sample_rows": sample_rows, "fk_nonnull": fk_nonnull})),
        MagicMock(mappings=lambda: MagicMock(one=lambda: {"match_count": match_count})),
        MagicMock(mappings=lambda: MagicMock(one=lambda: {"pk_total": pk_total, "pk_distinct": pk_distinct})),
    ]
    return conn


@patch("rigor_sf.profiling.create_engine")
def test_profile_edge_computes_rates(mock_create_engine):
    conn = _mock_conn_rows(100, 80, 60, 50, 50)
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    mock_create_engine.return_value = engine

    prof = profile_edge("sqlite://", "A", "a_id", "B", "id", sample_limit=1000)

    assert prof.sample_rows == 100
    assert prof.fk_nonnull == 80
    assert prof.match_count == 60
    assert prof.match_rate == 0.75
    assert prof.pk_total == 50
    assert prof.pk_distinct == 50
    assert prof.pk_unique_rate == 1.0
    assert prof.fk_null_rate == pytest.approx(0.2)


@patch("rigor_sf.profiling.create_engine")
def test_profile_edge_handles_zero_denominators(mock_create_engine):
    conn = _mock_conn_rows(0, 0, 0, 0, 0)
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    mock_create_engine.return_value = engine

    prof = profile_edge("sqlite://", "A", "a_id", "B", "id")

    assert prof.match_rate == 0.0
    assert prof.pk_unique_rate == 0.0
    assert prof.fk_null_rate == 1.0
