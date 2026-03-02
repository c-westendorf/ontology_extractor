"""
rigor/run_loader.py
-------------------
Phase A (offline mode) — Run Results Loader.

Reads a run folder produced by query_gen.py after the analyst has executed
the queries in Snowflake and dropped the result CSVs into results/.

Responsibilities:
  1. Parse results/profiling_edges.csv → EdgeProfile objects
  2. Parse results/column_profiles.csv → ColumnProfile objects
  3. Parse results/value_overlap.csv  → DirectionHint objects (optional)
  4. Merge stats back into inferred_relationships.csv rows
  5. Apply direction corrections from value_overlap where applicable
  6. Write the merged inferred_relationships.csv to runs/<run_id>/artifacts/
  7. Update run_meta.json with downstream_run reference when pipeline runs

Consumed by pipeline.py in --source offline or hybrid mode.
"""

from __future__ import annotations

import json
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


# ── Result dataclasses ────────────────────────────────────────────────────────

@dataclass
class EdgeProfile:
    """Profiling stats for one candidate FK edge, loaded from profiling_edges.csv."""
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    sample_rows: int
    fk_nonnull: int
    match_count: int
    match_rate: float           # fraction of non-null FKs that match a PK
    pk_distinct: int
    pk_total: int
    pk_unique_rate: float       # how unique is the referred column (1.0 = true PK)
    fk_null_rate: float         # fraction of the FK column that is null
    confidence_sql: float
    frequency: int
    evidence: str
    profiled: bool = True       # False when row comes from sql-ingest only (no Snowflake)


@dataclass
class ColumnProfile:
    """Per-column quality stats loaded from column_profiles.csv."""
    table_name: str
    column_name: str
    total_rows: int
    non_null_count: int
    null_rate: float
    distinct_count: int
    cardinality_ratio: float
    min_val: Optional[str]
    max_val: Optional[str]
    inferred_type: Optional[str]


@dataclass
class DirectionHint:
    """FK direction suggestion from value_overlap.csv."""
    table_a: str
    col_a: str
    table_b: str
    col_b: str
    a_coverage: float
    b_coverage: float
    direction_suggestion: str   # raw text from SQL CASE statement


# ── Public API ────────────────────────────────────────────────────────────────

class RunLoader:
    """
    Loads a query_gen run folder and merges profiling results into the
    inferred relationships CSV.

    Usage
    -----
        loader = RunLoader("runs/2026-02-27_001_initial")
        rel_df  = loader.merge_relationships(raw_edges)   # raw JoinEdge list from sql_ingest
        col_profiles = loader.column_profiles             # dict keyed by (table, col)
    """

    def __init__(self, run_dir: str):
        self.run_dir = Path(run_dir)
        if not self.run_dir.exists():
            raise FileNotFoundError(f"Run directory not found: {run_dir}")

        self._meta: Optional[dict] = None
        self._edge_profiles: Optional[Dict[Tuple[str,str,str,str], EdgeProfile]] = None
        self._column_profiles: Optional[Dict[Tuple[str,str], ColumnProfile]] = None
        self._direction_hints: Optional[Dict[Tuple[str,str,str,str], DirectionHint]] = None

    # ── Lazy properties ───────────────────────────────────────────────────────

    @property
    def meta(self) -> dict:
        if self._meta is None:
            meta_path = self.run_dir / "run_meta.json"
            if not meta_path.exists():
                raise FileNotFoundError(f"run_meta.json not found in {self.run_dir}")
            self._meta = json.loads(meta_path.read_text(encoding="utf-8"))
        return self._meta

    @property
    def edge_profiles(self) -> Dict[Tuple[str,str,str,str], EdgeProfile]:
        if self._edge_profiles is None:
            self._edge_profiles = self._load_edge_profiles()
        return self._edge_profiles

    @property
    def column_profiles(self) -> Dict[Tuple[str,str], ColumnProfile]:
        if self._column_profiles is None:
            self._column_profiles = self._load_column_profiles()
        return self._column_profiles

    @property
    def direction_hints(self) -> Dict[Tuple[str,str,str,str], DirectionHint]:
        if self._direction_hints is None:
            self._direction_hints = self._load_direction_hints()
        return self._direction_hints

    # ── Core merge logic ──────────────────────────────────────────────────────

    def merge_relationships(
        self,
        raw_edges_df: pd.DataFrame,
        overrides_approved: Optional[List[dict]] = None,
        overrides_rejected: Optional[List[dict]] = None,
    ) -> pd.DataFrame:
        """
        Merge profiling stats into the raw inferred-relationships DataFrame.

        Parameters
        ----------
        raw_edges_df        : DataFrame produced by relationships.write_inferred_relationships_csv
                              (columns: from_table, from_column, to_table, to_column,
                               confidence_sql, evidence, status, match_rate, pk_unique_rate,
                               fk_null_rate)
        overrides_approved  : list of {"from": {table, columns}, "to": {table, columns}} dicts
        overrides_rejected  : same format

        Returns
        -------
        DataFrame with profiling stats merged in + frequency + direction corrections applied.
        Writes merged CSV to runs/<run_id>/artifacts/inferred_relationships.csv.
        """
        df = raw_edges_df.copy()

        # Ensure v1 columns exist
        for col in ["frequency", "from_columns", "to_columns", "data_quality_flag"]:
            if col not in df.columns:
                df[col] = "" if col in ("from_columns", "to_columns", "data_quality_flag") else 0

        # Merge profiling stats row-by-row
        def _apply_profile(row):
            key = (
                str(row["from_table"]).upper(),
                str(row["from_column"]).upper(),
                str(row["to_table"]).upper(),
                str(row["to_column"]).upper(),
            )
            prof = self.edge_profiles.get(key)
            if prof is None:
                # Try reverse direction (in case SQL ingest flipped it)
                rev_key = (key[2], key[3], key[0], key[1])
                prof = self.edge_profiles.get(rev_key)

            if prof is not None:
                row["match_rate"]      = prof.match_rate
                row["pk_unique_rate"]  = prof.pk_unique_rate
                row["fk_null_rate"]    = prof.fk_null_rate
                row["frequency"]       = prof.frequency
                row["confidence_sql"]  = prof.confidence_sql
            else:
                # No profiling result — mark as unverified
                row["data_quality_flag"] = "not_profiled"

            return row

        df = df.apply(_apply_profile, axis=1)

        # Apply direction corrections from value_overlap
        df = self._apply_direction_corrections(df)

        # Apply overrides status
        if overrides_approved:
            df = self._apply_status(df, overrides_approved, "approved")
        if overrides_rejected:
            df = self._apply_status(df, overrides_rejected, "rejected")

        # Reorder columns to v1 contract
        ordered = [
            "from_table", "from_columns", "from_column",
            "to_table", "to_columns", "to_column",
            "confidence_sql", "frequency",
            "match_rate", "pk_unique_rate", "fk_null_rate",
            "status", "evidence", "data_quality_flag",
        ]
        for c in ordered:
            if c not in df.columns:
                df[c] = ""
        df = df[ordered]

        # Write artifact
        artifact_path = self.run_dir / "artifacts" / "inferred_relationships.csv"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(str(artifact_path), index=False)
        print(f"[run_loader] Merged relationships written → {artifact_path}")

        # Tag downstream_run in meta
        self._stamp_downstream_run()

        return df

    # ── Private loaders ───────────────────────────────────────────────────────

    def _load_edge_profiles(self) -> Dict[Tuple[str,str,str,str], EdgeProfile]:
        csv_path = self.run_dir / "results" / "profiling_edges.csv"
        if not csv_path.exists():
            warnings.warn(
                f"[run_loader] profiling_edges.csv not found in {self.run_dir}/results/. "
                "Run queries/01_profiling_edges.sql in Snowflake and export the result here. "
                "Proceeding without profiling stats — match_rate, pk_unique_rate, fk_null_rate "
                "will be empty. Trust gate will fall back to sql_confidence only.",
                stacklevel=3,
            )
            return {}

        df = pd.read_csv(str(csv_path))
        df.columns = [c.lower() for c in df.columns]

        profiles: Dict[Tuple[str,str,str,str], EdgeProfile] = {}
        for _, row in df.iterrows():
            key = (
                str(row.get("from_table", "")).upper(),
                str(row.get("from_column", "")).upper(),
                str(row.get("to_table", "")).upper(),
                str(row.get("to_column", "")).upper(),
            )
            profiles[key] = EdgeProfile(
                from_table       = str(row.get("from_table", "")),
                from_column      = str(row.get("from_column", "")),
                to_table         = str(row.get("to_table", "")),
                to_column        = str(row.get("to_column", "")),
                sample_rows      = int(row.get("sample_rows", 0) or 0),
                fk_nonnull       = int(row.get("fk_nonnull", 0) or 0),
                match_count      = int(row.get("match_count", 0) or 0),
                match_rate       = float(row.get("match_rate", 0.0) or 0.0),
                pk_distinct      = int(row.get("pk_distinct", 0) or 0),
                pk_total         = int(row.get("pk_total", 0) or 0),
                pk_unique_rate   = float(row.get("pk_unique_rate", 0.0) or 0.0),
                fk_null_rate     = float(row.get("fk_null_rate", 0.0) or 0.0),
                confidence_sql   = float(row.get("confidence_sql", 0.0) or 0.0),
                frequency        = int(row.get("frequency", 1) or 1),
                evidence         = str(row.get("evidence", "")),
                profiled         = True,
            )
        print(f"[run_loader] Loaded {len(profiles)} edge profiles from profiling_edges.csv")
        return profiles

    def _load_column_profiles(self) -> Dict[Tuple[str,str], ColumnProfile]:
        csv_path = self.run_dir / "results" / "column_profiles.csv"
        if not csv_path.exists():
            warnings.warn(
                f"[run_loader] column_profiles.csv not found in {self.run_dir}/results/. "
                "Column quality signals will be unavailable.",
                stacklevel=3,
            )
            return {}

        df = pd.read_csv(str(csv_path))
        df.columns = [c.lower() for c in df.columns]

        profiles: Dict[Tuple[str,str], ColumnProfile] = {}
        for _, row in df.iterrows():
            key = (
                str(row.get("table_name", "")).upper(),
                str(row.get("column_name", "")).upper(),
            )
            profiles[key] = ColumnProfile(
                table_name       = str(row.get("table_name", "")),
                column_name      = str(row.get("column_name", "")),
                total_rows       = int(row.get("total_rows", 0) or 0),
                non_null_count   = int(row.get("non_null_count", 0) or 0),
                null_rate        = float(row.get("null_rate", 0.0) or 0.0),
                distinct_count   = int(row.get("distinct_count", 0) or 0),
                cardinality_ratio= float(row.get("cardinality_ratio", 0.0) or 0.0),
                min_val          = str(row.get("min_val", "")) or None,
                max_val          = str(row.get("max_val", "")) or None,
                inferred_type    = str(row.get("inferred_type", "")) or None,
            )
        print(f"[run_loader] Loaded {len(profiles)} column profiles from column_profiles.csv")
        return profiles

    def _load_direction_hints(self) -> Dict[Tuple[str,str,str,str], DirectionHint]:
        csv_path = self.run_dir / "results" / "value_overlap.csv"
        if not csv_path.exists():
            return {}

        df = pd.read_csv(str(csv_path))
        df.columns = [c.lower() for c in df.columns]

        hints: Dict[Tuple[str,str,str,str], DirectionHint] = {}
        for _, row in df.iterrows():
            ta = str(row.get("table_a", "")).upper()
            ca = str(row.get("col_a", "")).upper()
            tb = str(row.get("table_b", "")).upper()
            cb = str(row.get("col_b", "")).upper()
            key  = (ta, ca, tb, cb)
            rkey = (tb, cb, ta, ca)
            hint = DirectionHint(
                table_a              = ta,
                col_a                = ca,
                table_b              = tb,
                col_b                = cb,
                a_coverage           = float(row.get("a_coverage", 0.0) or 0.0),
                b_coverage           = float(row.get("b_coverage", 0.0) or 0.0),
                direction_suggestion = str(row.get("direction_suggestion", "")),
            )
            hints[key]  = hint
            hints[rkey] = hint
        print(f"[run_loader] Loaded {len(hints)//2} direction hints from value_overlap.csv")
        return hints

    # ── Direction correction ──────────────────────────────────────────────────

    def _apply_direction_corrections(self, df: pd.DataFrame) -> pd.DataFrame:
        """Flip from/to for rows where value_overlap says direction is reversed."""
        if not self.direction_hints:
            return df

        def _fix_row(row):
            key = (
                str(row["from_table"]).upper(),
                str(row["from_column"]).upper(),
                str(row["to_table"]).upper(),
                str(row["to_column"]).upper(),
            )
            hint = self.direction_hints.get(key)
            if hint is None:
                return row

            suggestion = hint.direction_suggestion.lower()
            # If suggestion says the current to_table is actually the CHILD, flip
            if f"{str(row['to_table']).lower()} is likely the parent" in suggestion:
                return row  # already correct direction

            if f"{str(row['from_table']).lower()} is likely the parent" in suggestion:
                # Flip — from and to are reversed
                row["from_table"], row["to_table"] = row["to_table"], row["from_table"]
                row["from_column"], row["to_column"] = row["to_column"], row["from_column"]
                row["data_quality_flag"] = (str(row.get("data_quality_flag","")) + ";direction_corrected").strip(";")

            return row

        return df.apply(_fix_row, axis=1)

    # ── Status application ────────────────────────────────────────────────────

    @staticmethod
    def _apply_status(
        df: pd.DataFrame,
        override_list: List[dict],
        status: str,
    ) -> pd.DataFrame:
        """Set status column for rows matching override entries."""
        for entry in override_list:
            f = entry.get("from", {})
            t = entry.get("to", {})
            f_table   = str(f.get("table", "")).upper()
            f_columns = [c.upper() for c in f.get("columns", [])]
            t_table   = str(t.get("table", "")).upper()
            t_columns = [c.upper() for c in t.get("columns", [])]

            mask = (
                (df["from_table"].str.upper() == f_table) &
                (df["to_table"].str.upper() == t_table)
            )
            if f_columns:
                mask &= df["from_column"].str.upper().isin(f_columns)
            if t_columns:
                mask &= df["to_column"].str.upper().isin(t_columns)

            df.loc[mask, "status"] = status

        return df

    # ── Meta stamping ─────────────────────────────────────────────────────────

    def _stamp_downstream_run(self) -> None:
        """Record the pipeline execution timestamp in run_meta.json."""
        meta_path = self.run_dir / "run_meta.json"
        meta = self.meta.copy()
        meta["downstream_run"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    # ── Diagnostic helpers ────────────────────────────────────────────────────

    def summary(self) -> str:
        """Return a human-readable summary of what was loaded."""
        lines = [
            f"Run: {self.run_dir.name}",
            f"  Edge profiles loaded : {len(self.edge_profiles)}",
            f"  Column profiles      : {len(self.column_profiles)}",
            f"  Direction hints      : {len(self.direction_hints)//2 if self.direction_hints else 0}",
        ]
        unverified = sum(1 for k, v in self.edge_profiles.items() if not v.profiled)
        if unverified:
            lines.append(f"  WARNING: {unverified} edges have no profiling data")
        return "\n".join(lines)

    def data_quality_report(self) -> dict:
        """
        Produce a data_quality_report.json-compatible dict.
        Flags: high null rates, low cardinality on suspected PK cols,
        edges with no profiling result.
        """
        issues: list[dict] = []

        # Columns with null_rate > 0.20 on a join column
        for (table, col), cp in self.column_profiles.items():
            if cp.null_rate > 0.20:
                issues.append({
                    "type": "high_null_rate",
                    "table": table,
                    "column": col,
                    "null_rate": cp.null_rate,
                    "severity": "warning" if cp.null_rate < 0.50 else "error",
                })

        # Edges where match_rate < 0.50 (very poor referential integrity)
        for key, ep in self.edge_profiles.items():
            if ep.match_rate < 0.50:
                issues.append({
                    "type": "low_match_rate",
                    "from_table": ep.from_table,
                    "from_column": ep.from_column,
                    "to_table": ep.to_table,
                    "to_column": ep.to_column,
                    "match_rate": ep.match_rate,
                    "severity": "warning",
                })

        # Not-profiled edges
        not_found = [
            ep for ep in self.edge_profiles.values() if not ep.profiled
        ]
        for ep in not_found:
            issues.append({
                "type": "not_profiled",
                "from_table": ep.from_table,
                "from_column": ep.from_column,
                "to_table": ep.to_table,
                "to_column": ep.to_column,
                "severity": "info",
            })

        return {
            "run_id": self.run_dir.name,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "issues": issues,
            "total_issues": len(issues),
            "warnings": sum(1 for i in issues if i["severity"] == "warning"),
            "errors":   sum(1 for i in issues if i["severity"] == "error"),
        }
