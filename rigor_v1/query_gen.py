"""
rigor/query_gen.py
------------------
Phase 0 — Query Generation.

Reads SQL worksheets (or a pre-built edge list), produces a timestamped run
folder containing:

    runs/<run_id>/
        run_meta.json          — run identity + downstream link
        README.md              — human instructions for the analyst
        queries/
            01_profiling_edges.sql   — match_rate / pk_unique_rate / fk_null_rate
            02_column_profiles.sql   — null rate, cardinality, min/max per column
            03_value_overlap.sql     — bidirectional overlap for ambiguous edges
        results/               — analyst drops CSV exports here (empty at gen time)
        artifacts/             — pipeline writes inferred_relationships.csv here

The analyst runs the queries in Snowflake (worksheet or Cursor extension),
exports each result as CSV to results/, then runs Phase A against this run dir.
"""

from __future__ import annotations

import hashlib
import json
import os
import textwrap
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from .sql_ingest import JoinEdge, ingest_sql_dir, edges_to_inferred_fks


# ── Internal edge representation used during SQL generation ──────────────────

@dataclass
class _EdgeSpec:
    """Normalised, directed candidate edge ready for SQL generation."""
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    confidence_sql: float
    frequency: int          # how many SQL files referenced this edge
    evidence: str
    ambiguous_direction: bool = False


# ── Public API ────────────────────────────────────────────────────────────────

def generate_run(
    sql_dir: str,
    runs_dir: str = "runs",
    run_label: Optional[str] = None,
    sample_limit: int = 200_000,
) -> str:
    """
    Main entry point.  Ingest sql_dir, build the run folder, return its path.

    Parameters
    ----------
    sql_dir      : directory containing .sql worksheet files
    runs_dir     : parent directory for all run folders (default: "runs")
    run_label    : optional short label appended to the run_id (e.g. "initial")
    sample_limit : LIMIT N used in every profiling query

    Returns
    -------
    str  — absolute path to the generated run folder
    """
    sql_path = Path(sql_dir)
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL worksheet directory not found: {sql_dir}")

    # Collect worksheets and infer edges
    worksheet_files = sorted(sql_path.rglob("*.sql"))
    if not worksheet_files:
        raise ValueError(f"No .sql files found in {sql_dir}")

    raw_edges = ingest_sql_dir(sql_dir)
    directed = _build_directed_edges(raw_edges)

    # Stable hash of all worksheet content (for cache/diff detection)
    worksheets_hash = _hash_files(worksheet_files)

    # Build run_id  →  2026-02-27_001_initial  (counter pads to 3 digits)
    run_id = _make_run_id(runs_dir, run_label)
    run_dir = Path(runs_dir) / run_id
    (run_dir / "queries").mkdir(parents=True, exist_ok=True)
    (run_dir / "results").mkdir(parents=True, exist_ok=True)
    (run_dir / "artifacts").mkdir(parents=True, exist_ok=True)

    # Write SQL files
    _write_profiling_edges_sql(directed, run_dir, sample_limit)
    _write_column_profiles_sql(directed, run_dir, sample_limit)
    _write_value_overlap_sql(directed, run_dir, sample_limit)

    # Write run_meta.json
    meta = _build_run_meta(
        run_id=run_id,
        worksheet_files=worksheet_files,
        directed=directed,
        worksheets_hash=worksheets_hash,
        sample_limit=sample_limit,
    )
    (run_dir / "run_meta.json").write_text(
        json.dumps(meta, indent=2), encoding="utf-8"
    )

    # Write human-readable README
    (run_dir / "README.md").write_text(
        _build_readme(run_id, worksheet_files, directed, run_dir),
        encoding="utf-8",
    )

    print(f"[query_gen] Run folder created: {run_dir.resolve()}")
    print(f"[query_gen] {len(directed)} directed edges → 3 query files generated")
    print(f"[query_gen] Next: run queries in Snowflake, export CSVs to {run_dir}/results/")

    return str(run_dir.resolve())


# ── Edge normalisation ────────────────────────────────────────────────────────

def _build_directed_edges(raw_edges: List[JoinEdge]) -> List[_EdgeSpec]:
    """
    Convert raw undirected JoinEdges to directed _EdgeSpecs with frequency counts.
    Edges appearing in multiple files accumulate frequency and a confidence boost.
    Edges where direction cannot be resolved are flagged ambiguous_direction=True.
    """
    # Group by canonical (unordered) pair to accumulate frequency
    from collections import defaultdict
    groups: dict[tuple, list[JoinEdge]] = defaultdict(list)
    for e in raw_edges:
        key = tuple(sorted([
            (e.left_table, e.left_column),
            (e.right_table, e.right_column),
        ]))
        groups[key].append(e)

    specs: list[_EdgeSpec] = []
    seen: set[tuple] = set()

    for group in groups.values():
        # Representative edge (highest confidence in group)
        rep = max(group, key=lambda e: e.confidence)
        freq = len(group)

        # Frequency confidence boost
        freq_boost = 0.0
        if freq >= 10:
            freq_boost = 0.10
        elif freq >= 5:
            freq_boost = 0.05
        conf = min(rep.confidence + freq_boost, 0.95)

        # Determine direction
        lc, rc = rep.left_column, rep.right_column
        l_is_id = lc == "ID"
        r_is_id = rc == "ID"
        l_fkish = lc.endswith("_ID") and not l_is_id
        r_fkish = rc.endswith("_ID") and not r_is_id

        ambiguous = False
        if l_fkish and r_is_id:
            from_t, from_c, to_t, to_c = rep.left_table, lc, rep.right_table, rc
        elif r_fkish and l_is_id:
            from_t, from_c, to_t, to_c = rep.right_table, rc, rep.left_table, lc
        elif l_is_id and not r_is_id:
            from_t, from_c, to_t, to_c = rep.right_table, rc, rep.left_table, lc
        elif r_is_id and not l_is_id:
            from_t, from_c, to_t, to_c = rep.left_table, lc, rep.right_table, rc
        else:
            # Truly ambiguous — keep left→right, flag it
            from_t, from_c, to_t, to_c = rep.left_table, lc, rep.right_table, rc
            ambiguous = True

        dedup_key = (from_t, from_c, to_t, to_c)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        # Consolidate evidence snippets (unique file paths only)
        file_paths = sorted({e.evidence.split(" | ")[0] for e in group})
        evidence_str = "; ".join(file_paths)

        specs.append(_EdgeSpec(
            from_table=from_t,
            from_column=from_c,
            to_table=to_t,
            to_column=to_c,
            confidence_sql=round(conf, 4),
            frequency=freq,
            evidence=evidence_str,
            ambiguous_direction=ambiguous,
        ))

    return sorted(specs, key=lambda e: (-e.frequency, -e.confidence_sql, e.from_table))


# ── SQL file writers ──────────────────────────────────────────────────────────

def _write_profiling_edges_sql(
    edges: List[_EdgeSpec],
    run_dir: Path,
    sample_limit: int,
) -> None:
    """01_profiling_edges.sql — one UNION ALL block per edge."""
    run_id = run_dir.name
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    header = textwrap.dedent(f"""\
        -- =============================================================
        -- RIGOR-SF | Run: {run_id}
        -- File:    01_profiling_edges.sql
        -- Purpose: Profile {len(edges)} candidate join edges
        --          (match_rate, pk_unique_rate, fk_null_rate)
        -- Generated: {now}
        --
        -- INSTRUCTIONS:
        --   1. Open this file in your Snowflake worksheet or
        --      Cursor Snowflake extension.
        --   2. Run the ENTIRE script (Cmd/Ctrl+A, then Run).
        --   3. Export the result grid as CSV.
        --   4. Save to: {run_dir}/results/profiling_edges.csv
        --
        -- NOTE: Each edge is one SELECT block joined with UNION ALL.
        --       The final result has one row per edge.
        -- =============================================================
    """)

    blocks: list[str] = []
    for i, e in enumerate(edges):
        n = i + 1
        direction_note = " [AMBIGUOUS DIRECTION — see 03_value_overlap.sql]" if e.ambiguous_direction else ""
        block = textwrap.dedent(f"""\
            -- Edge {n}/{len(edges)}: {e.from_table}.{e.from_column} -> {e.to_table}.{e.to_column}{direction_note}
            -- Evidence: {e.evidence}
            -- SQL confidence: {e.confidence_sql}  |  Frequency: {e.frequency} file(s)
            SELECT
                '{e.from_table}'    AS from_table,
                '{e.from_column}'   AS from_column,
                '{e.to_table}'      AS to_table,
                '{e.to_column}'     AS to_column,
                fk_stats.sample_rows,
                fk_stats.fk_nonnull,
                overlap.match_count,
                IFF(fk_stats.fk_nonnull > 0,
                    overlap.match_count / fk_stats.fk_nonnull, 0)          AS match_rate,
                pk_stats.pk_distinct,
                pk_stats.pk_total,
                IFF(pk_stats.pk_total > 0,
                    pk_stats.pk_distinct / pk_stats.pk_total, 0)           AS pk_unique_rate,
                IFF(fk_stats.sample_rows > 0,
                    1 - (fk_stats.fk_nonnull / fk_stats.sample_rows), 1)  AS fk_null_rate,
                {e.confidence_sql}                                          AS confidence_sql,
                {e.frequency}                                               AS frequency,
                '{e.evidence.replace("'", "''")}'                           AS evidence
            FROM (
                SELECT
                    COUNT(*)                        AS sample_rows,
                    COUNT({e.from_column})          AS fk_nonnull
                FROM {e.from_table}
                LIMIT {sample_limit}
            ) AS fk_stats,
            (
                SELECT COUNT(*) AS match_count
                FROM (
                    SELECT {e.from_column} AS fk_val
                    FROM {e.from_table}
                    WHERE {e.from_column} IS NOT NULL
                    LIMIT {sample_limit}
                ) AS fk_sample
                INNER JOIN (
                    SELECT DISTINCT {e.to_column} AS pk_val
                    FROM {e.to_table}
                ) AS pk_set ON fk_sample.fk_val = pk_set.pk_val
            ) AS overlap,
            (
                SELECT
                    COUNT(*)                        AS pk_total,
                    COUNT(DISTINCT {e.to_column})   AS pk_distinct
                FROM {e.to_table}
            ) AS pk_stats""")
        blocks.append(block)

    sql = header + "\nUNION ALL\n\n".join(blocks) + "\n\nORDER BY match_rate DESC;\n"
    (run_dir / "queries" / "01_profiling_edges.sql").write_text(sql, encoding="utf-8")


def _write_column_profiles_sql(
    edges: List[_EdgeSpec],
    run_dir: Path,
    sample_limit: int,
) -> None:
    """02_column_profiles.sql — null rate, cardinality, min/max per join column."""
    run_id = run_dir.name
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Collect unique (table, column) pairs referenced in edges
    col_pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for e in edges:
        for t, c in [(e.from_table, e.from_column), (e.to_table, e.to_column)]:
            if (t, c) not in seen:
                seen.add((t, c))
                col_pairs.append((t, c))
    col_pairs.sort()

    header = textwrap.dedent(f"""\
        -- =============================================================
        -- RIGOR-SF | Run: {run_id}
        -- File:    02_column_profiles.sql
        -- Purpose: Column-level quality profiles for {len(col_pairs)} join columns
        --          (null rate, cardinality, min/max, inferred type)
        -- Generated: {now}
        --
        -- INSTRUCTIONS:
        --   1. Run the ENTIRE script in Snowflake.
        --   2. Export result as CSV.
        --   3. Save to: {run_dir}/results/column_profiles.csv
        -- =============================================================

    """)

    blocks: list[str] = []
    for i, (table, col) in enumerate(col_pairs):
        block = textwrap.dedent(f"""\
            -- Column {i+1}/{len(col_pairs)}: {table}.{col}
            SELECT
                '{table}'                                       AS table_name,
                '{col}'                                         AS column_name,
                COUNT(*)                                        AS total_rows,
                COUNT({col})                                    AS non_null_count,
                IFF(COUNT(*) > 0,
                    1 - COUNT({col}) / COUNT(*), 1)             AS null_rate,
                COUNT(DISTINCT {col})                           AS distinct_count,
                IFF(COUNT({col}) > 0,
                    COUNT(DISTINCT {col}) / COUNT({col}), 0)    AS cardinality_ratio,
                MIN({col})::VARCHAR                             AS min_val,
                MAX({col})::VARCHAR                             AS max_val,
                TYPEOF(MIN({col}))                              AS inferred_type
            FROM {table}""")
        blocks.append(block)

    sql = header + "\nUNION ALL\n\n".join(blocks) + "\n\nORDER BY table_name, column_name;\n"
    (run_dir / "queries" / "02_column_profiles.sql").write_text(sql, encoding="utf-8")


def _write_value_overlap_sql(
    edges: List[_EdgeSpec],
    run_dir: Path,
    sample_limit: int,
) -> None:
    """03_value_overlap.sql — bidirectional overlap for ambiguous-direction edges."""
    ambiguous = [e for e in edges if e.ambiguous_direction]
    run_id = run_dir.name
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    header = textwrap.dedent(f"""\
        -- =============================================================
        -- RIGOR-SF | Run: {run_id}
        -- File:    03_value_overlap.sql
        -- Purpose: Bidirectional value overlap for {len(ambiguous)} ambiguous edge(s)
        --          to determine correct FK direction.
        -- Generated: {now}
        --
        -- OPTIONAL — only needed if you want to resolve ambiguous edges.
        -- Interpretation:
        --   If table_b has fewer distinct values and b_coverage is near 1.0,
        --   table_b is likely the parent (referred) table.
        --
        -- INSTRUCTIONS:
        --   1. Run in Snowflake.
        --   2. Export as CSV → save to: {run_dir}/results/value_overlap.csv
        -- =============================================================

    """)

    if not ambiguous:
        sql = header + "-- No ambiguous edges detected. No queries to run.\nSELECT 'no_ambiguous_edges' AS status;\n"
        (run_dir / "queries" / "03_value_overlap.sql").write_text(sql, encoding="utf-8")
        return

    blocks: list[str] = []
    for i, e in enumerate(ambiguous):
        block = textwrap.dedent(f"""\
            -- Ambiguous edge {i+1}/{len(ambiguous)}: {e.from_table}.{e.from_column} <-> {e.to_table}.{e.to_column}
            -- Evidence: {e.evidence}
            SELECT
                '{e.from_table}'    AS table_a,
                '{e.from_column}'   AS col_a,
                '{e.to_table}'      AS table_b,
                '{e.to_column}'     AS col_b,
                a_counts.a_distinct,
                b_counts.b_distinct,
                a_in_b.a_in_b_count,
                b_in_a.b_in_a_count,
                IFF(a_counts.a_distinct > 0,
                    a_in_b.a_in_b_count / a_counts.a_distinct, 0)  AS a_coverage,
                IFF(b_counts.b_distinct > 0,
                    b_in_a.b_in_a_count / b_counts.b_distinct, 0)  AS b_coverage,
                CASE
                    WHEN b_counts.b_distinct < a_counts.a_distinct
                     AND IFF(b_counts.b_distinct > 0,
                             b_in_a.b_in_a_count / b_counts.b_distinct, 0) > 0.9
                    THEN '{e.to_table} is likely the PARENT (referred) table'
                    WHEN a_counts.a_distinct < b_counts.b_distinct
                     AND IFF(a_counts.a_distinct > 0,
                             a_in_b.a_in_b_count / a_counts.a_distinct, 0) > 0.9
                    THEN '{e.from_table} is likely the PARENT (referred) table'
                    ELSE 'Direction unclear — review manually'
                END AS direction_suggestion
            FROM
                (SELECT COUNT(DISTINCT {e.from_column}) AS a_distinct FROM {e.from_table}) a_counts,
                (SELECT COUNT(DISTINCT {e.to_column})   AS b_distinct FROM {e.to_table})   b_counts,
                (SELECT COUNT(DISTINCT a.{e.from_column}) AS a_in_b_count
                 FROM {e.from_table} a
                 WHERE EXISTS (
                     SELECT 1 FROM {e.to_table} b
                     WHERE b.{e.to_column} = a.{e.from_column}
                 )) a_in_b,
                (SELECT COUNT(DISTINCT b.{e.to_column}) AS b_in_a_count
                 FROM {e.to_table} b
                 WHERE EXISTS (
                     SELECT 1 FROM {e.from_table} a
                     WHERE a.{e.from_column} = b.{e.to_column}
                 )) b_in_a""")
        blocks.append(block)

    sql = header + "\nUNION ALL\n\n".join(blocks) + ";\n"
    (run_dir / "queries" / "03_value_overlap.sql").write_text(sql, encoding="utf-8")


# ── run_meta.json builder ─────────────────────────────────────────────────────

def _build_run_meta(
    run_id: str,
    worksheet_files: list[Path],
    directed: list[_EdgeSpec],
    worksheets_hash: str,
    sample_limit: int,
) -> dict:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ambiguous_count = sum(1 for e in directed if e.ambiguous_direction)

    return {
        "run_id": run_id,
        "generated_at": now,
        "generated_by": "rigor-sf v1.0 / query_gen.py",
        "sql_worksheets_ingested": [str(f) for f in worksheet_files],
        "worksheets_hash": worksheets_hash,
        "candidate_edges_found": len(directed),
        "ambiguous_direction_edges": ambiguous_count,
        "sample_limit": sample_limit,
        "queries_generated": {
            "01_profiling_edges.sql": {
                "purpose": f"Match rate and uniqueness profiling for {len(directed)} candidate join edges",
                "estimated_rows_scanned": f"~{len(directed) * sample_limit // 1000}k (up to {sample_limit:,} rows per edge)",
                "instructions": f"Run in Snowflake. Export result as CSV to results/profiling_edges.csv",
                "required": True,
            },
            "02_column_profiles.sql": {
                "purpose": f"Null rates, cardinality, and value distributions for join columns",
                "instructions": f"Run in Snowflake. Export result as CSV to results/column_profiles.csv",
                "required": True,
            },
            "03_value_overlap.sql": {
                "purpose": f"Bidirectional overlap for {ambiguous_count} ambiguous-direction edge(s)",
                "instructions": f"Optional. Run and export to results/value_overlap.csv",
                "required": ambiguous_count > 0,
            },
        },
        "downstream_run": None,
        "notes": "",
    }


# ── README builder ────────────────────────────────────────────────────────────

def _build_readme(
    run_id: str,
    worksheet_files: list[Path],
    directed: list[_EdgeSpec],
    run_dir: Path,
) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    ambiguous = [e for e in directed if e.ambiguous_direction]

    # Top edges by frequency
    top_edges = sorted(directed, key=lambda e: (-e.frequency, -e.confidence_sql))[:5]
    top_rows = "\n".join(
        f"- `{e.from_table}.{e.from_column}` → `{e.to_table}.{e.to_column}` "
        f"(frequency: {e.frequency}, confidence: {e.confidence_sql})"
        for e in top_edges
    )

    # Worksheet summary
    ws_rows = "\n".join(f"| {f.name} | see query files |" for f in worksheet_files)

    return textwrap.dedent(f"""\
        # RIGOR-SF Query Package
        ## Run: `{run_id}`

        Generated: {now}
        Source: {len(worksheet_files)} SQL worksheet(s) → {len(directed)} candidate join edges

        ---

        ## What to do

        ### Step 1 — Run edge profiling (required)

        Open `queries/01_profiling_edges.sql` in your Snowflake worksheet
        or the Cursor Snowflake extension. Run the entire script.

        Export the result grid as CSV → save to:
        ```
        {run_dir}/results/profiling_edges.csv
        ```

        ### Step 2 — Run column profiles (required)

        Open `queries/02_column_profiles.sql`. Run and export → save to:
        ```
        {run_dir}/results/column_profiles.csv
        ```

        ### Step 3 — Run value overlap (optional)

        Only needed if you want to resolve ambiguous FK direction on
        {len(ambiguous)} flagged edge(s). Open `queries/03_value_overlap.sql`.
        Run and export → save to:
        ```
        {run_dir}/results/value_overlap.csv
        ```

        ### Step 4 — Run the pipeline

        Once result CSVs are in place, run Phase A:
        ```bash
        python -m rigor.pipeline \\
            --config rigor/config.yaml \\
            --run-dir {run_dir} \\
            --phase infer
        ```

        ---

        ## What was found

        | Source file | Edges contributed |
        |---|---|
        {ws_rows}

        **Top candidate edges by frequency:**
        {top_rows}

        {"**Ambiguous direction edges (see 03_value_overlap.sql):**" if ambiguous else "No ambiguous direction edges detected."}
        {"".join(chr(10) + f"- `{e.from_table}.{e.from_column}` <-> `{e.to_table}.{e.to_column}`" for e in ambiguous)}

        ---

        ## Run ancestry

        - **Run ID:** `{run_id}`
        - **Previous run:** see `runs/` directory for prior runs
        - **Pipeline execution:** recorded in `run_meta.json` → `downstream_run` after pipeline runs
        - **Worksheets hash:** see `run_meta.json` → `worksheets_hash` (changes if SQL files change)

        ---

        ## Results CSV schemas expected

        **profiling_edges.csv** columns:
        `from_table, from_column, to_table, to_column, sample_rows, fk_nonnull,
        match_count, match_rate, pk_distinct, pk_total, pk_unique_rate,
        fk_null_rate, confidence_sql, frequency, evidence`

        **column_profiles.csv** columns:
        `table_name, column_name, total_rows, non_null_count, null_rate,
        distinct_count, cardinality_ratio, min_val, max_val, inferred_type`

        **value_overlap.csv** columns (optional):
        `table_a, col_a, table_b, col_b, a_distinct, b_distinct,
        a_in_b_count, b_in_a_count, a_coverage, b_coverage, direction_suggestion`
    """)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _hash_files(files: list[Path]) -> str:
    h = hashlib.sha256()
    for f in sorted(files):
        h.update(f.read_bytes())
    return "sha256:" + h.hexdigest()[:16]


def _make_run_id(runs_dir: str, label: Optional[str]) -> str:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    runs_path = Path(runs_dir)
    runs_path.mkdir(parents=True, exist_ok=True)

    # Find the next counter for today
    existing = [
        d.name for d in runs_path.iterdir()
        if d.is_dir() and d.name.startswith(date_str)
    ]
    counter = len(existing) + 1
    suffix = f"_{label}" if label else ""
    return f"{date_str}_{counter:03d}{suffix}"
