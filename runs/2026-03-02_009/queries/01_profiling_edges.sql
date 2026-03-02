-- =============================================================
-- RIGOR-SF | Run: 2026-03-02_009
-- File:    01_profiling_edges.sql
-- Purpose: Profile 1 candidate join edges
--          (match_rate, pk_unique_rate, fk_null_rate)
-- Generated: 2026-03-02T16:00:06Z
--
-- INSTRUCTIONS:
--   1. Open this file in your Snowflake worksheet or
--      Cursor Snowflake extension.
--   2. Run the ENTIRE script (Cmd/Ctrl+A, then Run).
--   3. Export the result grid as CSV.
--   4. Save to: runs/2026-03-02_009/results/profiling_edges.csv
--
-- NOTE: Each edge is one SELECT block joined with UNION ALL.
--       The final result has one row per edge.
-- =============================================================
-- Edge 1/1: TABLE_B.A_ID -> TABLE_A.ID
-- Evidence: /private/var/folders/32/x61r3nkd55dd30vm0g0c6wp00000gn/T/pytest-of-chris/pytest-14/test_query_gen_creates_run_fol0/sql_worksheets/test.sql
-- SQL confidence: 0.95  |  Frequency: 1 file(s)
SELECT
    'TABLE_B'    AS from_table,
    'A_ID'   AS from_column,
    'TABLE_A'      AS to_table,
    'ID'     AS to_column,
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
    0.95                                          AS confidence_sql,
    1                                               AS frequency,
    '/private/var/folders/32/x61r3nkd55dd30vm0g0c6wp00000gn/T/pytest-of-chris/pytest-14/test_query_gen_creates_run_fol0/sql_worksheets/test.sql'                           AS evidence
FROM (
    SELECT
        COUNT(*)                        AS sample_rows,
        COUNT(A_ID)          AS fk_nonnull
    FROM TABLE_B
    LIMIT 200000
) AS fk_stats,
(
    SELECT COUNT(*) AS match_count
    FROM (
        SELECT A_ID AS fk_val
        FROM TABLE_B
        WHERE A_ID IS NOT NULL
        LIMIT 200000
    ) AS fk_sample
    INNER JOIN (
        SELECT DISTINCT ID AS pk_val
        FROM TABLE_A
    ) AS pk_set ON fk_sample.fk_val = pk_set.pk_val
) AS overlap,
(
    SELECT
        COUNT(*)                        AS pk_total,
        COUNT(DISTINCT ID)   AS pk_distinct
    FROM TABLE_A
) AS pk_stats

ORDER BY match_rate DESC;
