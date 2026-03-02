-- =============================================================
-- RIGOR-SF | Run: 2026-03-02_008
-- File:    02_column_profiles.sql
-- Purpose: Column-level quality profiles for 2 join columns
--          (null rate, cardinality, min/max, inferred type)
-- Generated: 2026-03-02T16:00:06Z
--
-- INSTRUCTIONS:
--   1. Run the ENTIRE script in Snowflake.
--   2. Export result as CSV.
--   3. Save to: runs/2026-03-02_008/results/column_profiles.csv
-- =============================================================

-- Column 1/2: TABLE_A.ID
SELECT
    'TABLE_A'                                       AS table_name,
    'ID'                                         AS column_name,
    COUNT(*)                                        AS total_rows,
    COUNT(ID)                                    AS non_null_count,
    IFF(COUNT(*) > 0,
        1 - COUNT(ID) / COUNT(*), 1)             AS null_rate,
    COUNT(DISTINCT ID)                           AS distinct_count,
    IFF(COUNT(ID) > 0,
        COUNT(DISTINCT ID) / COUNT(ID), 0)    AS cardinality_ratio,
    MIN(ID)::VARCHAR                             AS min_val,
    MAX(ID)::VARCHAR                             AS max_val,
    TYPEOF(MIN(ID))                              AS inferred_type
FROM TABLE_A
UNION ALL

-- Column 2/2: TABLE_B.A_ID
SELECT
    'TABLE_B'                                       AS table_name,
    'A_ID'                                         AS column_name,
    COUNT(*)                                        AS total_rows,
    COUNT(A_ID)                                    AS non_null_count,
    IFF(COUNT(*) > 0,
        1 - COUNT(A_ID) / COUNT(*), 1)             AS null_rate,
    COUNT(DISTINCT A_ID)                           AS distinct_count,
    IFF(COUNT(A_ID) > 0,
        COUNT(DISTINCT A_ID) / COUNT(A_ID), 0)    AS cardinality_ratio,
    MIN(A_ID)::VARCHAR                             AS min_val,
    MAX(A_ID)::VARCHAR                             AS max_val,
    TYPEOF(MIN(A_ID))                              AS inferred_type
FROM TABLE_B

ORDER BY table_name, column_name;
