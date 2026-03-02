-- =============================================================
-- RIGOR-SF | Run: 2026-03-02_004
-- File:    03_value_overlap.sql
-- Purpose: Bidirectional value overlap for 0 ambiguous edge(s)
--          to determine correct FK direction.
-- Generated: 2026-03-02T15:50:18Z
--
-- OPTIONAL — only needed if you want to resolve ambiguous edges.
-- Interpretation:
--   If table_b has fewer distinct values and b_coverage is near 1.0,
--   table_b is likely the parent (referred) table.
--
-- INSTRUCTIONS:
--   1. Run in Snowflake.
--   2. Export as CSV → save to: runs/2026-03-02_004/results/value_overlap.csv
-- =============================================================

-- No ambiguous edges detected. No queries to run.
SELECT 'no_ambiguous_edges' AS status;
