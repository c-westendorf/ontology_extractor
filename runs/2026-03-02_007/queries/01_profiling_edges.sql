-- =============================================================
-- RIGOR-SF | Run: 2026-03-02_007
-- File:    01_profiling_edges.sql
-- Purpose: Profile 3 candidate join edges
--          (match_rate, pk_unique_rate, fk_null_rate)
-- Generated: 2026-03-02T15:56:33Z
--
-- INSTRUCTIONS:
--   1. Open this file in your Snowflake worksheet or
--      Cursor Snowflake extension.
--   2. Run the ENTIRE script (Cmd/Ctrl+A, then Run).
--   3. Export the result grid as CSV.
--   4. Save to: runs/2026-03-02_007/results/profiling_edges.csv
--
-- NOTE: Each edge is one SELECT block joined with UNION ALL.
--       The final result has one row per edge.
-- =============================================================
-- Edge 1/3: ORDERS.CUSTOMER_ID -> CUSTOMERS.ID
-- Evidence: /var/folders/32/x61r3nkd55dd30vm0g0c6wp00000gn/T/tmp01423vb2/sql_worksheets/customers_orders.sql
-- SQL confidence: 0.95  |  Frequency: 1 file(s)
SELECT
    'ORDERS'    AS from_table,
    'CUSTOMER_ID'   AS from_column,
    'CUSTOMERS'      AS to_table,
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
    '/var/folders/32/x61r3nkd55dd30vm0g0c6wp00000gn/T/tmp01423vb2/sql_worksheets/customers_orders.sql'                           AS evidence
FROM (
    SELECT
        COUNT(*)                        AS sample_rows,
        COUNT(CUSTOMER_ID)          AS fk_nonnull
    FROM ORDERS
    LIMIT 1000
) AS fk_stats,
(
    SELECT COUNT(*) AS match_count
    FROM (
        SELECT CUSTOMER_ID AS fk_val
        FROM ORDERS
        WHERE CUSTOMER_ID IS NOT NULL
        LIMIT 1000
    ) AS fk_sample
    INNER JOIN (
        SELECT DISTINCT ID AS pk_val
        FROM CUSTOMERS
    ) AS pk_set ON fk_sample.fk_val = pk_set.pk_val
) AS overlap,
(
    SELECT
        COUNT(*)                        AS pk_total,
        COUNT(DISTINCT ID)   AS pk_distinct
    FROM CUSTOMERS
) AS pk_stats
UNION ALL

-- Edge 2/3: ORDER_ITEMS.ORDER_ID -> ORDERS.ID
-- Evidence: /var/folders/32/x61r3nkd55dd30vm0g0c6wp00000gn/T/tmp01423vb2/sql_worksheets/order_items.sql
-- SQL confidence: 0.95  |  Frequency: 1 file(s)
SELECT
    'ORDER_ITEMS'    AS from_table,
    'ORDER_ID'   AS from_column,
    'ORDERS'      AS to_table,
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
    '/var/folders/32/x61r3nkd55dd30vm0g0c6wp00000gn/T/tmp01423vb2/sql_worksheets/order_items.sql'                           AS evidence
FROM (
    SELECT
        COUNT(*)                        AS sample_rows,
        COUNT(ORDER_ID)          AS fk_nonnull
    FROM ORDER_ITEMS
    LIMIT 1000
) AS fk_stats,
(
    SELECT COUNT(*) AS match_count
    FROM (
        SELECT ORDER_ID AS fk_val
        FROM ORDER_ITEMS
        WHERE ORDER_ID IS NOT NULL
        LIMIT 1000
    ) AS fk_sample
    INNER JOIN (
        SELECT DISTINCT ID AS pk_val
        FROM ORDERS
    ) AS pk_set ON fk_sample.fk_val = pk_set.pk_val
) AS overlap,
(
    SELECT
        COUNT(*)                        AS pk_total,
        COUNT(DISTINCT ID)   AS pk_distinct
    FROM ORDERS
) AS pk_stats
UNION ALL

-- Edge 3/3: ORDER_ITEMS.PRODUCT_ID -> PRODUCTS.ID
-- Evidence: /var/folders/32/x61r3nkd55dd30vm0g0c6wp00000gn/T/tmp01423vb2/sql_worksheets/order_items.sql
-- SQL confidence: 0.95  |  Frequency: 1 file(s)
SELECT
    'ORDER_ITEMS'    AS from_table,
    'PRODUCT_ID'   AS from_column,
    'PRODUCTS'      AS to_table,
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
    '/var/folders/32/x61r3nkd55dd30vm0g0c6wp00000gn/T/tmp01423vb2/sql_worksheets/order_items.sql'                           AS evidence
FROM (
    SELECT
        COUNT(*)                        AS sample_rows,
        COUNT(PRODUCT_ID)          AS fk_nonnull
    FROM ORDER_ITEMS
    LIMIT 1000
) AS fk_stats,
(
    SELECT COUNT(*) AS match_count
    FROM (
        SELECT PRODUCT_ID AS fk_val
        FROM ORDER_ITEMS
        WHERE PRODUCT_ID IS NOT NULL
        LIMIT 1000
    ) AS fk_sample
    INNER JOIN (
        SELECT DISTINCT ID AS pk_val
        FROM PRODUCTS
    ) AS pk_set ON fk_sample.fk_val = pk_set.pk_val
) AS overlap,
(
    SELECT
        COUNT(*)                        AS pk_total,
        COUNT(DISTINCT ID)   AS pk_distinct
    FROM PRODUCTS
) AS pk_stats

ORDER BY match_rate DESC;
