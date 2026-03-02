-- =============================================================
-- RIGOR-SF | Run: 2026-03-02_006
-- File:    02_column_profiles.sql
-- Purpose: Column-level quality profiles for 6 join columns
--          (null rate, cardinality, min/max, inferred type)
-- Generated: 2026-03-02T15:52:37Z
--
-- INSTRUCTIONS:
--   1. Run the ENTIRE script in Snowflake.
--   2. Export result as CSV.
--   3. Save to: runs/2026-03-02_006/results/column_profiles.csv
-- =============================================================

-- Column 1/6: CUSTOMERS.ID
SELECT
    'CUSTOMERS'                                       AS table_name,
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
FROM CUSTOMERS
UNION ALL

-- Column 2/6: ORDERS.CUSTOMER_ID
SELECT
    'ORDERS'                                       AS table_name,
    'CUSTOMER_ID'                                         AS column_name,
    COUNT(*)                                        AS total_rows,
    COUNT(CUSTOMER_ID)                                    AS non_null_count,
    IFF(COUNT(*) > 0,
        1 - COUNT(CUSTOMER_ID) / COUNT(*), 1)             AS null_rate,
    COUNT(DISTINCT CUSTOMER_ID)                           AS distinct_count,
    IFF(COUNT(CUSTOMER_ID) > 0,
        COUNT(DISTINCT CUSTOMER_ID) / COUNT(CUSTOMER_ID), 0)    AS cardinality_ratio,
    MIN(CUSTOMER_ID)::VARCHAR                             AS min_val,
    MAX(CUSTOMER_ID)::VARCHAR                             AS max_val,
    TYPEOF(MIN(CUSTOMER_ID))                              AS inferred_type
FROM ORDERS
UNION ALL

-- Column 3/6: ORDERS.ID
SELECT
    'ORDERS'                                       AS table_name,
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
FROM ORDERS
UNION ALL

-- Column 4/6: ORDER_ITEMS.ORDER_ID
SELECT
    'ORDER_ITEMS'                                       AS table_name,
    'ORDER_ID'                                         AS column_name,
    COUNT(*)                                        AS total_rows,
    COUNT(ORDER_ID)                                    AS non_null_count,
    IFF(COUNT(*) > 0,
        1 - COUNT(ORDER_ID) / COUNT(*), 1)             AS null_rate,
    COUNT(DISTINCT ORDER_ID)                           AS distinct_count,
    IFF(COUNT(ORDER_ID) > 0,
        COUNT(DISTINCT ORDER_ID) / COUNT(ORDER_ID), 0)    AS cardinality_ratio,
    MIN(ORDER_ID)::VARCHAR                             AS min_val,
    MAX(ORDER_ID)::VARCHAR                             AS max_val,
    TYPEOF(MIN(ORDER_ID))                              AS inferred_type
FROM ORDER_ITEMS
UNION ALL

-- Column 5/6: ORDER_ITEMS.PRODUCT_ID
SELECT
    'ORDER_ITEMS'                                       AS table_name,
    'PRODUCT_ID'                                         AS column_name,
    COUNT(*)                                        AS total_rows,
    COUNT(PRODUCT_ID)                                    AS non_null_count,
    IFF(COUNT(*) > 0,
        1 - COUNT(PRODUCT_ID) / COUNT(*), 1)             AS null_rate,
    COUNT(DISTINCT PRODUCT_ID)                           AS distinct_count,
    IFF(COUNT(PRODUCT_ID) > 0,
        COUNT(DISTINCT PRODUCT_ID) / COUNT(PRODUCT_ID), 0)    AS cardinality_ratio,
    MIN(PRODUCT_ID)::VARCHAR                             AS min_val,
    MAX(PRODUCT_ID)::VARCHAR                             AS max_val,
    TYPEOF(MIN(PRODUCT_ID))                              AS inferred_type
FROM ORDER_ITEMS
UNION ALL

-- Column 6/6: PRODUCTS.ID
SELECT
    'PRODUCTS'                                       AS table_name,
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
FROM PRODUCTS

ORDER BY table_name, column_name;
