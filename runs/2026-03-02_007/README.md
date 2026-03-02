        # RIGOR-SF Query Package
        ## Run: `2026-03-02_007`

        Generated: 2026-03-02 15:56 UTC
        Source: 2 SQL worksheet(s) → 3 candidate join edges

        ---

        ## What to do

        ### Step 1 — Run edge profiling (required)

        Open `queries/01_profiling_edges.sql` in your Snowflake worksheet
        or the Cursor Snowflake extension. Run the entire script.

        Export the result grid as CSV → save to:
        ```
        runs/2026-03-02_007/results/profiling_edges.csv
        ```

        ### Step 2 — Run column profiles (required)

        Open `queries/02_column_profiles.sql`. Run and export → save to:
        ```
        runs/2026-03-02_007/results/column_profiles.csv
        ```

        ### Step 3 — Run value overlap (optional)

        Only needed if you want to resolve ambiguous FK direction on
        0 flagged edge(s). Open `queries/03_value_overlap.sql`.
        Run and export → save to:
        ```
        runs/2026-03-02_007/results/value_overlap.csv
        ```

        ### Step 4 — Run the pipeline

        Once result CSVs are in place, run Phase A:
        ```bash
        python -m rigor.pipeline \
            --config rigor/config.yaml \
            --run-dir runs/2026-03-02_007 \
            --phase infer
        ```

        ---

        ## What was found

        | Source file | Edges contributed |
        |---|---|
        | customers_orders.sql | see query files |
| order_items.sql | see query files |

        **Top candidate edges by frequency:**
        - `ORDERS.CUSTOMER_ID` → `CUSTOMERS.ID` (frequency: 1, confidence: 0.95)
- `ORDER_ITEMS.ORDER_ID` → `ORDERS.ID` (frequency: 1, confidence: 0.95)
- `ORDER_ITEMS.PRODUCT_ID` → `PRODUCTS.ID` (frequency: 1, confidence: 0.95)

        No ambiguous direction edges detected.


        ---

        ## Run ancestry

        - **Run ID:** `2026-03-02_007`
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
