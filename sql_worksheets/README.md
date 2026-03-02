Place analyst worksheet exports here as `.sql` files.

## Accepted inputs

- File type: `.sql` only
- Source: existing analyst-authored Snowflake worksheets or query exports
- Typical content: business queries with explicit `JOIN ... ON ...` relationships

## Naming guidance

Use stable, descriptive filenames so runs are auditable over time, for example:

- `finance_orders_joins_2026_03.sql`
- `marketing_attribution_core.sql`
- `customer360_modeling_team.sql`

## Data safety

Do not include secrets, access tokens, or unnecessary PII literals in worksheet files.
Keep only SQL logic needed for relationship inference.

## Next step

Run Phase 0 to generate profiling SQL from these worksheets:

```bash
rigor --config config/config.yaml \
  --sql-dir sql_worksheets/ --phase query-gen
```
