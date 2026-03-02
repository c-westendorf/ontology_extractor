# RIGOR-SF Onboarding: Worksheet-First Jump Start

## Who this is for

This guide is for a single owner running the end-to-end flow:
collect analyst SQL worksheets, run RIGOR phases, coordinate Snowflake profiling handoff, and review outputs.

For phase details, see [README.md](README.md#pipeline-phases).

## What you need from analysts

Provide existing analyst SQL worksheet exports as `.sql` files and place them in `sql_worksheets/`.

- Use production-like joins analysts actually trust.
- Include queries that represent important business relationships.
- No sample generator is included in this repo; onboarding assumes existing analyst exports.

Important: there is no prescribed automated Snowflake execution command in this repo.
Analysts execute generated profiling SQL manually in Snowflake worksheets or a Snowflake IDE extension.

## Minimal path

Use this when you only have SQL worksheets.

1. Place analyst `.sql` files in `sql_worksheets/`.
2. Generate profiling package:

```bash
rigor --config config/config.yaml \
  --sql-dir sql_worksheets/ --phase query-gen
```

3. Run infer without `--run-dir` (or before profiling CSVs are returned):

```bash
rigor --config config/config.yaml \
  --sql-dir sql_worksheets/ --phase infer
```

Result: extraction works, but confidence/profiling quality signals are weaker.

## Better path

Use this for higher-quality results.

1. SQL worksheets in `sql_worksheets/*.sql` (required).
2. Optional metadata context:
   - `metadata/tables.csv` (`table_name,comment`)
   - `metadata/columns.csv` (`table_name,column_name,comment`)
3. Profiling handoff:
   - Run `query-gen` first.
   - Analyst executes generated run SQL in Snowflake.
   - Analyst exports CSVs into `runs/<ts>/results/`.
4. Run infer with `--run-dir runs/<ts>/` to merge profiling data.

## End-to-end runbook

```bash
# Phase 0: Generate Snowflake profiling SQL from analyst worksheets
rigor --config config/config.yaml \
  --sql-dir sql_worksheets/ --phase query-gen

# Find the generated run instructions
ls -1dt runs/*/README.md | head -1

# Analyst step (manual, outside this CLI):
# - Open runs/<ts>/queries/01_profiling_edges.sql in Snowflake
# - Open runs/<ts>/queries/02_column_profiles.sql in Snowflake
# - Optionally run runs/<ts>/queries/03_value_overlap.sql
# - Export to runs/<ts>/results/

# Phase 1: Merge worksheet evidence + profiling signals
rigor --config config/config.yaml \
  --sql-dir sql_worksheets/ --run-dir runs/<timestamp>/ --phase infer

# Phase 2: Human review
streamlit run rigor_sf/ui/app.py

# Phase 3: Generate OWL
rigor --config config/config.yaml --phase generate

# Phase 4: Validate
rigor --config config/config.yaml --phase validate
```

Generated run instructions live under [runs/](runs/) at `runs/<ts>/README.md` and are required reading for the analyst handoff.

## How to interpret first outputs

- `data/inferred_relationships.csv`
  - Relationship candidates extracted from worksheet joins.
  - Contains status/confidence/evidence used by the review UI and generation.
- `data/data_quality_report.json`
  - Data quality and profiling diagnostics for extracted edges.
  - Use this to prioritize manual review when match rates are low or profiling is incomplete.

## Common failure modes

1. Empty or irrelevant worksheets
   - Symptom: very few/no candidate relationships.
   - Fix: add analyst queries with real `JOIN ... ON ...` predicates and rerun `query-gen` + `infer`.

2. Missing `runs/<ts>/results/` CSVs
   - Symptom: infer succeeds with warnings and weaker confidence signals.
   - Fix: complete analyst handoff from `runs/<ts>/README.md`, then rerun infer with `--run-dir`.

3. Low edge counts
   - Symptom: extracted relationship graph is sparse.
   - Fix: include more cross-domain analyst worksheets and ensure joins cover key business entities.
