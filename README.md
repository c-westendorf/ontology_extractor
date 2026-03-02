# RIGOR-SF: Ontology Generator for Snowflake

RIGOR-SF is an iterative ontology generation pipeline that transforms Snowflake schemas into OWL 2 DL ontologies through SQL analysis, data profiling, LLM-based generation, and human review.

## Overview

The pipeline extracts relationships from SQL worksheets, validates them with profiling data, and generates a semantically rich OWL ontology using a two-stage LLM approach (Generator + Judge).

### Key Features

- **5-Phase Pipeline**: Query-gen → Infer → Review → Generate → Validate
- **Offline-First Profiling**: SQL templates for analyst workflow (async, production-safe)
- **Evidence-Based**: Every assertion traces to SQL evidence, profiling stats, or human decision
- **Incremental Generation**: Skip unchanged tables, force-regenerate specific tables
- **Auto-Approve**: Configurable thresholds for automatic relationship approval
- **SPARQL Validation**: Coverage checking and duplicate IRI detection
- **Timestamped Artifacts**: Versioned outputs with symlinks to latest

---

## Quick Start

### 1. Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[snowflake]"
```

Without Snowflake dependencies:
```bash
pip install -e .
```

### 2. Configure

```bash
cp rigor_sf/config.example.yaml config/config.yaml
# Edit config/config.yaml with your settings
```

### 3. Ensure Cursor CLI Works

```bash
agent --help
```

If not found, install Cursor CLI per Cursor docs and ensure `agent` is on PATH.

---

## Pipeline Phases

### Phase 0: Query Generation

Generate profiling SQL templates from SQL worksheets:

```bash
rigor --config config/config.yaml \
  --sql-dir sql_worksheets/ --phase query-gen
```

**Outputs:**
- `runs/<timestamp>/queries/01_profiling_edges.sql`
- `runs/<timestamp>/queries/02_column_profiles.sql`
- `runs/<timestamp>/queries/03_value_overlap.sql`
- `runs/<timestamp>/run_meta.json`
- `runs/<timestamp>/README.md`

**Analyst Workflow:**
1. Download run folder
2. Execute SQL in Snowflake
3. Export results to `runs/<timestamp>/results/*.csv`
4. Return folder to pipeline

### Phase 1: Infer (Relationship Extraction)

Extract relationships and merge profiling data:

```bash
rigor --config config/config.yaml \
  --sql-dir sql_worksheets/ --run-dir runs/<timestamp>/ --phase infer
```

**Outputs:**
- `data/inferred_relationships.csv`
- `data/data_quality_report.json`

### Phase 2: Review (Human-in-the-Loop)

Launch the Streamlit review UI:

```bash
streamlit run rigor_sf/ui/app.py
```

**Features:**
- Filter relationships by status, confidence, match_rate
- Cell-level editing
- Bulk actions (flip direction, write overrides)
- Table classification (fact, dimension, entity, bridge, staging)

**Outputs:**
- Updated `data/inferred_relationships.csv`
- Updated `golden/overrides.yaml`

### Phase 3: Generate (OWL Creation)

Generate OWL ontology from approved relationships:

```bash
rigor --config config/config.yaml --phase generate
```

**Outputs:**
- `data/core.owl` (symlink to latest)
- `data/core_<timestamp>.owl` (versioned)
- `data/fragments/<TABLE>.ttl`
- `data/provenance.jsonl`

### Phase 4: Validate

Verify OWL quality and coverage:

```bash
rigor --config config/config.yaml --phase validate
```

**Outputs:**
- `data/validation_report.json`

**Checks:**
- OWL parse success
- Duplicate IRI detection
- Coverage (% of approved edges in ontology)
- Bridge table validation

---

## CLI Reference

### Full Command Syntax

```bash
rigor \
  --config PATH \
  --phase PHASE \
  [--sql-dir PATH] \
  [--run-dir PATH] \
  [--force-regenerate TABLE] \
  [--non-interactive]
```

### Options

| Option | Required | Description |
|--------|----------|-------------|
| `--config` | Yes | Path to config.yaml |
| `--phase` | Yes | Phase to run: `query-gen`, `infer`, `review`, `generate`, `validate`, `all` |
| `--sql-dir` | Phase 0,1 | Directory containing SQL worksheets |
| `--run-dir` | Phase 1 | Directory containing profiling results |
| `--force-regenerate` | No | Force re-generation of specific table(s) (repeatable) |
| `--non-interactive` | No | Skip interactive prompts; auto-skip on LLM failure |

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Configuration error |
| 2 | Phase prerequisite not met (e.g., missing profiling) |
| 3 | Validation failed (coverage < threshold) |
| 4 | LLM generation failed (after retries) |

---

## Example Workflows

### First-Time Setup (with Profiling)

```bash
# Phase 0: Generate profiling SQL
rigor --config config/config.yaml \
  --sql-dir sql_worksheets/ --phase query-gen

# [Analyst executes SQL in Snowflake, exports CSVs to runs/<ts>/results/]

# Phase 1: Infer relationships
rigor --config config/config.yaml \
  --sql-dir sql_worksheets/ --run-dir runs/2026-03-01_001/ --phase infer

# Phase 2: Review in UI
streamlit run rigor_sf/ui/app.py

# Phase 3: Generate OWL
rigor --config config/config.yaml --phase generate

# Phase 4: Validate
rigor --config config/config.yaml --phase validate
```

### Incremental Update (Single Table)

Force regeneration of a specific table:

```bash
rigor --config config/config.yaml \
  --phase generate --force-regenerate CUSTOMERS
```

Multiple tables:

```bash
rigor --config config/config.yaml \
  --phase generate --force-regenerate CUSTOMERS --force-regenerate ORDERS
```

### CI/CD Mode (Non-Interactive)

```bash
rigor --config config/config.yaml \
  --phase all --sql-dir sql_worksheets/ --non-interactive
```

### Run All Phases

```bash
rigor --config config/config.yaml \
  --phase all --sql-dir sql_worksheets/
```

---

## Configuration

See `rigor_sf/config.example.yaml` for full configuration options.

### Key Configuration Sections

```yaml
# LLM provider (only 'cursor' supported in v2)
llm:
  provider: "cursor"
  max_retries: 3
  interactive_on_failure: true

# Auto-approve thresholds
review:
  auto_approve_threshold: 0.95  # match_rate threshold
  auto_approve_confidence: 0.80  # SQL confidence threshold

# Ontology settings
ontology:
  base_iri: "http://example.org/rigor#"
  format: "xml"  # xml | turtle | n3

# Validation gates
validation:
  coverage_warn_threshold: 0.50
  coverage_pass_threshold: 0.90
```

---

## Data Files

### Input Files

| Path | Format | Purpose |
|------|--------|---------|
| `sql_worksheets/*.sql` | SQL | Join inference source |
| `metadata/tables.csv` | CSV | Table comments (table, comment) |
| `metadata/columns.csv` | CSV | Column comments (table, column, comment) |
| `config/config.yaml` | YAML | Pipeline configuration |
| `runs/<ts>/results/*.csv` | CSV | Profiling results from analyst |

### Output Files

| Path | Format | Purpose |
|------|--------|---------|
| `data/inferred_relationships.csv` | CSV | Review queue |
| `data/data_quality_report.json` | JSON | Profiling diagnostics |
| `golden/overrides.yaml` | YAML | Human decisions |
| `data/fragments/<TABLE>.ttl` | Turtle | Per-table OWL delta |
| `data/core.owl` | RDF/XML | Merged ontology (symlink) |
| `data/core_<timestamp>.owl` | RDF/XML | Versioned ontology |
| `data/provenance.jsonl` | JSONL | Generation audit trail |
| `data/validation_report.json` | JSON | Validation results |

---

## Troubleshooting

### "Prerequisite not met" (Exit Code 2)

This means profiling data is missing. Complete the analyst workflow:

1. Run Phase 0: `--phase query-gen`
2. Execute generated SQL in Snowflake
3. Export results to `runs/<ts>/results/`
4. Run Phase 1: `--phase infer --run-dir runs/<ts>/`

### "LLM generation failed" (Exit Code 4)

The LLM failed after all retries. Options:

1. Check Cursor CLI is working: `agent --help`
2. Run with `--non-interactive` to auto-skip failing tables
3. Use interactive mode to retry or provide manual TTL

### "Validation failed" (Exit Code 3)

Coverage is below the threshold. Check:

1. `data/validation_report.json` for missing edges
2. Ensure all approved relationships are in `golden/overrides.yaml`
3. Re-run generate phase for missing tables

### Symlink Issues on Windows

If symlinks fail, the pipeline falls back to file copy. The latest version is still accessible at `data/core.owl`.

### Low Match Rates

Low `match_rate` in profiling indicates potential data quality issues:

1. Check for NULL values in FK columns
2. Verify referential integrity in source data
3. Consider rejecting edges with `match_rate < 0.50`

---

## Snowflake Notes

Connection URL format:
```
snowflake://USER:PASSWORD@ACCOUNT/DB/SCHEMA?warehouse=WH&role=ROLE
```

For tables without enforced foreign keys, provide SQL worksheets for relationship inference.

---

## Lumina MCP (Optional)

HTTP connector for LLM-wrapped metadata at `rigor/metadata/lumina_mcp.py`.

Configure in `config.yaml`:
```yaml
metadata:
  lumina:
    enabled: true
    base_url: "https://your-lumina-instance"
    bearer_token: "your-token"
    timeout_seconds: 30
    retry_count: 2
```

---

## Testing

Run the test suite:

```bash
pytest rigor_sf/tests/ -v
```

With coverage:

```bash
pytest rigor_sf/tests/ --cov=rigor_sf --cov-report=term-missing
```

Run full post-migration verification (writes JSON + Markdown reports to `artifacts/`):

```bash
scripts/verify_migration.sh
```

---

## Documentation

- [SPEC_V2.md](SPEC_V2.md) - Full product specification
- [CONSTITUTION.md](CONSTITUTION.md) - Project principles
- [rigor_sf/config.example.yaml](rigor_sf/config.example.yaml) - Configuration reference

---

## Migration from v0

See [MIGRATION.md](MIGRATION.md) for upgrade instructions from the v0 prototype.

Key changes:
- `cursor_agent` config section renamed to `llm`
- New `ontology`, `review`, `validation`, `profiling` config sections
- New `query-gen` phase (Phase 0)
- Timestamped artifact versioning
- SPARQL-based validation
