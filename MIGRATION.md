# Migration Guide: v0 to v2

This guide covers upgrading from the RIGOR-SF v0 prototype to v2.

---

## Overview of Changes

### New Features in v2

| Feature | v0 | v2 |
|---------|----|----|
| Phases | 4 (A-D) | 5 (0-4) with explicit query-gen |
| Profiling | Live optional | Offline-first analyst workflow |
| Validation | Parse-only | SPARQL-based coverage + constraints |
| Versioning | None | Timestamped artifacts |
| Review | Always manual | Configurable auto-approve |
| Error Handling | Basic | Retry + interactive recovery |

---

## Configuration Migration

### 1. Rename `cursor_agent` to `llm`

**v0 (old):**
```yaml
cursor_agent:
  command: "agent"
  output_format: "json"
  debug: false
```

**v2 (new):**
```yaml
llm:
  provider: "cursor"
  model: "claude-3.5-sonnet"
  command: "agent"
  output_format: "json"
  debug: false
  max_retries: 3
  interactive_on_failure: true
```

### 2. Add `ontology` Section

**v2 (new):**
```yaml
ontology:
  base_iri: "http://example.org/rigor#"  # moved from hardcoded prompts.py
  format: "xml"  # xml | turtle | n3
  naming: "standard"  # PascalCase classes, camelCase properties
```

### 3. Add `review` Section

**v2 (new):**
```yaml
review:
  auto_approve_threshold: 0.95  # match_rate threshold
  auto_approve_confidence: 0.80  # SQL confidence threshold
  require_human_review: true
```

### 4. Add `profiling` Section

**v2 (new):**
```yaml
profiling:
  sample_limit: 200000
  match_rate_threshold: 0.90
  null_rate_warning: 0.20
  frequency_boost_5: 0.05
  frequency_boost_10: 0.10
```

### 5. Add `validation` Section

**v2 (new):**
```yaml
validation:
  coverage_warn_threshold: 0.50
  coverage_pass_threshold: 0.90
  allow_duplicate_iris: false
```

### 6. Update `paths` Section

**v2 additions:**
```yaml
paths:
  # ... existing paths ...
  runs_dir: "runs"  # NEW: base directory for query-gen runs
  data_quality_report: "data/data_quality_report.json"  # NEW
  validation_report: "data/validation_report.json"  # NEW
```

### 7. Update `lumina` Section

**v2 additions:**
```yaml
metadata:
  lumina:
    # ... existing fields ...
    timeout_seconds: 30  # NEW
    retry_count: 2  # NEW
```

---

## File Structure Changes

### New Directories

```
project/
├── runs/                           # NEW: Query-gen runs
│   └── <timestamp>/
│       ├── queries/
│       │   ├── 01_profiling_edges.sql
│       │   ├── 02_column_profiles.sql
│       │   └── 03_value_overlap.sql
│       ├── results/               # Analyst exports CSVs here
│       ├── run_meta.json
│       └── README.md
```

### New Output Files

```
data/
├── core.owl                        # Now a symlink to latest
├── core_2026-03-01T12-34-56Z.owl  # NEW: Versioned artifact
├── data_quality_report.json        # NEW: Profiling diagnostics
├── validation_report.json          # NEW: Validation results
└── validation_report_<ts>.json     # NEW: Versioned validation
```

---

## CLI Changes

### New Phase: `query-gen`

v2 adds Phase 0 for generating profiling SQL templates:

```bash
# NEW in v2
rigor --config config/config.yaml \
  --sql-dir sql_worksheets/ --phase query-gen
```

### New Arguments

| Argument | Description |
|----------|-------------|
| `--force-regenerate TABLE` | Force re-generation of specific table(s) |
| `--non-interactive` | Skip interactive prompts; auto-skip on failure |

### Phase Flow

**v0:**
```
infer → review → generate → validate
```

**v2:**
```
query-gen → [analyst workflow] → infer → review → generate → validate
```

---

## Workflow Changes

### Profiling is Now Offline-First

In v0, profiling could run live against the database. In v2, profiling follows an analyst workflow:

1. **Phase 0 (query-gen)**: Generate SQL templates
2. **Analyst Workflow**: Execute SQL in Snowflake, export CSVs
3. **Phase 1 (infer)**: Load profiling results

This decouples the pipeline from production databases and allows for async workflows.

### Auto-Approve Reduces Manual Review

Configure thresholds to auto-approve high-confidence edges:

```yaml
review:
  auto_approve_threshold: 0.95
  auto_approve_confidence: 0.80
```

Edges meeting both thresholds are auto-approved with `[auto-approved]` in the evidence field.

### Incremental Generation

v2 caches table fingerprints and skips unchanged tables:

```bash
# Force regeneration of specific tables
rigor --phase generate --force-regenerate CUSTOMERS
```

---

## Exit Codes

v2 introduces semantic exit codes:

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Configuration error |
| 2 | Prerequisite not met (missing profiling) |
| 3 | Validation failed |
| 4 | LLM generation failed |

---

## Breaking Changes

### 1. `base_iri` Moved to Config

The `BASE_IRI` constant in `prompts.py` is now configured via `ontology.base_iri`.

**Action:** Add `ontology.base_iri` to your config.yaml.

### 2. Profiling Required for Generate

Phase 3 (generate) now requires profiling data. Running generate without profiling exits with code 2.

**Action:** Complete the analyst workflow before running generate.

### 3. Validation Report Schema

The validation report now includes SPARQL-based coverage metrics:

```json
{
  "coverage": {
    "approved_edges": 45,
    "covered_edges": 43,
    "coverage_rate": 0.956,
    "missing_edges": [...]
  }
}
```

**Action:** Update any scripts that parse validation_report.json.

---

## Migration Checklist

- [ ] Update `config.yaml` with new sections (llm, ontology, review, profiling, validation)
- [ ] Rename `cursor_agent` to `llm` in config
- [ ] Add `ontology.base_iri` (was hardcoded)
- [ ] Create `runs/` directory for query-gen outputs
- [ ] Update CI/CD scripts for new exit codes
- [ ] Update documentation references to new phase names
- [ ] Test with `--phase query-gen` before running infer

---

## Full Config Template

See [rigor_sf/config.example.yaml](rigor_sf/config.example.yaml) for a complete v2 configuration template with inline comments.
