# Product Specification — RIGOR-SF v2

**Date:** 2026-03-01
**Owner:** Chris (@westendorf.chris)
**Status:** Draft v2 (audit-driven revision)
**Supersedes:** PRODUCT_SPEC.md (v0)

---

## 1. Executive Summary

RIGOR-SF is an iterative ontology generation pipeline that transforms Snowflake schemas into OWL 2 DL ontologies through a combination of SQL analysis, data profiling, LLM-based generation, and human review. This v2 specification is based on a comprehensive audit of the rigor_v1 prototype.

### 1.1 Key Changes from v0

| Aspect | v0 | v2 |
|--------|----|----|
| Phases | 4 (A-D) | 5 (0-4) with explicit query-gen phase |
| LLM Integration | Cursor-only | Cursor-only (interface prepared for future providers) |
| Profiling | Live optional | Offline-first with analyst workflow (async, may take days) |
| Validation | Parse-only | SPARQL-based coverage + constraint checking |
| Versioning | None | Timestamped artifacts (suffix pattern) |
| Testing | None | Required pytest suite |
| Review Gate | Always manual | Configurable auto-approve threshold |
| Scale Target | Undefined | Medium (50-200 tables) |

---

## 2. Goals

### 2.1 Primary Goal
Produce the most **precise**, **auditable**, and **maintainable** OWL ontology representing the organization's Snowflake data world model.

### 2.2 Secondary Goals
1. **Evidence-First**: Every assertion traces to SQL evidence, profiling stats, or explicit human decision
2. **Reproducibility**: Deterministic inputs → deterministic outputs; overrides ensure replayability
3. **Human Authority**: Review UI produces machine-readable overrides that take precedence over automation
4. **Incremental Generation**: Process tables in dependency order; support delta updates

### 2.3 Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Edge Coverage | ≥90% of approved edges in OWL | SPARQL validation |
| Parse Success | 100% of fragments valid Turtle | RDFLib parse |
| Human Review Rate | ≤20% edges require manual decision | Proposed → Approved ratio |
| Generation Time | <30s per table | Pipeline instrumentation |

---

## 3. Non-Goals (v2 Scope)

- Full SQL grammar coverage (CTEs, complex predicates, schema qualifiers)
- Real-time / streaming ontology updates
- Automated business metrics definitions
- External ontology repository integration (BioPortal planned for v3)
- Multi-user concurrent review (single analyst workflow)
- Very large scale (500+ tables) — requires optimization deferred to v3
- Alternative LLM providers (OpenAI, local models) — interface only in v2

---

## 4. Users & Roles

| Role | Responsibilities | Primary Touchpoints |
|------|------------------|---------------------|
| **Ontology Builder** | Runs phases 0-4, configures pipeline | CLI, config.yaml |
| **Data Analyst** | Executes profiling SQL in Snowflake, exports CSVs | SQL templates, Snowflake UI |
| **Domain Reviewer** | Reviews relationships, approves/rejects, classifies tables | Streamlit UI |
| **Ontology Consumer** | Uses core.owl + provenance for downstream apps | RDF files |

---

## 5. System Architecture

### 5.1 High-Level Design

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              RIGOR-SF Pipeline                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Phase 0          Phase 1          Phase 2         Phase 3       Phase 4   │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  ┌─────────┐ │
│  │ Query    │───▶│ Infer    │───▶│ Review   │───▶│ Generate │─▶│Validate │ │
│  │ Gen      │    │          │    │ (UI)     │    │          │  │         │ │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘  └─────────┘ │
│       │               │               │               │              │      │
│       ▼               ▼               ▼               ▼              ▼      │
│  SQL templates   relationships   overrides.yaml   core.owl    report.json  │
│  run_meta.json   .csv + DQ       (updated)        fragments/               │
│                  report                           provenance               │
│                                                                             │
│  ◀─────────────── Offline Analyst Workflow ───────────────▶                │
│     (Execute SQL in Snowflake, export CSVs, return)                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Core Principles

1. **Stateless Phases**: Each phase reads previous outputs, writes new ones; can re-run independently
2. **Offline-First Profiling**: SQL templates generated for analyst to execute; decouples from production DB
3. **Two-Stage LLM**: Generator for creativity, Judge for correctness
4. **Topological Processing**: Tables sorted by FK dependencies; parents before children

### 5.3 Component Inventory

| Component | Module | Purpose |
|-----------|--------|---------|
| Config | `config.py` | YAML + Pydantic validation |
| SQL Parser | `sql_ingest.py` | Extract joins from worksheets |
| Query Generator | `query_gen.py` | Profiling SQL templates |
| Run Loader | `run_loader.py` | Merge profiling CSVs |
| Schema Inspector | `db_introspect.py` | SQLAlchemy reflection |
| Traverser | `traverse.py` | Topological sort |
| Overrides | `overrides.py` | YAML approval/rejection |
| Prompts | `prompts.py` | LLM prompt construction |
| LLM Client | `cursor_cli.py` → `llm_provider.py` (v2) | LLM invocation |
| OWL Merger | `owl.py` | RDFLib graph merge |
| Retrieval | `retrieval/` | Context snippets |
| Metadata | `metadata/` | CSV + Lumina enrichment |
| UI | `ui/app.py` | Streamlit review |
| Pipeline | `pipeline.py` | Orchestration |

---

## 6. Workflow Phases

### Phase 0 — Query Generation

**Command:**
```bash
python -m rigor.pipeline --config rigor/config.yaml --sql-dir sql_worksheets/ --phase query-gen
```

**Inputs:**
- SQL worksheets (`sql_worksheets/*.sql`)

**Outputs:**
- `runs/<timestamp>/queries/01_profiling_edges.sql`
- `runs/<timestamp>/queries/02_column_profiles.sql`
- `runs/<timestamp>/queries/03_value_overlap.sql`
- `runs/<timestamp>/run_meta.json`
- `runs/<timestamp>/README.md`

**Behavior:**
1. Parse SQL files, extract join edges
2. Build directed edge candidates with confidence scores
3. Generate profiling SQL with UNION ALL pattern
4. Write run metadata with worksheets hash for cache invalidation

**Analyst Workflow:**
1. Download run folder
2. Execute SQL in Snowflake
3. Export results to `runs/.../results/*.csv`
4. Return folder to pipeline

**Timeline:** Async workflow — analyst may take multiple days. Pipeline waits indefinitely for profiling CSVs before proceeding to Phase 3 (Generate).

---

### Phase 1 — Infer (Relationship Extraction)

**Command:**
```bash
python -m rigor.pipeline --config rigor/config.yaml --sql-dir sql_worksheets/ --run-dir runs/<timestamp>/ --phase infer
```

**Inputs:**
- SQL worksheets (re-parsed)
- Profiling CSVs (from analyst)
- Existing overrides (if any)

**Outputs:**
- `data/inferred_relationships.csv`
- `data/data_quality_report.json`

**Behavior:**
1. Ingest SQL joins → raw edges
2. Load profiling CSVs via RunLoader
3. Merge stats (match_rate, pk_unique_rate, fk_null_rate)
4. Apply direction corrections from value_overlap
5. Apply override statuses (approved/rejected)
6. Generate data quality report (warnings for high null rates, low match rates)

---

### Phase 2 — Review (Human-in-the-Loop)

**Command:**
```bash
streamlit run -m rigor.ui.app
```

**Tabs:**

1. **Relationships Tab**
   - Filter by status, confidence, match_rate, keyword
   - Cell-level editing of all columns
   - Bulk actions: flip direction, write overrides

2. **Table Classification Tab**
   - Assign: fact, dimension, entity, bridge, staging
   - Automatic suggestion based on edge degree
   - Bulk save to overrides.yaml

3. **How to Run Tab**
   - Workflow instructions

**Outputs:**
- Updated `data/inferred_relationships.csv`
- Updated `golden/overrides.yaml`

**Auto-Approve Mode:**
Phase 2 can be partially automated via configurable thresholds:
- Edges with `match_rate ≥ auto_approve_threshold` AND `confidence_sql ≥ auto_approve_confidence` are auto-approved
- Remaining edges require human review
- Configure in `config.yaml`:
  ```yaml
  review:
    auto_approve_threshold: 0.95    # match_rate threshold
    auto_approve_confidence: 0.80   # SQL confidence threshold
    require_human_review: true      # if false, skip UI entirely for auto-approved
  ```

---

### Phase 3 — Generate (OWL Creation)

**Command:**
```bash
python -m rigor.pipeline --config rigor/config.yaml --phase generate
```

**Inputs:**
- Schema (Snowflake introspection or offline)
- `data/inferred_relationships.csv`
- `golden/overrides.yaml`
- Metadata CSVs + Lumina (optional)
- Existing `data/core.owl` (if incremental)

**Outputs:**
- `data/core.owl` (RDF/XML)
- `data/fragments/<TABLE>.ttl`
- `data/provenance.jsonl`

**Behavior:**
For each table (topologically sorted):
1. Check incremental cache — skip if no schema/relationship changes since last run
2. Build schema context with classification
3. Retrieve core ontology snippets
4. Retrieve external ontology hints (v3)
5. Call LLM Generator → JSON header + Turtle
6. Call LLM Judge → validated Turtle
7. Validate relation names against overrides (warn if mismatch)
8. Merge fragment into core graph
9. Log provenance (created entities, assumptions, classification)

**Incremental Generation:**
A table is re-generated when:
- Schema changed (columns, types, comments)
- Relationships changed (FK added/removed/modified to or from this table)
- Explicit `--force-regenerate TABLE` flag used

**LLM Prompts:**
- Generator: TABLE_CLASSIFICATION + SCHEMA + CORE_SNIPPETS + EXTERNAL_HINTS
- Judge: SCHEMA + CANDIDATE_TURTLE + CORE_SNIPPETS

**Failure Handling:**
If LLM fails after `max_retries`:
1. Log error with full context
2. Prompt user interactively: "Table X failed. (S)kip / (R)etry / (H)alt?"
3. If batch mode (`--non-interactive`), skip table and continue

**Profiling Requirement:**
Generation phase **requires** profiling CSVs. If profiling is missing:
- Pipeline fails with clear error message
- User must complete Phase 0 → analyst workflow → Phase 1 before Phase 3

---

### Phase 4 — Validate

**Command:**
```bash
python -m rigor.pipeline --config rigor/config.yaml --phase validate
```

**Inputs:**
- `data/core.owl`
- `data/inferred_relationships.csv`

**Outputs:**
- `data/validation_report.json`

**Checks:**
1. OWL parse success (RDFLib)
2. Duplicate IRI detection
3. Coverage: % of approved edges represented in ontology (SPARQL-based in v2)
4. Constraint consistency (planned v3)

**Gates:**
- Coverage ≥ 50% (warning) / ≥ 90% (pass)
- Zero duplicate IRIs (pass) / >0 (fail)

---

## 7. Data Contracts

### 7.1 Input Files

| Path | Format | Purpose |
|------|--------|---------|
| `sql_worksheets/*.sql` | SQL | Join inference source |
| `metadata/tables.csv` | CSV | Table comments |
| `metadata/columns.csv` | CSV | Column comments |
| `rigor/config.yaml` | YAML | Pipeline configuration |
| `runs/<ts>/results/*.csv` | CSV | Profiling results from analyst |

### 7.2 Output Files

| Path | Format | Purpose |
|------|--------|---------|
| `data/inferred_relationships.csv` | CSV | Review queue |
| `data/data_quality_report.json` | JSON | Profiling diagnostics |
| `golden/overrides.yaml` | YAML | Durable decisions |
| `data/fragments/<TABLE>.ttl` | Turtle | Per-table delta |
| `data/core.owl` | RDF/XML | Merged ontology |
| `data/provenance.jsonl` | JSONL | Generation audit trail |
| `data/validation_report.json` | JSON | Validation results |

### 7.3 Relationships CSV Schema (v1)

```csv
from_table,from_column,from_columns,to_table,to_column,to_columns,
confidence_sql,frequency,match_rate,pk_unique_rate,fk_null_rate,
status,evidence,data_quality_flag
```

**Notes:**
- `from_columns`/`to_columns`: semicolon-separated for composite keys
- `from_column`/`to_column`: legacy single-column (first of composite)
- `status`: proposed | approved | rejected
- `data_quality_flag`: ok | warning | error

### 7.4 Overrides YAML Schema

```yaml
approve:
  - from: { table: TABLE, columns: [COL1, COL2] }
    to:   { table: TABLE, columns: [COL1, COL2] }
    relation: optionalSemanticName  # e.g., "places", "belongsTo"

reject:
  - from: { table: TABLE, columns: [COL] }
    to:   { table: TABLE, columns: [COL] }

table_classification:
  ORDERS: fact
  CUSTOMERS: dimension
  ORDER_ITEMS: bridge
  STG_EVENTS: staging

rename: []  # future: rename columns/tables in OWL
```

**Relation Name Validation:**
When `relation` is specified in an override:
- Pipeline validates that generated OWL contains property with matching name
- If LLM generates different property name, log warning in validation report
- Warning includes: expected name, actual name, suggestion to update override or re-prompt

### 7.5 Provenance JSONL Schema

```json
{
  "table": "ORDERS",
  "timestamp": "2026-03-01T12:34:56Z",
  "classification": "fact",
  "created_entities": ["orders:Order", "orders:hasCustomer"],
  "assumptions": ["Assumed 1:N relationship based on match_rate 0.95"],
  "generator_model": "claude-3.5-sonnet",
  "judge_model": "claude-3.5-sonnet",
  "fragment_path": "data/fragments/ORDERS.ttl"
}
```

---

## 8. OWL Generation Rules

### 8.1 Allowed Constructs

The LLM may generate the following OWL 2 DL constructs:

| Construct | Usage | Example |
|-----------|-------|---------|
| `owl:Class` | Table → Class mapping | `:Customer a owl:Class` |
| `owl:ObjectProperty` | FK → Relationship | `:hasOrder a owl:ObjectProperty` |
| `owl:DatatypeProperty` | Column → Data mapping | `:customerName a owl:DatatypeProperty` |
| `rdfs:domain` / `rdfs:range` | Property constraints | `:hasOrder rdfs:domain :Customer` |
| `owl:FunctionalProperty` | 1:1 relationships | When pk_unique_rate = 1.0 |
| `owl:cardinality` | Cardinality restrictions | Based on profiling stats |
| `rdfs:subClassOf` | Class hierarchy | Inferred from naming patterns |

### 8.2 IRI Naming Convention

**Standard:** PascalCase for classes, camelCase for properties

| Source | OWL IRI | Example |
|--------|---------|---------|
| Table `CUSTOMERS` | Class `:Customer` | Singular, PascalCase |
| Column `CUSTOMER_ID` | Property `:customerId` | camelCase |
| FK relationship | Property `:hasCustomer` | camelCase with `has` prefix |
| Bridge table | Reified class | `:OrderItem` (not `:ORDER_ITEMS`) |

### 8.3 Classification-Driven Patterns

Table classification enforces structural patterns in generated OWL:

| Classification | OWL Pattern | Description |
|----------------|-------------|-------------|
| **fact** | Central class with many incoming properties | Event/transaction records |
| **dimension** | Class with outgoing properties to facts | Descriptive attributes |
| **entity** | Standalone class, may have sub-hierarchy | Core business concept |
| **bridge** | Reified association class | M:N relationship table → class with two object properties |
| **staging** | Annotated with `rigor:staging true` | Lower confidence, ETL artifacts |

**Bridge Table Pattern:**
```turtle
# Table ORDER_ITEMS bridges ORDERS and PRODUCTS
:OrderItem a owl:Class ;
    rdfs:comment "Bridge table linking orders to products" ;
    rigor:classification "bridge" .

:orderItemOrder a owl:ObjectProperty ;
    rdfs:domain :OrderItem ;
    rdfs:range :Order .

:orderItemProduct a owl:ObjectProperty ;
    rdfs:domain :OrderItem ;
    rdfs:range :Product .
```

### 8.4 Scale Considerations

**Target: 50-200 tables**

| Aspect | Approach |
|--------|----------|
| Topological batching | Process 10 tables per batch, merge fragments |
| Core snippet retrieval | Limit to 20 most relevant lines per table |
| Fragment caching | Skip unchanged tables (incremental mode) |
| Memory | Stream large OWL files, don't load entire graph |

---

## 9. Configuration Schema

```yaml
db:
  url: "snowflake://USER:PASSWORD@ACCOUNT/DB/SCHEMA?warehouse=WH&role=ROLE"
  schema: null           # override if different from URL
  include_tables: []     # whitelist; empty = all
  exclude_tables: []     # blacklist

llm:                     # v2: Cursor-only, interface prepared for future
  provider: "cursor"     # cursor (only supported in v2)
  model: "claude-3.5-sonnet"
  command: "agent"       # for cursor provider
  output_format: "json"
  debug: false
  max_retries: 3
  interactive_on_failure: true  # prompt user on failure; false = skip

review:                  # NEW in v2: auto-approve configuration
  auto_approve_threshold: 0.95    # match_rate threshold for auto-approve
  auto_approve_confidence: 0.80   # SQL confidence threshold
  require_human_review: true      # if false, skip UI for auto-approved edges

paths:
  core_in: "data/core.owl"
  core_out: "data/core.owl"
  provenance_jsonl: "data/provenance.jsonl"
  fragments_dir: "data/fragments"
  inferred_relationships_csv: "data/inferred_relationships.csv"
  overrides_yaml: "golden/overrides.yaml"
  runs_dir: "runs"       # NEW: base directory for query-gen runs

ontology:                # NEW in v2
  base_iri: "http://example.org/rigor#"  # moved from hardcoded
  format: "xml"          # xml | turtle | n3
  naming: "standard"     # standard = PascalCase classes, camelCase properties

metadata:
  tables_csv: "metadata/tables.csv"
  columns_csv: "metadata/columns.csv"
  lumina:
    enabled: false
    base_url: ""
    bearer_token: ""
    chat_path: "/chat"
    extra_headers: {}
    strict_json: true
    timeout_seconds: 30  # NEW
    retry_count: 2       # NEW

profiling:               # NEW in v2
  sample_limit: 200000
  match_rate_threshold: 0.90
  null_rate_warning: 0.20
  frequency_boost_5: 0.05
  frequency_boost_10: 0.10

validation:              # NEW in v2
  coverage_warn_threshold: 0.50
  coverage_pass_threshold: 0.90
  allow_duplicate_iris: false
```

---

## 10. Trust Hierarchy & Confidence Scoring

### 10.1 Trust Levels (Highest to Lowest)

1. **Explicit Human Approval** — Always included in OWL
2. **High-Evidence Profiling** — match_rate ≥ 0.90, included automatically
3. **SQL-Inferred Candidates** — confidence_sql ≥ 0.70, proposed for review
4. **LLM Suggestions** — Never auto-promoted; require human approval

### 10.2 SQL Confidence Calculation

```
Base: 0.60 (join exists in SQL)
+ 0.10 if left column matches ID pattern
+ 0.10 if right column matches ID pattern
+ 0.15 if pattern suggests FK direction (*_ID → ID)
+ 0.05 if edge appears in 5+ worksheets
+ 0.10 if edge appears in 10+ worksheets
Max: 0.95
```

### 10.3 Profiling Statistics

| Stat | Meaning | Threshold |
|------|---------|-----------|
| `match_rate` | % of FK values found in PK | ≥0.90 = high confidence |
| `pk_unique_rate` | Cardinality of referred column | =1.0 = true PK |
| `fk_null_rate` | % of FK column that is null | ≤0.20 = acceptable |
| `frequency` | # of worksheets containing edge | ≥5 = boost confidence |

### 10.4 Data Quality Flags

| Flag | Condition | Action |
|------|-----------|--------|
| `ok` | All thresholds pass | Auto-include if approved or high evidence |
| `warning` | null_rate > 0.20 OR match_rate < 0.90 | Require human review |
| `error` | match_rate < 0.50 OR pk_unique_rate < 0.80 | Flag for investigation |

---

## 11. Rules (Do / Don't)

### 11.1 Do

- **Do** trace every assertion to evidence (SQL snippet, profiling stat, or override)
- **Do** store all human decisions in `golden/overrides.yaml`
- **Do** regenerate from scratch when prompts change; overrides ensure stability
- **Do** treat generation as a compiler: deterministic inputs → deterministic outputs
- **Do** version artifacts with timestamps for audit trail
- **Do** enhance existing functions rather than creating new ones

### 11.2 Don't

- **Don't** allow LLM to invent relationships not in approved/high-evidence candidates
- **Don't** promote low-evidence relationships to OWL restrictions without profiling
- **Don't** hardcode values that belong in configuration
- **Don't** skip human review for edges with data quality warnings
- **Don't** overwrite artifacts without versioning

---

## 12. Identified Gaps & Remediation Plan

### 12.1 Critical Gaps (v2 Must-Fix)

| ID | Gap | Current State | v2 Remediation |
|----|-----|---------------|----------------|
| G1 | External ontologies | Stub returns [] | Defer to v3 |
| G2 | LLM abstraction | Hardcoded Cursor | Keep Cursor-only; define interface for future |
| G5 | Coverage validation | Substring matching | SPARQL-based verification |
| G6 | No versioning | Overwrites artifacts | Timestamp suffix pattern |
| G10 | Hardcoded BASE_IRI | In prompts.py | Move to config.yaml |
| G21 | No incremental generation | Re-runs all tables | Skip unchanged (schema + relationships) |
| G22 | No auto-approve | Always manual review | Configurable threshold auto-approve |

### 12.2 Major Gaps (v2 Should-Fix)

| ID | Gap | v2 Remediation |
|----|-----|----------------|
| G8 | No error recovery | Retry + interactive prompt on failure |
| G9 | Lumina fragile | Add logging, circuit breaker |
| G12 | Classification guidance hardcoded | Move to config.yaml |
| G23 | Missing profiling not handled | Block generate phase; require profiling |
| G24 | Relation name not validated | Warn if generated name differs from override |

### 12.3 Moderate Gaps (v3 Planned)

| ID | Gap | v3 Plan |
|----|-----|---------|
| G1 | External ontologies | BioPortal API + local OWL catalog |
| G4 | No cardinality detection | Add cardinality_suggestion field |
| G11 | M:N not modeled | Generate reified association classes |
| G14 | Provenance not RDF | Generate PROV-O triples |

### 12.4 Minor Gaps (Backlog)

| ID | Gap | Notes |
|----|-----|-------|
| G3 | Direction heuristics fragile | Use value_overlap more aggressively |
| G13 | Composite key OWL | Model as property sets |
| G15 | No schema evolution tracking | Store diffs in run_meta |
| G17 | Judge doesn't see composite keys | Pass full FK info |
| G18 | Null rate threshold hardcoded | Move to config |

---

## 13. Testing Requirements (New in v2)

### 13.1 Unit Tests (Required)

| Module | Coverage Target | Key Tests |
|--------|-----------------|-----------|
| `sql_ingest.py` | 90% | JOIN parsing, alias resolution, confidence calc |
| `query_gen.py` | 85% | SQL template generation, edge deduplication |
| `run_loader.py` | 85% | CSV parsing, direction correction, merge logic |
| `overrides.py` | 90% | YAML read/write, upsert, is_approved/rejected |
| `prompts.py` | 80% | Prompt construction, classification guidance |
| `owl.py` | 90% | Merge, parse, duplicate detection |
| `traverse.py` | 95% | Topological sort, cycle handling |

### 13.2 Integration Tests (Required)

| Scenario | Description |
|----------|-------------|
| Full pipeline | End-to-end with mock database |
| Phase isolation | Each phase runs independently |
| Override precedence | Approved edges always included |
| Error recovery | LLM failure triggers retry |

### 13.3 Validation Tests (Required)

| Test | Assertion |
|------|-----------|
| OWL parse | All generated Turtle parses successfully |
| IRI uniqueness | No duplicate IRIs across fragments |
| Coverage | Approved edges found in ontology |

### 13.4 Test Data

- `tests/fixtures/worksheets/` — Sample SQL files
- `tests/fixtures/profiling/` — Sample profiling CSVs
- `tests/fixtures/schemas/` — Mock table definitions
- `tests/fixtures/expected/` — Expected outputs

---

## 14. Production Readiness Checklist

### 14.1 Required for v2 Release

| Item | Status | Owner |
|------|--------|-------|
| LLM abstraction layer | Planned | Dev |
| Configuration validation | Exists | — |
| Error handling with retry | Planned | Dev |
| Structured logging (not print) | Planned | Dev |
| Artifact versioning | Planned | Dev |
| Unit test suite | Planned | Dev |
| Integration test suite | Planned | Dev |
| User documentation | Planned | Dev |

### 14.2 Recommended for v2 Release

| Item | Status | Notes |
|------|--------|-------|
| Secrets management | Not started | Use env vars or vault |
| Metrics/instrumentation | Not started | Timing per phase |
| CI/CD pipeline | Not started | GitHub Actions |

### 14.3 Deferred to v3

| Item | Notes |
|------|-------|
| External ontology integration | BioPortal API |
| Ontology viewer UI | RDF graph visualization |
| SPARQL query builder | Validation enhancement |
| Multi-user review | Concurrent analyst support |

---

## 15. Migration Guide (v0 → v2)

### 15.1 Configuration Changes

```yaml
# OLD (v0)
cursor_agent:
  command: "agent"

# NEW (v2)
llm:
  provider: "cursor"
  command: "agent"
  max_retries: 3

# NEW (v2)
ontology:
  base_iri: "http://example.org/rigor#"
```

### 15.2 File Structure Changes

```
# OLD (v0)
data/
  inferred_relationships.csv
  core.owl

# NEW (v2)
data/
  inferred_relationships.csv
  data_quality_report.json      # NEW
  core.owl
  core_2026-03-01T12-34-56Z.owl # NEW: versioned
  validation_report.json        # NEW
runs/                           # NEW
  2026-03-01_001_initial/
    queries/
    results/
    run_meta.json
```

### 15.3 CLI Changes

```bash
# OLD (v0)
python -m rigor.pipeline --phase infer

# NEW (v2)
python -m rigor.pipeline --phase query-gen  # NEW phase
python -m rigor.pipeline --phase infer --run-dir runs/<ts>/
```

---

## 16. CLI Reference

### 16.1 Pipeline Commands

```bash
# Full command syntax
python -m rigor.pipeline \
  --config PATH \
  --phase PHASE \
  [--sql-dir PATH] \
  [--run-dir PATH] \
  [--force-regenerate TABLE] \
  [--non-interactive]
```

### 16.2 Command Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `--config` | Yes | — | Path to config.yaml |
| `--phase` | Yes | — | Phase to run (see below) |
| `--sql-dir` | Phase 0,1 | — | Directory containing SQL worksheets |
| `--run-dir` | Phase 1 | — | Directory containing profiling results |
| `--force-regenerate` | No | — | Force re-generation of specific table(s) |
| `--non-interactive` | No | false | Skip interactive prompts; auto-skip on failure |

### 16.3 Phase Values

| Phase | Description | Required Options |
|-------|-------------|------------------|
| `query-gen` | Generate profiling SQL templates | `--sql-dir` |
| `infer` | Extract relationships, merge profiling | `--sql-dir`, `--run-dir` (if profiling exists) |
| `review` | Launch Streamlit UI | — |
| `generate` | Create OWL from approved relationships | — |
| `validate` | Verify OWL quality and coverage | — |
| `all` | Run infer → generate → validate | `--sql-dir` |

### 16.4 Example Workflows

**First-time setup (with profiling):**
```bash
# Phase 0: Generate profiling SQL
python -m rigor.pipeline --config rigor/config.yaml \
  --sql-dir sql_worksheets/ --phase query-gen

# [Analyst executes SQL in Snowflake, exports CSVs to runs/<ts>/results/]

# Phase 1: Infer relationships with profiling
python -m rigor.pipeline --config rigor/config.yaml \
  --sql-dir sql_worksheets/ --run-dir runs/2026-03-01_001/ --phase infer

# Phase 2: Review in UI
streamlit run -m rigor.ui.app

# Phase 3: Generate OWL
python -m rigor.pipeline --config rigor/config.yaml --phase generate

# Phase 4: Validate
python -m rigor.pipeline --config rigor/config.yaml --phase validate
```

**Incremental update (single table):**
```bash
python -m rigor.pipeline --config rigor/config.yaml \
  --phase generate --force-regenerate CUSTOMERS
```

**CI/CD mode (non-interactive):**
```bash
python -m rigor.pipeline --config rigor/config.yaml \
  --phase all --sql-dir sql_worksheets/ --non-interactive
```

### 16.5 Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Configuration error |
| 2 | Phase prerequisite not met (e.g., missing profiling) |
| 3 | Validation failed (coverage < threshold) |
| 4 | LLM generation failed (after retries) |

---

## 17. Validation SPARQL Queries

### 17.1 Coverage Check

Verify that approved edges are represented as object properties in the ontology:

```sparql
# Check if an approved edge exists as an ObjectProperty
# Parameters: ?fromClass, ?toClass (derived from table names)

PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rigor: <http://example.org/rigor#>

SELECT ?property ?domain ?range
WHERE {
  ?property a owl:ObjectProperty .
  ?property rdfs:domain ?domain .
  ?property rdfs:range ?range .
  FILTER(?domain = rigor:Customer && ?range = rigor:Order)
}
```

**Coverage calculation:**
```python
# Pseudocode for coverage validation
approved_edges = load_approved_edges_from_csv()
covered = 0

for edge in approved_edges:
    from_class = table_to_class(edge.from_table)  # CUSTOMERS → Customer
    to_class = table_to_class(edge.to_table)      # ORDERS → Order

    query = f"""
    ASK {{
      ?prop a owl:ObjectProperty .
      ?prop rdfs:domain <{base_iri}{from_class}> .
      ?prop rdfs:range <{base_iri}{to_class}> .
    }}
    """
    if graph.query(query):
        covered += 1

coverage_rate = covered / len(approved_edges)
```

### 17.2 Duplicate IRI Detection

Find classes or properties defined multiple times:

```sparql
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

# Find duplicate class definitions
SELECT ?class (COUNT(?class) AS ?count)
WHERE {
  ?class a owl:Class .
}
GROUP BY ?class
HAVING (COUNT(?class) > 1)
```

### 17.3 Relation Name Validation

Check if override-specified relation names exist in ontology:

```sparql
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rigor: <http://example.org/rigor#>

# Check if expected property name exists
# Parameter: ?expectedName (from override relation field)

ASK {
  rigor:hasCustomer a owl:ObjectProperty .
}
```

### 17.4 Classification Annotation Check

Verify tables have classification annotations:

```sparql
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rigor: <http://example.org/rigor#>

# Find all classes with their classifications
SELECT ?class ?classification
WHERE {
  ?class a owl:Class .
  OPTIONAL { ?class rigor:classification ?classification }
}
ORDER BY ?class
```

### 17.5 Bridge Table Validation

Verify bridge tables have exactly two outgoing object properties:

```sparql
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rigor: <http://example.org/rigor#>

# Find bridge classes and count their outgoing properties
SELECT ?bridgeClass (COUNT(?prop) AS ?outgoingProps)
WHERE {
  ?bridgeClass a owl:Class .
  ?bridgeClass rigor:classification "bridge" .
  ?prop rdfs:domain ?bridgeClass .
  ?prop a owl:ObjectProperty .
}
GROUP BY ?bridgeClass
HAVING (COUNT(?prop) != 2)
```

### 17.6 Validation Report Schema

The validation phase produces `data/validation_report.json`:

```json
{
  "timestamp": "2026-03-01T12:34:56Z",
  "owl_parse": {
    "success": true,
    "triple_count": 1523
  },
  "duplicate_iris": {
    "count": 0,
    "duplicates": []
  },
  "coverage": {
    "approved_edges": 45,
    "covered_edges": 43,
    "coverage_rate": 0.956,
    "missing_edges": [
      {"from": "LEGACY_ORDERS", "to": "CUSTOMERS", "reason": "Table excluded"}
    ]
  },
  "relation_names": {
    "expected": 12,
    "matched": 11,
    "mismatches": [
      {"expected": "placedBy", "actual": "hasCustomer", "edge": "ORDERS→CUSTOMERS"}
    ]
  },
  "classifications": {
    "total_classes": 50,
    "classified": 48,
    "unclassified": ["TMP_EXPORT", "BACKUP_DATA"]
  },
  "gates": {
    "parse": "pass",
    "duplicates": "pass",
    "coverage": "pass",
    "overall": "pass"
  }
}
```

---

## 18. Appendix

### 18.1 Glossary

| Term | Definition |
|------|------------|
| **Edge** | A candidate FK relationship between two tables |
| **Fragment** | Per-table OWL/Turtle delta |
| **Override** | Human decision (approve/reject) stored in YAML |
| **Profiling** | Statistical validation of edge quality |
| **Provenance** | Audit trail of generation decisions |

### 18.2 Related Documents

- [CONSTITUTION.md](CONSTITUTION.md) — Project principles
- [README.md](README.md) — Quick start guide
- [rigor_v1/](rigor_v1/) — Prototype implementation

### 18.3 References

- RIGOR Paper: Retrieval-augmented Iterative Generation of RDB Ontologies
- OWL 2 DL Specification: https://www.w3.org/TR/owl2-overview/
- RDFLib Documentation: https://rdflib.readthedocs.io/

---

## 19. Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| v0 | 2026-02-27 | Chris | Initial prototype spec |
| v2-draft | 2026-03-01 | Chris | Audit-driven revision; added phases, gaps, testing |
| v2 | 2026-03-01 | Chris | Clarified: Cursor-only LLM, timestamp versioning, async profiling, configurable auto-approve, incremental generation, OWL restrictions, classification patterns, IRI naming |
| v2.1 | 2026-03-01 | Chris | Added: CLI reference (§16), SPARQL validation queries (§17), exit codes, validation report schema |
