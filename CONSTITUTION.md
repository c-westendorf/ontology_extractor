# RIGOR-SF Project Constitution

**Established:** 2026-03-01
**Revised:** 2026-03-01 (aligned with SPEC_V2.md)
**Based on:** SPEC_V2.md

---

## Core Mission

Produce the most **precise**, **auditable**, and **maintainable** OWL ontology representing the organization's Snowflake data world model through a human-in-the-loop iterative workflow.

---

## Foundational Principles

### 1. Evidence-First Assertions
Every structural assertion (relationships, classifications) must be **traceable to evidence**:
- SQL join snippets from worksheets
- Profiling statistics from Snowflake
- Explicit human decisions in overrides

**Never allow the LLM to invent relationships not present in approved or high-evidence candidates.**

### 2. Human-in-the-Loop Authority
- The Streamlit review UI is the primary mechanism for human oversight
- All durable human decisions live in `golden/overrides.yaml`
- Humans can approve, reject, flip direction, edit composite keys, and classify tables
- Human decisions take precedence over automated inference
- Auto-approve thresholds can reduce manual review burden while preserving human override capability

### 3. Deterministic Reproducibility
Treat ontology generation as a **compiler**:
- Deterministic inputs → deterministic outputs (as much as possible)
- Re-running the pipeline with the same inputs and overrides must yield stable results
- Overrides ensure "replayability" across regenerations
- Timestamped artifact versioning enables audit trails

### 4. Separation of Concerns by Phase
Keep pipeline phases distinct to allow human intervention:
- **Phase 0 (Query Gen)**: Generate profiling SQL templates
- **Phase 1 (Infer)**: Extract candidates from SQL, merge profiling stats
- **Phase 2 (Review)**: Human approval/rejection via UI (with optional auto-approve)
- **Phase 3 (Generate)**: Produce OWL only after decisions are made
- **Phase 4 (Validate)**: Verify OWL quality and coverage

---

## Development Guidelines

### Code Evolution
- **Enhance existing functions** rather than creating new ones ad hoc
- Extend contracts (add columns/fields) rather than forking logic
- Only create new modules when a capability cannot be expressed cleanly by extension
- Avoid backwards-compatibility hacks; delete unused code completely

### Quality Gates (Automated)
- Turtle must parse successfully (RDFLib)
- Core graph must serialize successfully
- No duplicate IRIs for the same semantic entity
- Relationship inclusion policy:
  - **Keep** if approved OR match_rate ≥ 0.90
  - **Drop** if rejected
  - **Defer** (do not generate object properties) if proposed with low evidence
- Coverage validation via SPARQL (≥90% of approved edges in OWL)

### Quality Gates (Human)
- Review top-N edges by degree (hub tables) first
- Classify top-N tables by edge degree (bridge/fact/dimension)
- Validate relation names match overrides

---

## Data Contracts

### Input Files
| Path | Purpose |
|------|---------|
| `sql_worksheets/*.sql` | SQL worksheets for join inference |
| `metadata/tables.csv` | Optional table comments |
| `metadata/columns.csv` | Optional column comments |
| `rigor/config.yaml` | Pipeline configuration |
| `runs/<ts>/results/*.csv` | Profiling results from analyst |

### Output Files
| Path | Purpose |
|------|---------|
| `runs/<ts>/queries/*.sql` | Profiling SQL templates |
| `runs/<ts>/run_meta.json` | Run metadata with worksheet hashes |
| `data/inferred_relationships.csv` | Review queue with evidence |
| `data/data_quality_report.json` | Profiling diagnostics |
| `golden/overrides.yaml` | Durable approvals/rejections/classifications |
| `data/fragments/<TABLE>.ttl` | Per-table delta fragments |
| `data/core.owl` | Merged ontology (symlink to latest) |
| `data/core_<timestamp>.owl` | Versioned ontology snapshots |
| `data/provenance.jsonl` | LLM generation metadata per table |
| `data/validation_report.json` | Validation results |

### Override Schema
```yaml
approve:
  - from: { table: TABLE, columns: [COL1, COL2] }
    to:   { table: TABLE, columns: [COL1, COL2] }
    relation: optionalSemanticName

reject:
  - from: { table: TABLE, columns: [COL] }
    to:   { table: TABLE, columns: [COL] }

table_classification:
  TABLE_NAME: bridge|fact|dimension|entity|staging
```

---

## Trust Hierarchy

1. **Explicit human approval** (highest trust)
2. **High-evidence profiling** (match_rate ≥ 0.90)
3. **SQL-inferred candidates** (confidence_sql heuristics)
4. **LLM suggestions** (lowest trust — never auto-promoted)

---

## Anti-Patterns (Don't)

- Don't promote low-evidence relationships to OWL restrictions (functional, cardinality) without profiling
- Don't allow the LLM to invent relationships not grounded in evidence
- Don't fork logic into new functions when existing ones can be extended
- Don't create new modules unless strictly necessary
- Don't over-engineer beyond current requirements
- Don't skip profiling before generation phase
- Don't overwrite artifacts without versioning

---

## User Roles

| Role | Responsibility |
|------|----------------|
| **Ontology Builder** | Runs phases 0-4, configures pipeline |
| **Data Analyst** | Executes profiling SQL in Snowflake, exports CSVs |
| **Domain Reviewer** | Reviews relationships, approves/rejects, classifies tables |
| **Ontology Consumer** | Uses `core.owl` + provenance for downstream reasoning |

---

## Non-Goals (v2 Scope)

- Full SQL grammar coverage (CTEs, complex predicates, schema qualifiers)
- Real-time / streaming ontology updates
- Automated business metrics definitions
- External ontology repository integration (BioPortal planned for v3)
- Multi-user concurrent review
- Very large scale (500+ tables)
- Alternative LLM providers (interface only in v2)

---

## OWL Generation Standards

### IRI Naming Convention
- **Classes**: PascalCase (e.g., `:Customer`, `:OrderItem`)
- **Properties**: camelCase (e.g., `:hasOrder`, `:customerId`)
- **Base IRI**: Configurable via `ontology.base_iri` in config.yaml

### Classification-Driven Patterns
| Classification | OWL Pattern |
|----------------|-------------|
| **fact** | Central class with many incoming properties |
| **dimension** | Class with outgoing properties to facts |
| **entity** | Standalone class, may have sub-hierarchy |
| **bridge** | Reified association class with two object properties |
| **staging** | Annotated with `rigor:staging true` |

---

## Amendment Process

This constitution should be updated when:
- Core workflow phases change
- New data contracts are introduced
- Trust hierarchy rules are modified
- Quality gates are added or removed
- OWL generation standards evolve

All amendments require updating both this document and SPEC_V2.md to maintain alignment.
