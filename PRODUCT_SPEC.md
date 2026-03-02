# Product Specification — RIGOR-SF (Cursor) Ontology Builder

**Date:** 2026-02-27  
**Owner:** Chris (@westendorf.chris)  
**Status:** Draft v0 (implemented prototype + review UI)

## 1. Background and grounding

This product implements a Snowflake-first adaptation of **RIGOR** (Retrieval‑augmented Iterative Generation of RDB Ontologies): an iterative loop that generates **provenance-tagged delta ontology fragments** per table, refines them, and merges them into a growing OWL ontology. citeturn0search0turn0search2

Snowflake warehouses typically do **not** enforce foreign keys; thus relationship structure must be inferred from **SQL worksheets / query logic** (joins) plus optional statistical validation. This spec adds a local, reviewable workflow and durable overrides (a “golden mapping layer”) to optimize for a **precise world model ontology**.

Cursor is used in “Option 2” mode: the **Cursor CLI Agent** is invoked from Python using JSON output formatting for robust automation. citeturn0search1turn0search13

## 2. Goals

### Primary goal
Produce the most **precise**, **auditable**, and **maintainable** OWL ontology representing the organization’s Snowflake data world model.

### Secondary goals
- Minimize manual ontology engineering by using LLM generation where it is safe.
- Make every structural assertion (relationships, classifications) **traceable to evidence** and **human decisions**.
- Enable re-running the pipeline without losing decisions (“replayability”).

## 3. Non-goals (for v0)
- Full SQL grammar coverage (e.g., every Snowflake syntax edge case) — we use best-effort heuristics.
- Fully automated discovery of business metrics definitions (will be added later via Lumina/LLM summarization).
- External ontology repository integration beyond stubs (BioPortal/local OWL catalog can be added later).

## 4. Users & roles

- **Ontology Builder (Data/Analytics Engineer):** runs inference, profiling, generation.
- **Domain Reviewer (Ops/Finance/Product):** reviews inferred relationships and table classifications; approves/rejects; adds notes.
- **Ontology Consumer (KG/ML/Apps):** consumes `core.owl` + provenance for downstream reasoning/graph workloads.

## 5. System overview

### Inputs
- Snowflake schema (tables, columns, types) via SQLAlchemy.
- SQL worksheets (`.sql`) exported from Snowflake UI / repo (`sql_worksheets/`).
- Optional metadata:
  - CSV (`metadata/tables.csv`, `metadata/columns.csv`)
  - Lumina MCP (HTTP LLM wrapper; requires deterministic JSON output contract)

### Outputs
- `data/inferred_relationships.csv` — review queue with evidence and (optional) profiling stats.
- `golden/overrides.yaml` — durable approvals/rejections + semantic relation names + table classification.
- `data/fragments/<TABLE>.ttl` — per-table delta fragments for debugging.
- `data/core.owl` — merged ontology (core world model).
- `data/provenance.jsonl` — LLM header per table (created entities, assumptions).

## 6. Workflow & phases

### Phase A — Infer (SQL ingestion)
Command:
- `rigor --config config/config.yaml --sql-dir sql_worksheets/ --phase infer`

Behavior:
- Parse `.sql` files and extract join edges:
  - `FROM`/`JOIN` table references + aliases
  - `ON a.col = b.col` equality predicates
- Convert join edges to FK-like candidates with:
  - `confidence_sql` heuristic score
  - `evidence` (file path + ON snippet)
- Write `data/inferred_relationships.csv` for review

**Decision:** We separate inference from generation so humans can intervene before the ontology hardens.

### Phase B — Profile (Snowflake validation) …  
Command:
- `rigor ... --phase profile`

Behavior:
- For each candidate relationship, compute evidence statistics (sampling-limited):
  - `match_rate` (fk values that match pk values)
  - `pk_unique_rate`
  - `fk_null_rate`
- Write stats back to `inferred_relationships.csv`

**Decision:** profiling is not optional “nice-to-have” for trust; it is the primary automated validator.

### Phase C — Review (Local UI)
Command:
- `streamlit run rigor_sf/ui/app.py`

Capabilities:
- Filter relationships by confidence/match_rate/text search
- **Approve / Reject** relationships
- **Flip direction** (creates a new reversed candidate)
- **Composite keys**: edit `from_columns` / `to_columns` (semicolon-separated)
- Write durable decisions to `golden/overrides.yaml`
- **Table classification**: mark tables as `bridge|fact|dimension|entity|staging|...`

**Decision:** The UI is the primary human-in-the-loop mechanism; it produces machine-readable overrides.

### Phase D — Generate (RIGOR loop)
Command:
- `rigor ... --phase generate`

Behavior:
- Load schema + metadata
- Load overrides and apply:
  - skip rejected edges
  - prefer explicitly approved edges
  - include only high-evidence edges (profiling threshold) unless explicitly approved
- For each table:
  - Build context: schema + approved/inferred FKs + core snippets + (optional) table classification
  - Cursor agent generates delta TTL + JSON header
  - Cursor agent judge refines TTL
  - Merge TTL into core graph
  - Persist fragment + provenance

## 7. Data model and file contracts

### inferred_relationships.csv
Columns (v0):
- `from_table`, `from_columns` (semicolon-separated), `to_table`, `to_columns`
- `from_column`, `to_column` (back-compat first column)
- `confidence_sql`
- `match_rate`, `pk_unique_rate`, `fk_null_rate` (optional)
- `status` = proposed | approved | rejected
- `evidence`

### golden/overrides.yaml
Top-level keys:
- `approve`: list of edges
- `reject`: list of edges
- `rename`: … (future)
- `table_classification`: map of TABLE -> class string

Edge item schema:
```yaml
- from: { table: TABLE, columns: [COL1, COL2, ...] }
  to:   { table: TABLE, columns: [COL1, COL2, ...] }
  relation: optionalSemanticName
```

## 8. Rules (Do / Don’t)

### Do
- **Do** keep every relationship assertion tied to evidence:
  - SQL snippet and/or profiling stats and/or explicit override decision.
- **Do** store all durable human decisions in `golden/overrides.yaml`.
- **Do** regenerate from scratch whenever you change prompts; overrides must keep results stable.
- **Do** treat ontology generation as a *compiler*:
  - deterministic inputs → deterministic outputs (as much as possible).

### Don’t
- **Don’t** allow the LLM to invent relationships not present in approved/high-evidence candidates.
- **Don’t** promote low-evidence relationships to OWL restrictions (functional, cardinality) without profiling.
- **Don’t** create *new functions* ad hoc; **enhance existing functions** and extend contracts (add columns/fields) rather than forking logic. (Exceptions: new module when a capability cannot be expressed cleanly by extension.)

## 9. Quality gates (trust)

Automated gates (must pass):
- Turtle parses successfully (rdflib parse)
- Core graph serializes successfully
- No duplicate IRIs for the same semantic entity (…)
- Relationship inclusion policy:
  - keep if approved OR match_rate ≥ 0.90
  - drop if rejected
  - otherwise leave as “proposed” and do not generate object properties

Human gates:
- Review top-N edges by degree (hub tables) first
- Classify top-N tables by edge degree (bridge/fact/dimension)

## 10. Open issues / next steps (scoped with “…”)

1) Robust SQL parsing:
- Add support for `USING(...)`, composite join extraction (`AND`), `WHERE`-style joins, view/dbt lineage, …

2) Lumina MCP:
- Define deterministic JSON contract for metadata and worksheet summaries, …
- Add UI panel for worksheet-level business context extraction, …

3) Cardinality inference:
- Use profiling to estimate 1:1, 1:M, M:N and optionally encode OWL restrictions, …

4) Bridge table modeling:
- If classified as bridge, generate object properties between the two endpoint entities and represent membership semantics, …

## 11. Package audit and repairs (this release)

### Implemented
- Local Streamlit UI with:
  - approve/reject
  - flip direction
  - composite key editing (semicolon lists)
  - table classification editor
- Overrides now support composite keys (`columns: [..]`)
- Schema prompt context now includes `TABLE_CLASSIFICATION` when provided

### Known limitations (still)
- Pipeline uses single-column evidence matching for filtering; composite-edge profiling is not implemented yet (…)
- SQL inference heuristic still misses `USING`, multi-predicate joins, and subquery alias edge cases (…)
