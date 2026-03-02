# RIGOR-SF v2 Implementation Checklist

**Created:** 2026-03-01
**Purpose:** Track implementation progress against SPEC_V2.md requirements

---

## How to Use

- Check off items as they are completed: `[x]`
- Mark in-progress items: `[~]`
- Mark blocked items: `[-]`
- Add completion date in Notes column

---

## 1. Configuration (SPEC §9)

### 1.1 Config Classes
| Requirement | Status | Test | Notes |
|-------------|--------|------|-------|
| `LLMConfig` with provider, model, max_retries | `[x]` | `[x]` | 2026-03-01 |
| `LLMConfig.interactive_on_failure` field | `[x]` | `[x]` | 2026-03-01 |
| `ReviewConfig` with auto_approve_threshold | `[x]` | `[x]` | 2026-03-01 |
| `ReviewConfig` with auto_approve_confidence | `[x]` | `[x]` | 2026-03-01 |
| `ReviewConfig.require_human_review` field | `[x]` | `[x]` | 2026-03-01 |
| `OntologyConfig` with base_iri | `[x]` | `[x]` | 2026-03-01 |
| `OntologyConfig` with format (xml/turtle/n3) | `[x]` | `[x]` | 2026-03-01 |
| `OntologyConfig` with naming convention | `[x]` | `[x]` | 2026-03-01 |
| `ProfilingConfig` with sample_limit | `[x]` | `[x]` | 2026-03-01 |
| `ProfilingConfig` with match_rate_threshold | `[x]` | `[x]` | 2026-03-01 |
| `ProfilingConfig` with null_rate_warning | `[x]` | `[x]` | 2026-03-01 |
| `ProfilingConfig` with frequency_boost_* | `[x]` | `[x]` | 2026-03-01 |
| `ValidationConfig` with coverage_warn_threshold | `[x]` | `[x]` | 2026-03-01 |
| `ValidationConfig` with coverage_pass_threshold | `[x]` | `[x]` | 2026-03-01 |
| `ValidationConfig` with allow_duplicate_iris | `[x]` | `[x]` | 2026-03-01 |
| `PathsConfig.runs_dir` field | `[x]` | `[x]` | 2026-03-01 |
| `LuminaConfig.timeout_seconds` field | `[x]` | `[x]` | 2026-03-01 |
| `LuminaConfig.retry_count` field | `[x]` | `[x]` | 2026-03-01 |

### 1.2 Config Files
| Requirement | Status | Notes |
|-------------|--------|-------|
| `config.example.yaml` updated with all fields | `[x]` | 2026-03-01 |
| Inline comments in example config | `[x]` | 2026-03-01 |
| Migration notes for cursor_agent → llm | `[x]` | 2026-03-01 |

---

## 2. CLI Interface (SPEC §16)

### 2.1 Arguments
| Argument | Status | Test | Notes |
|----------|--------|------|-------|
| `--config PATH` (required) | `[x]` | `[ ]` | Exists in v1 |
| `--phase PHASE` (required) | `[x]` | `[ ]` | Exists in v1 |
| `--sql-dir PATH` | `[x]` | `[ ]` | Exists in v1 |
| `--run-dir PATH` | `[x]` | `[ ]` | Exists in v1 |
| `--force-regenerate TABLE` (repeatable) | `[x]` | `[x]` | 2026-03-01 pipeline.py |
| `--non-interactive` flag | `[x]` | `[x]` | 2026-03-01 pipeline.py |

### 2.2 Phase Values
| Phase | Status | Test | Notes |
|-------|--------|------|-------|
| `query-gen` | `[x]` | `[ ]` | Exists in v1 |
| `infer` | `[x]` | `[ ]` | Exists in v1 |
| `review` | `[x]` | `[ ]` | Exists in v1 |
| `generate` | `[x]` | `[ ]` | Exists in v1 |
| `validate` | `[x]` | `[ ]` | Exists in v1 |
| `all` | `[x]` | `[ ]` | Exists in v1 |

### 2.3 Exit Codes
| Code | Meaning | Status | Test | Notes |
|------|---------|--------|------|-------|
| 0 | Success | `[x]` | `[x]` | 2026-03-01 |
| 1 | Configuration error | `[x]` | `[x]` | 2026-03-01 |
| 2 | Prerequisite not met | `[x]` | `[x]` | 2026-03-01 |
| 3 | Validation failed | `[x]` | `[x]` | 2026-03-01 |
| 4 | LLM generation failed | `[x]` | `[x]` | 2026-03-01 |

---

## 3. Phase 0: Query Generation (SPEC §6)

| Requirement | Status | Test | Notes |
|-------------|--------|------|-------|
| Parse SQL worksheets | `[x]` | `[ ]` | Exists in v1 |
| Extract join edges | `[x]` | `[ ]` | Exists in v1 |
| Build confidence scores | `[x]` | `[ ]` | Exists in v1 |
| Generate `01_profiling_edges.sql` | `[x]` | `[ ]` | Exists in v1 |
| Generate `02_column_profiles.sql` | `[x]` | `[ ]` | Exists in v1 |
| Generate `03_value_overlap.sql` | `[x]` | `[ ]` | Exists in v1 |
| Write `run_meta.json` with worksheets hash | `[x]` | `[ ]` | Exists in v1 |
| Write `README.md` with instructions | `[x]` | `[ ]` | Exists in v1 |
| Timestamped run folder | `[x]` | `[ ]` | Exists in v1 |

---

## 4. Phase 1: Infer (SPEC §6)

| Requirement | Status | Test | Notes |
|-------------|--------|------|-------|
| Ingest SQL joins → raw edges | `[x]` | `[ ]` | Exists in v1 |
| Load profiling CSVs via RunLoader | `[x]` | `[ ]` | Exists in v1 |
| Merge stats (match_rate, pk_unique_rate, fk_null_rate) | `[x]` | `[ ]` | Exists in v1 |
| Apply direction corrections from value_overlap | `[x]` | `[ ]` | Exists in v1 |
| Apply override statuses (approved/rejected) | `[x]` | `[ ]` | Exists in v1 |
| Generate data quality report | `[x]` | `[ ]` | Exists in v1 |
| **Auto-approve edges above thresholds** | `[x]` | `[x]` | 2026-03-01 pipeline.py |
| Mark auto-approved in evidence field | `[x]` | `[x]` | 2026-03-01 pipeline.py |
| Log auto-approved count | `[x]` | `[x]` | 2026-03-01 pipeline.py |

---

## 5. Phase 2: Review (SPEC §6)

| Requirement | Status | Test | Notes |
|-------------|--------|------|-------|
| Streamlit UI launches | `[x]` | `[ ]` | Exists in v1 |
| Relationships tab with filters | `[x]` | `[ ]` | Exists in v1 |
| Cell-level editing | `[x]` | `[ ]` | Exists in v1 |
| Bulk flip direction | `[x]` | `[ ]` | Exists in v1 |
| Bulk write overrides | `[x]` | `[ ]` | Exists in v1 |
| Table classification tab | `[x]` | `[ ]` | Exists in v1 |
| Classification suggestions | `[x]` | `[ ]` | Exists in v1 |
| How to Run tab | `[x]` | `[ ]` | Exists in v1 |
| Auto-approved badge display | `[ ]` | `[ ]` | Optional |

---

## 6. Phase 3: Generate (SPEC §6)

### 6.1 Prerequisites
| Requirement | Status | Test | Notes |
|-------------|--------|------|-------|
| Check profiling exists before generate | `[x]` | `[x]` | 2026-03-01 |
| Raise PrerequisiteError with helpful message | `[x]` | `[x]` | 2026-03-01 |
| Exit code 2 if profiling missing | `[x]` | `[x]` | 2026-03-01 |

### 6.2 Core Generation
| Requirement | Status | Test | Notes |
|-------------|--------|------|-------|
| Load schema (Snowflake or offline) | `[x]` | `[ ]` | Exists in v1 |
| Topological sort tables | `[x]` | `[ ]` | Exists in v1 |
| Load metadata (CSV + Lumina) | `[x]` | `[ ]` | Exists in v1 |
| Load overrides and classifications | `[x]` | `[ ]` | Exists in v1 |
| Build schema context with classification | `[x]` | `[ ]` | Exists in v1 |
| Retrieve core ontology snippets | `[x]` | `[ ]` | Exists in v1 |
| External ontology hints (stub for v3) | `[x]` | `[ ]` | Exists in v1 |
| Call LLM Generator | `[x]` | `[ ]` | Exists in v1 |
| Call LLM Judge | `[x]` | `[ ]` | Exists in v1 |
| Merge fragment into core graph | `[x]` | `[ ]` | Exists in v1 |
| Log provenance | `[x]` | `[ ]` | Exists in v1 |

### 6.3 New v2 Features
| Requirement | Status | Test | Notes |
|-------------|--------|------|-------|
| **Incremental cache check** | `[x]` | `[x]` | 2026-03-01 generation_cache.py |
| Compute schema + relationship hash | `[x]` | `[x]` | 2026-03-01 generation_cache.py compute_fingerprint() |
| Skip unchanged tables | `[x]` | `[x]` | 2026-03-01 pipeline.py cache.is_valid() |
| Honor --force-regenerate flag | `[x]` | `[x]` | 2026-03-01 pipeline.py force_tables set |
| **Timestamp versioned output** | `[x]` | `[x]` | 2026-03-01 versioning.py |
| Write `core_<timestamp>.owl` | `[x]` | `[x]` | 2026-03-01 |
| Create symlink `core.owl` → latest | `[x]` | `[x]` | 2026-03-01 |
| **Validate relation names vs overrides** | `[ ]` | `[ ]` | NEW |
| Warn if generated name differs | `[ ]` | `[ ]` | NEW |

### 6.4 Error Handling
| Requirement | Status | Test | Notes |
|-------------|--------|------|-------|
| Retry LLM calls up to max_retries | `[x]` | `[x]` | 2026-03-01 pipeline.py |
| Exponential backoff | `[x]` | `[x]` | 2026-03-01 llm_provider.py with_retry() |
| Interactive S/R/H prompt on failure | `[x]` | `[x]` | 2026-03-01 prompt_user_recovery() |
| --non-interactive auto-skips | `[x]` | `[x]` | 2026-03-01 pipeline.py |
| Log full context on failure | `[x]` | `[ ]` | 2026-03-01 |
| Exit code 4 on unrecoverable LLM failure | `[x]` | `[x]` | 2026-03-01 |

### 6.5 Prompts
| Requirement | Status | Test | Notes |
|-------------|--------|------|-------|
| Remove hardcoded BASE_IRI | `[x]` | `[x]` | 2026-03-01 prompts.py |
| Pass base_iri to build_gen_prompt() | `[x]` | `[x]` | 2026-03-01 |
| Pass base_iri to build_judge_prompt() | `[x]` | `[x]` | 2026-03-01 |

---

## 7. Phase 4: Validate (SPEC §6, §17)

### 7.1 Validation Checks
| Requirement | Status | Test | Notes |
|-------------|--------|------|-------|
| OWL parse success (RDFLib) | `[x]` | `[x]` | Exists in v1 |
| Duplicate IRI detection | `[x]` | `[x]` | 2026-03-01 SPARQL query |
| **SPARQL-based coverage check** | `[x]` | `[x]` | 2026-03-01 sparql_validation.py |
| Coverage gates (50% warn, 90% pass) | `[x]` | `[x]` | 2026-03-01 |
| Relation name validation | `[x]` | `[x]` | 2026-03-01 check_relation_names() |
| Classification coverage check | `[x]` | `[x]` | 2026-03-01 check_classifications() |
| Bridge table validation (2 props) | `[x]` | `[x]` | 2026-03-01 validate_bridge_tables() |

### 7.2 Validation Report (SPEC §17.6)
| Field | Status | Test | Notes |
|-------|--------|------|-------|
| `timestamp` | `[x]` | `[x]` | 2026-03-01 |
| `owl_parse.success` | `[x]` | `[x]` | 2026-03-01 |
| `owl_parse.triple_count` | `[x]` | `[x]` | 2026-03-01 |
| `duplicate_iris.count` | `[x]` | `[x]` | 2026-03-01 |
| `duplicate_iris.duplicates[]` | `[x]` | `[x]` | 2026-03-01 |
| `coverage.approved_edges` | `[x]` | `[x]` | 2026-03-01 |
| `coverage.covered_edges` | `[x]` | `[x]` | 2026-03-01 |
| `coverage.coverage_rate` | `[x]` | `[x]` | 2026-03-01 |
| `coverage.missing_edges[]` | `[x]` | `[x]` | 2026-03-01 |
| `relation_names.expected` | `[x]` | `[x]` | 2026-03-01 |
| `relation_names.matched` | `[x]` | `[x]` | 2026-03-01 |
| `relation_names.mismatches[]` | `[x]` | `[x]` | 2026-03-01 |
| `classifications.total_classes` | `[x]` | `[x]` | 2026-03-01 |
| `classifications.classified` | `[x]` | `[x]` | 2026-03-01 |
| `classifications.unclassified[]` | `[x]` | `[x]` | 2026-03-01 |
| `gates.parse` | `[x]` | `[x]` | 2026-03-01 |
| `gates.duplicates` | `[x]` | `[x]` | 2026-03-01 |
| `gates.coverage` | `[x]` | `[x]` | 2026-03-01 |
| `gates.overall` | `[x]` | `[x]` | 2026-03-01 |

### 7.3 Exit Behavior
| Requirement | Status | Test | Notes |
|-------------|--------|------|-------|
| Exit code 3 on validation failure | `[x]` | `[x]` | 2026-03-01 |
| Versioned validation_report.json | `[x]` | `[x]` | 2026-03-01 |

---

## 8. LLM Provider (SPEC §9)

| Requirement | Status | Test | Notes |
|-------------|--------|------|-------|
| `LLMProvider` abstract base class | `[x]` | `[x]` | 2026-03-01 llm_provider.py |
| `LLMResponse` dataclass | `[x]` | `[x]` | 2026-03-01 |
| `CursorProvider` implementation | `[x]` | `[x]` | 2026-03-01 |
| `get_provider()` factory function | `[x]` | `[x]` | 2026-03-01 create_provider() |
| Retry decorator with backoff | `[x]` | `[x]` | 2026-03-01 with_retry() decorator |
| Interactive failure prompt | `[x]` | `[x]` | 2026-03-01 prompt_user_recovery() |
| Non-interactive skip mode | `[x]` | `[x]` | 2026-03-01 --non-interactive flag |

---

## 9. Data Contracts (SPEC §7)

### 9.1 Input Files
| File | Status | Notes |
|------|--------|-------|
| `sql_worksheets/*.sql` | `[x]` | Exists |
| `metadata/tables.csv` | `[x]` | Exists |
| `metadata/columns.csv` | `[x]` | Exists |
| `rigor/config.yaml` | `[x]` | Update schema |
| `runs/<ts>/results/*.csv` | `[x]` | Exists |

### 9.2 Output Files
| File | Status | Notes |
|------|--------|-------|
| `runs/<ts>/queries/*.sql` | `[x]` | Exists |
| `runs/<ts>/run_meta.json` | `[x]` | Exists |
| `data/inferred_relationships.csv` | `[x]` | Exists |
| `data/data_quality_report.json` | `[x]` | Exists |
| `golden/overrides.yaml` | `[x]` | Exists |
| `data/fragments/<TABLE>.ttl` | `[x]` | Exists |
| `data/core.owl` (symlink) | `[x]` | 2026-03-01 versioning.py |
| `data/core_<timestamp>.owl` | `[x]` | 2026-03-01 versioning.py |
| `data/provenance.jsonl` | `[x]` | Exists |
| `data/validation_report.json` | `[x]` | 2026-03-01 versioned |

### 9.3 Relationships CSV Schema
| Column | Status | Notes |
|--------|--------|-------|
| from_table | `[x]` | Exists |
| from_column | `[x]` | Exists |
| from_columns | `[x]` | Exists |
| to_table | `[x]` | Exists |
| to_column | `[x]` | Exists |
| to_columns | `[x]` | Exists |
| confidence_sql | `[x]` | Exists |
| frequency | `[x]` | Exists |
| match_rate | `[x]` | Exists |
| pk_unique_rate | `[x]` | Exists |
| fk_null_rate | `[x]` | Exists |
| status | `[x]` | Exists |
| evidence | `[x]` | Exists |
| data_quality_flag | `[x]` | Exists |

### 9.4 Overrides YAML Schema
| Field | Status | Notes |
|-------|--------|-------|
| approve[].from.table | `[x]` | Exists |
| approve[].from.columns | `[x]` | Exists |
| approve[].to.table | `[x]` | Exists |
| approve[].to.columns | `[x]` | Exists |
| approve[].relation | `[x]` | Exists |
| reject[] | `[x]` | Exists |
| table_classification | `[x]` | Exists |

---

## 10. OWL Generation Rules (SPEC §8)

### 10.1 Allowed Constructs
| Construct | Status | Notes |
|-----------|--------|-------|
| owl:Class | `[x]` | Exists |
| owl:ObjectProperty | `[x]` | Exists |
| owl:DatatypeProperty | `[x]` | Exists |
| rdfs:domain / rdfs:range | `[x]` | Exists |
| owl:FunctionalProperty | `[ ]` | Verify |
| owl:cardinality | `[ ]` | Verify |
| rdfs:subClassOf | `[x]` | Exists |

### 10.2 IRI Naming Convention
| Rule | Status | Notes |
|------|--------|-------|
| Classes: PascalCase | `[x]` | 2026-03-01 in prompts.py |
| Properties: camelCase | `[x]` | 2026-03-01 in prompts.py |
| FK relationships: `has` prefix | `[ ]` | Verify in prompts |

### 10.3 Classification Patterns
| Classification | Pattern | Status | Notes |
|----------------|---------|--------|-------|
| fact | Central class, incoming props | `[ ]` | Verify |
| dimension | Outgoing props to facts | `[ ]` | Verify |
| entity | Standalone, may have hierarchy | `[ ]` | Verify |
| bridge | Reified class, 2 object props | `[ ]` | Verify |
| staging | rigor:staging annotation | `[ ]` | Verify |

---

## 11. Trust Hierarchy (SPEC §10)

| Level | Rule | Status | Notes |
|-------|------|--------|-------|
| 1 | Explicit human approval → always included | `[x]` | Exists |
| 2 | match_rate ≥ 0.90 → auto-included | `[ ]` | Verify threshold |
| 3 | confidence_sql ≥ 0.70 → proposed | `[x]` | Exists |
| 4 | LLM suggestions → never auto-promoted | `[x]` | Exists |

---

## 12. Testing (SPEC §13)

### 12.1 Unit Tests
| Module | Target | Status | Coverage |
|--------|--------|--------|----------|
| exit_codes.py | 90% | `[x]` | 100% (13 tests) |
| config.py | 90% | `[x]` | 95%+ (24 tests) |
| llm_provider.py | 85% | `[x]` | 95%+ (25 tests) - includes retry/backoff |
| versioning.py | 90% | `[x]` | 95%+ (22 tests) |
| sparql_validation.py | 85% | `[x]` | 95%+ (61 tests) - includes bridge validation |
| prompts.py | 80% | `[x]` | 95%+ (24 tests) |
| generation_cache.py | 90% | `[x]` | 95%+ (44 tests) - fingerprint, cache, integration |
| logging_config.py | 90% | `[x]` | 95%+ (41 tests) - formatter, phase logger, file logging |
| lumina_mcp.py | 85% | `[x]` | 95%+ (43 tests) - circuit breaker, retry, backoff |
| sql_ingest.py | 90% | `[x]` | 90%+ (46 tests) - 2026-03-01 |
| query_gen.py | 85% | `[x]` | 85%+ (35 tests) - 2026-03-01 |
| run_loader.py | 85% | `[x]` | 85%+ (38 tests) - 2026-03-01 |
| overrides.py | 90% | `[x]` | 90%+ (47 tests) - 2026-03-01 |
| owl.py | 90% | `[x]` | 90%+ (19 tests) - 2026-03-01 |
| traverse.py | 95% | `[x]` | 95%+ (24 tests) - 2026-03-01 |

### 12.2 Integration Tests
| Scenario | Status | Notes |
|----------|--------|-------|
| Full pipeline end-to-end | `[x]` | 2026-03-02 - test_pipeline_phases.py (16 tests) |
| Phase isolation | `[x]` | 2026-03-02 - test_pipeline_phases.py |
| Override precedence | `[x]` | 2026-03-02 - test_override_precedence.py (15 tests) |
| Error recovery | `[x]` | 2026-03-02 - test_error_recovery.py (17 tests) |
| Incremental generation | `[x]` | 2026-03-02 - test_incremental.py (15 tests) |

### 12.3 Test Fixtures
| Fixture | Status | Notes |
|---------|--------|-------|
| tests/fixtures/worksheets/ | `[x]` | 2026-03-01 - SQL samples |
| tests/fixtures/profiling/ | `[x]` | 2026-03-01 - CSV samples |
| tests/fixtures/schemas/ | `[x]` | 2026-03-01 - overrides.yaml |
| tests/fixtures/expected/ | `[x]` | 2026-03-01 - created (empty) |
| tests/conftest.py | `[x]` | 2026-03-01 - shared fixtures |

---

## 13. Production Readiness (SPEC §14)

### 13.1 Required for v2
| Item | Status | Notes |
|------|--------|-------|
| LLM abstraction layer | `[x]` | 2026-03-01 llm_provider.py |
| Configuration validation | `[x]` | Exists + enhanced |
| Error handling with retry | `[x]` | 2026-03-01 pipeline.py |
| Structured logging | `[x]` | 2026-03-01 logging_config.py |
| Artifact versioning | `[x]` | 2026-03-01 versioning.py |
| Unit test suite | `[x]` | 560 tests passing (505 unit + 55 integration) |
| Integration test suite | `[x]` | 2026-03-02 - 55 tests in 4 test modules |
| User documentation | `[x]` | 2026-03-02 - README.md + MIGRATION.md |

### 13.2 Recommended
| Item | Status | Notes |
|------|--------|-------|
| Secrets management (env vars) | `[ ]` | |
| Metrics/instrumentation | `[ ]` | |
| CI/CD pipeline | `[ ]` | |
| Lumina circuit breaker | `[x]` | 2026-03-01 lumina_mcp.py |

---

## 14. Documentation

| Document | Status | Notes |
|----------|--------|-------|
| README.md updated | `[x]` | 2026-03-02 - Full rewrite with phases, CLI, troubleshooting |
| config.example.yaml updated | `[x]` | 2026-03-01 |
| SPEC_V2.md finalized | `[x]` | |
| CONSTITUTION.md aligned | `[x]` | |
| IMPLEMENTATION_PLAN.md | `[x]` | |
| TASKS.md | `[x]` | |
| Migration guide (v0→v2) | `[x]` | 2026-03-02 - MIGRATION.md created |

---

## 15. Final Verification

Run these commands to verify implementation:

```bash
# 1. Config loads correctly
python -c "from rigor_v1.config import load_config; load_config('rigor/config.yaml')"

# 2. Query-gen phase works
python -m rigor.pipeline --config rigor/config.yaml --sql-dir sql_worksheets/ --phase query-gen

# 3. Infer phase works (with profiling)
python -m rigor.pipeline --config rigor/config.yaml --sql-dir sql_worksheets/ --run-dir runs/<ts>/ --phase infer

# 4. Generate fails without profiling (exit code 2)
python -m rigor.pipeline --config rigor/config.yaml --phase generate
echo "Exit code: $?"  # Should be 2

# 5. Generate works with profiling
python -m rigor.pipeline --config rigor/config.yaml --phase generate

# 6. Validate produces correct report
python -m rigor.pipeline --config rigor/config.yaml --phase validate

# 7. Check versioned artifacts
ls -la data/core*.owl
ls -la data/validation_report*.json

# 8. Run tests
pytest tests/ -v --cov=rigor_v1 --cov-report=term-missing

# 9. Check coverage meets targets
pytest tests/ --cov=rigor_v1 --cov-fail-under=85
```

---

## Sign-Off

| Milestone | Date | Approver |
|-----------|------|----------|
| P0 Features Complete | ____-__-__ | ________ |
| P1 Features Complete | ____-__-__ | ________ |
| Tests Passing | ____-__-__ | ________ |
| Documentation Complete | ____-__-__ | ________ |
| Ready for Release | ____-__-__ | ________ |
