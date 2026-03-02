# RIGOR-SF v2 Task Breakdown

**Created:** 2026-03-01
**Source:** IMPLEMENTATION_PLAN.md, SPEC_V2.md
**Total Effort:** ~60 hours

---

## Task Legend

- **Priority:** P0 (Must Have), P1 (Should Have), P2 (Nice to Have)
- **Status:** `[ ]` Todo, `[~]` In Progress, `[x]` Done, `[-]` Blocked
- **Effort:** Estimated hours

---

## Epic 1: Configuration Refactor

**Total Effort:** 4 hours | **Priority:** P0

### 1.1 Add New Config Classes
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| CFG-01 | Create `LLMConfig` class with provider, model, max_retries, interactive_on_failure | 0.5h | `[ ]` | |
| CFG-02 | Create `ReviewConfig` class with auto_approve_threshold, auto_approve_confidence | 0.5h | `[ ]` | |
| CFG-03 | Create `OntologyConfig` class with base_iri, format, naming | 0.5h | `[ ]` | |
| CFG-04 | Create `ProfilingConfig` class with thresholds | 0.5h | `[ ]` | |
| CFG-05 | Create `ValidationConfig` class with coverage thresholds | 0.5h | `[ ]` | |

### 1.2 Update Existing Config
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| CFG-06 | Update `PathsConfig` with `runs_dir` field | 0.25h | `[ ]` | |
| CFG-07 | Update `LuminaConfig` with `timeout_seconds`, `retry_count` | 0.25h | `[ ]` | |
| CFG-08 | Update `AppConfig` to include all new config sections | 0.5h | `[ ]` | Rename cursor_agent → llm |
| CFG-09 | Update `config.example.yaml` with all new fields | 0.5h | `[ ]` | Add inline comments |

### 1.3 Validation & Testing
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| CFG-10 | Add Pydantic validators for new config fields | 0.5h | `[ ]` | URL format, threshold ranges |
| CFG-11 | Write unit tests for config loading | 0.5h | `[ ]` | Test defaults, overrides |

---

## Epic 2: Exit Codes & Error Handling

**Total Effort:** 6 hours | **Priority:** P0

### 2.1 Exit Code Infrastructure
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| EXIT-01 | Create `exit_codes.py` with `ExitCode` enum (0-4) | 0.5h | `[ ]` | |
| EXIT-02 | Create `PrerequisiteError` exception class | 0.25h | `[ ]` | |
| EXIT-03 | Create `ValidationError` exception class | 0.25h | `[ ]` | |
| EXIT-04 | Create `LLMError` exception class | 0.25h | `[ ]` | |
| EXIT-05 | Create `ConfigError` exception class | 0.25h | `[ ]` | |

### 2.2 Pipeline Integration
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| EXIT-06 | Update `main()` to wrap config loading with try/except | 0.5h | `[ ]` | Return ExitCode.CONFIG_ERROR |
| EXIT-07 | Update `main()` to catch PrerequisiteError → code 2 | 0.5h | `[ ]` | |
| EXIT-08 | Update `main()` to catch ValidationError → code 3 | 0.5h | `[ ]` | |
| EXIT-09 | Update `main()` to catch LLMError → code 4 | 0.5h | `[ ]` | |
| EXIT-10 | Add `sys.exit(main())` to script entry point | 0.25h | `[ ]` | |

### 2.3 Phase-Specific Error Handling
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| EXIT-11 | Update `phase_query_gen()` to raise appropriate exceptions | 0.5h | `[ ]` | |
| EXIT-12 | Update `phase_infer()` to raise appropriate exceptions | 0.5h | `[ ]` | |
| EXIT-13 | Update `phase_generate()` to raise appropriate exceptions | 0.5h | `[ ]` | |
| EXIT-14 | Update `phase_validate()` to raise ValidationError on failure | 0.5h | `[ ]` | |
| EXIT-15 | Write integration tests for exit codes | 1h | `[ ]` | Test each exit code scenario |

---

## Epic 3: LLM Provider Interface

**Total Effort:** 5 hours | **Priority:** P0

### 3.1 Abstract Interface
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| LLM-01 | Create `llm_provider.py` module | 0.25h | `[ ]` | |
| LLM-02 | Create `LLMResponse` dataclass | 0.25h | `[ ]` | content, raw_output, success, error |
| LLM-03 | Create `LLMProvider` abstract base class | 0.5h | `[ ]` | generate(), name() methods |
| LLM-04 | Create `get_provider()` factory function | 0.25h | `[ ]` | Provider registry |

### 3.2 Cursor Implementation
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| LLM-05 | Create `CursorProvider` class | 1h | `[ ]` | Port from cursor_cli.py |
| LLM-06 | Implement `CursorProvider.generate()` | 0.5h | `[ ]` | Subprocess call |
| LLM-07 | Implement `CursorProvider._parse_output()` | 0.5h | `[ ]` | JSON extraction |

### 3.3 Integration
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| LLM-08 | Update `pipeline.py` to use new provider interface | 0.5h | `[ ]` | Replace cursor_cli calls |
| LLM-09 | Deprecate/remove `cursor_cli.py` | 0.25h | `[ ]` | Add deprecation notice or delete |
| LLM-10 | Write unit tests for LLM provider | 1h | `[ ]` | Mock subprocess |

---

## Epic 4: Error Recovery & Retry Logic

**Total Effort:** 3 hours | **Priority:** P0

### 4.1 Retry Infrastructure
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| RETRY-01 | Create `with_retry` decorator | 0.75h | `[ ]` | Exponential backoff |
| RETRY-02 | Create `_prompt_user()` helper for S/R/H | 0.5h | `[ ]` | Interactive input |
| RETRY-03 | Add `--non-interactive` flag handling | 0.25h | `[ ]` | Auto-skip on failure |

### 4.2 Integration
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| RETRY-04 | Wrap `CursorProvider.generate()` with retry | 0.5h | `[ ]` | Use decorator |
| RETRY-05 | Pass interactive flag from args to provider | 0.25h | `[ ]` | |
| RETRY-06 | Log full context on failure | 0.25h | `[ ]` | Prompt, error, attempt # |
| RETRY-07 | Write tests for retry logic | 0.5h | `[ ]` | Mock failures |

---

## Epic 5: Timestamp Versioning

**Total Effort:** 4 hours | **Priority:** P0

### 5.1 Versioning Module
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| VER-01 | Create `versioning.py` module | 0.25h | `[ ]` | |
| VER-02 | Implement `version_artifact()` function | 1h | `[ ]` | Timestamped name + symlink |
| VER-03 | Implement `get_latest_version()` helper | 0.5h | `[ ]` | Resolve symlink |
| VER-04 | Handle Windows symlink fallback | 0.5h | `[ ]` | Copy if symlink fails |

### 5.2 Integration
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| VER-05 | Update `phase_generate()` to version `core.owl` | 0.5h | `[ ]` | |
| VER-06 | Update `phase_validate()` to version `validation_report.json` | 0.5h | `[ ]` | |
| VER-07 | Write tests for versioning | 0.75h | `[ ]` | |

---

## Epic 6: Auto-Approve Logic

**Total Effort:** 3 hours | **Priority:** P0

### 6.1 Implementation
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| AUTO-01 | Add auto-approve logic to `phase_infer()` | 1h | `[ ]` | Check thresholds |
| AUTO-02 | Update evidence field with "[auto-approved]" marker | 0.25h | `[ ]` | |
| AUTO-03 | Log auto-approved count | 0.25h | `[ ]` | |
| AUTO-04 | Add auto-approved badge to UI (optional) | 0.5h | `[ ]` | P2 |

### 6.2 Testing
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| AUTO-05 | Write unit tests for auto-approve | 0.5h | `[ ]` | |
| AUTO-06 | Write integration test for threshold behavior | 0.5h | `[ ]` | |

---

## Epic 7: Profiling Prerequisite Check

**Total Effort:** 2 hours | **Priority:** P0

### 7.1 Implementation
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| PRE-01 | Create `_check_profiling_exists()` function | 0.5h | `[ ]` | Check for match_rate data |
| PRE-02 | Add prerequisite check at `phase_generate()` entry | 0.5h | `[ ]` | |
| PRE-03 | Raise `PrerequisiteError` with helpful message | 0.25h | `[ ]` | Include next steps |

### 7.2 Testing
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| PRE-04 | Write test for missing profiling detection | 0.5h | `[ ]` | |
| PRE-05 | Write test for exit code 2 | 0.25h | `[ ]` | |

---

## Epic 8: SPARQL-Based Validation

**Total Effort:** 5 hours | **Priority:** P0

### 8.1 SPARQL Module
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| SPARQL-01 | Create `sparql_validation.py` module | 0.25h | `[ ]` | |
| SPARQL-02 | Implement `table_to_class()` conversion | 0.5h | `[ ]` | CUSTOMERS → Customer |
| SPARQL-03 | Implement `check_edge_coverage()` with ASK query | 1h | `[ ]` | |
| SPARQL-04 | Implement `compute_coverage()` for full report | 0.75h | `[ ]` | |
| SPARQL-05 | Implement `check_duplicate_iris()` | 0.5h | `[ ]` | |
| SPARQL-06 | Implement `validate_bridge_tables()` | 0.5h | `[ ]` | |

### 8.2 Integration
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| SPARQL-07 | Update `phase_validate()` to use SPARQL coverage | 0.5h | `[ ]` | Replace substring matching |
| SPARQL-08 | Write tests for SPARQL validation | 1h | `[ ]` | |

---

## Epic 9: Validation Report Schema

**Total Effort:** 3 hours | **Priority:** P0

### 9.1 Report Structure
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| REPORT-01 | Create `ValidationReport` dataclass | 0.5h | `[ ]` | Match SPEC_V2 §17.6 |
| REPORT-02 | Implement `_load_approved_edges()` helper | 0.25h | `[ ]` | |
| REPORT-03 | Implement `_check_relation_names()` helper | 0.5h | `[ ]` | |
| REPORT-04 | Implement `_check_classifications()` helper | 0.5h | `[ ]` | |

### 9.2 Integration
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| REPORT-05 | Update `phase_validate()` to build full report | 0.75h | `[ ]` | |
| REPORT-06 | Add gates logic with pass/warn/fail | 0.25h | `[ ]` | |
| REPORT-07 | Write tests for validation report | 0.5h | `[ ]` | |

---

## Epic 10: Prompts Update

**Total Effort:** 1 hour | **Priority:** P0

### 10.1 Implementation
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| PROMPT-01 | Remove `BASE_IRI` constant from `prompts.py` | 0.1h | `[ ]` | |
| PROMPT-02 | Add `base_iri` parameter to `build_gen_prompt()` | 0.2h | `[ ]` | |
| PROMPT-03 | Add `base_iri` parameter to `build_judge_prompt()` | 0.2h | `[ ]` | |
| PROMPT-04 | Update pipeline.py to pass `cfg.ontology.base_iri` | 0.25h | `[ ]` | |
| PROMPT-05 | Write tests for prompt construction | 0.25h | `[ ]` | |

---

## Epic 11: Incremental Generation

**Total Effort:** 6 hours | **Priority:** P1

### 11.1 Cache Infrastructure
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| INCR-01 | Implement `_compute_table_hash()` | 1h | `[ ]` | Schema + relationships |
| INCR-02 | Implement `_load_generation_cache()` | 0.5h | `[ ]` | |
| INCR-03 | Implement `_save_generation_cache()` | 0.5h | `[ ]` | |

### 11.2 CLI Integration
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| INCR-04 | Add `--force-regenerate` argument to argparser | 0.25h | `[ ]` | action=append |
| INCR-05 | Pass force_regenerate to phase_generate() | 0.25h | `[ ]` | |

### 11.3 Phase Integration
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| INCR-06 | Add cache check in phase_generate() loop | 1h | `[ ]` | Skip unchanged tables |
| INCR-07 | Log skipped tables | 0.25h | `[ ]` | |
| INCR-08 | Handle --force-regenerate override | 0.25h | `[ ]` | |

### 11.4 Testing
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| INCR-09 | Write tests for hash computation | 0.5h | `[ ]` | |
| INCR-10 | Write tests for cache skip behavior | 0.75h | `[ ]` | |
| INCR-11 | Write tests for --force-regenerate | 0.5h | `[ ]` | |

---

## Epic 12: CLI Argument Updates

**Total Effort:** 1 hour | **Priority:** P1

### 12.1 Implementation
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| CLI-01 | Add `--force-regenerate` argument | 0.25h | `[ ]` | Covered in INCR-04 |
| CLI-02 | Add `--non-interactive` argument | 0.25h | `[ ]` | |
| CLI-03 | Update argparser help text | 0.25h | `[ ]` | |
| CLI-04 | Update README with new CLI options | 0.25h | `[ ]` | |

---

## Epic 13: Structured Logging

**Total Effort:** 4 hours | **Priority:** P1

### 13.1 Logging Infrastructure
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| LOG-01 | Create `logging_config.py` module | 0.5h | `[ ]` | |
| LOG-02 | Implement `setup_logging()` function | 0.5h | `[ ]` | Console + file handlers |
| LOG-03 | Add log level configuration | 0.25h | `[ ]` | Via config.yaml |

### 13.2 Migration
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| LOG-04 | Replace print() in `pipeline.py` | 0.5h | `[ ]` | |
| LOG-05 | Replace print() in `sql_ingest.py` | 0.25h | `[ ]` | |
| LOG-06 | Replace print() in `query_gen.py` | 0.25h | `[ ]` | |
| LOG-07 | Replace print() in `run_loader.py` | 0.25h | `[ ]` | |
| LOG-08 | Replace print() in `cursor_cli.py` / `llm_provider.py` | 0.25h | `[ ]` | |
| LOG-09 | Replace print() in `ui/app.py` | 0.25h | `[ ]` | |
| LOG-10 | Replace print() in remaining modules | 0.5h | `[ ]` | |

### 13.3 Testing
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| LOG-11 | Write tests for logging setup | 0.5h | `[ ]` | |

---

## Epic 14: Lumina Error Handling

**Total Effort:** 2 hours | **Priority:** P1

### 14.1 Circuit Breaker
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| LUMINA-01 | Create `CircuitBreaker` class | 0.5h | `[ ]` | |
| LUMINA-02 | Add circuit breaker to `LuminaMCPClient` | 0.25h | `[ ]` | |

### 14.2 Retry & Timeout
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| LUMINA-03 | Add retry loop with configurable count | 0.25h | `[ ]` | |
| LUMINA-04 | Add configurable timeout | 0.25h | `[ ]` | |
| LUMINA-05 | Add structured logging | 0.25h | `[ ]` | |

### 14.3 Testing
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| LUMINA-06 | Write tests for circuit breaker | 0.5h | `[ ]` | |

---

## Epic 15: Test Suite

**Total Effort:** 12 hours | **Priority:** P1

### 15.1 Infrastructure
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| TEST-01 | Create `tests/` directory structure | 0.25h | `[ ]` | |
| TEST-02 | Create `tests/conftest.py` with fixtures | 1h | `[ ]` | |
| TEST-03 | Create `tests/fixtures/worksheets/` sample SQL | 0.5h | `[ ]` | |
| TEST-04 | Create `tests/fixtures/profiling/` sample CSVs | 0.5h | `[ ]` | |
| TEST-05 | Create `tests/fixtures/schemas/` mock tables | 0.5h | `[ ]` | |
| TEST-06 | Create `pytest.ini` configuration | 0.25h | `[ ]` | |

### 15.2 Unit Tests
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| TEST-07 | Write `test_sql_ingest.py` (90% coverage) | 1h | `[ ]` | |
| TEST-08 | Write `test_query_gen.py` (85% coverage) | 1h | `[ ]` | |
| TEST-09 | Write `test_run_loader.py` (85% coverage) | 1h | `[ ]` | |
| TEST-10 | Write `test_overrides.py` (90% coverage) | 0.75h | `[ ]` | |
| TEST-11 | Write `test_prompts.py` (80% coverage) | 0.75h | `[ ]` | |
| TEST-12 | Write `test_owl.py` (90% coverage) | 0.75h | `[ ]` | |
| TEST-13 | Write `test_traverse.py` (95% coverage) | 0.5h | `[ ]` | |
| TEST-14 | Write `test_sparql_validation.py` | 1h | `[ ]` | |

### 15.3 Integration Tests
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| TEST-15 | Write `test_pipeline_phases.py` | 1.5h | `[ ]` | Phase isolation |
| TEST-16 | Write `test_incremental.py` | 0.75h | `[ ]` | Cache behavior |
| TEST-17 | Write `test_error_recovery.py` | 0.75h | `[ ]` | Retry, exit codes |

---

## Epic 16: Documentation

**Total Effort:** 4 hours | **Priority:** P2

### 16.1 README Updates
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| DOC-01 | Update Quick Start with new phases | 0.5h | `[ ]` | |
| DOC-02 | Document all CLI arguments | 0.5h | `[ ]` | |
| DOC-03 | Add troubleshooting section | 0.5h | `[ ]` | |
| DOC-04 | Add incremental workflow examples | 0.5h | `[ ]` | |

### 16.2 Config Documentation
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| DOC-05 | Update `config.example.yaml` with inline comments | 0.5h | `[ ]` | |
| DOC-06 | Create migration guide for v0 → v2 config | 0.5h | `[ ]` | |

### 16.3 Code Documentation
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| DOC-07 | Add docstrings to new modules | 0.5h | `[ ]` | |
| DOC-08 | Update module-level docstrings | 0.5h | `[ ]` | |

---

## Epic 17: UI Polish (Optional)

**Total Effort:** 3 hours | **Priority:** P2

### 17.1 Enhancements
| ID | Task | Effort | Status | Notes |
|----|------|--------|--------|-------|
| UI-01 | Add auto-approved badge to relationships tab | 0.5h | `[ ]` | |
| UI-02 | Improve table classification suggestions | 0.5h | `[ ]` | |
| UI-03 | Add keyboard shortcuts | 0.5h | `[ ]` | |
| UI-04 | Add progress indicators | 0.5h | `[ ]` | |
| UI-05 | Add data quality flag column display | 0.5h | `[ ]` | |
| UI-06 | Style improvements | 0.5h | `[ ]` | |

---

## Summary by Priority

### P0 — Must Have (31 hours)
| Epic | Tasks | Effort |
|------|-------|--------|
| Configuration Refactor | 11 | 4h |
| Exit Codes & Error Handling | 15 | 6h |
| LLM Provider Interface | 10 | 5h |
| Error Recovery & Retry | 7 | 3h |
| Timestamp Versioning | 7 | 4h |
| Auto-Approve Logic | 6 | 3h |
| Profiling Prerequisite | 5 | 2h |
| SPARQL Validation | 8 | 5h |
| Validation Report | 7 | 3h |
| Prompts Update | 5 | 1h |

### P1 — Should Have (25 hours)
| Epic | Tasks | Effort |
|------|-------|--------|
| Incremental Generation | 11 | 6h |
| CLI Arguments | 4 | 1h |
| Structured Logging | 11 | 4h |
| Lumina Error Handling | 6 | 2h |
| Test Suite | 17 | 12h |

### P2 — Nice to Have (7 hours)
| Epic | Tasks | Effort |
|------|-------|--------|
| Documentation | 8 | 4h |
| UI Polish | 6 | 3h |

---

## Sprint Planning Suggestion

### Sprint 1 (Week 1) — Foundation
**Goal:** Core infrastructure for v2 compliance

| Epic | Tasks |
|------|-------|
| Configuration Refactor | CFG-01 through CFG-11 |
| Exit Codes | EXIT-01 through EXIT-15 |
| LLM Provider | LLM-01 through LLM-10 |
| Error Recovery | RETRY-01 through RETRY-07 |
| Versioning | VER-01 through VER-07 |

**Deliverables:**
- New config schema working
- Exit codes functional
- LLM provider abstraction complete
- Retry logic working
- Versioned artifacts

---

### Sprint 2 (Week 2) — Features
**Goal:** Auto-approve, validation, incremental

| Epic | Tasks |
|------|-------|
| Auto-Approve | AUTO-01 through AUTO-06 |
| Profiling Check | PRE-01 through PRE-05 |
| SPARQL Validation | SPARQL-01 through SPARQL-08 |
| Validation Report | REPORT-01 through REPORT-07 |
| Prompts Update | PROMPT-01 through PROMPT-05 |
| Incremental | INCR-01 through INCR-11 |

**Deliverables:**
- Auto-approve functional
- SPARQL coverage validation
- Full validation report
- Incremental generation working

---

### Sprint 3 (Week 3) — Quality
**Goal:** Testing, logging, documentation

| Epic | Tasks |
|------|-------|
| Logging | LOG-01 through LOG-11 |
| Lumina | LUMINA-01 through LUMINA-06 |
| Test Suite | TEST-01 through TEST-17 |
| Documentation | DOC-01 through DOC-08 |

**Deliverables:**
- Structured logging throughout
- Lumina circuit breaker
- Full test suite passing
- Documentation updated

---

## Verification Checklist

After all tasks complete:

- [ ] `rigor --phase query-gen` exits 0
- [ ] `rigor --phase generate` exits 2 if no profiling
- [ ] `rigor --phase validate` exits 3 if coverage < 50%
- [ ] `core.owl` is symlink to `core_<timestamp>.owl`
- [ ] Auto-approved edges marked in CSV
- [ ] Incremental run skips unchanged tables
- [ ] `--force-regenerate TABLE` works
- [ ] `--non-interactive` auto-skips on failure
- [ ] `validation_report.json` matches SPEC_V2 §17.6
- [ ] All unit tests pass with coverage targets
- [ ] All integration tests pass
- [ ] README reflects new CLI
- [ ] config.example.yaml has all fields
