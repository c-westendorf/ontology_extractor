# RIGOR-SF v2 Implementation Plan

**Created:** 2026-03-01
**Based on:** SPEC_V2.md, rigor_sf audit
**Estimated Effort:** ~60 hours (~2 weeks at 30h/week)

---

## Executive Summary

This plan upgrades rigor_sf to meet SPEC_V2.md requirements. The v1 prototype provides **~65% reusable code**. Key areas requiring work:

| Category | Effort | Priority |
|----------|--------|----------|
| Config Refactor | 4h | P0 |
| Exit Codes & Error Handling | 6h | P0 |
| LLM Provider Interface | 5h | P0 |
| Timestamp Versioning | 4h | P0 |
| Auto-Approve Logic | 3h | P0 |
| SPARQL Validation | 5h | P0 |
| Incremental Generation | 6h | P1 |
| Test Suite | 12h | P1 |
| Logging & Polish | 8h | P2 |

---

## Phase 1: Foundation (P0 — Must Have)

### 1.1 Configuration Schema Refactor

**File:** `rigor_sf/config.py`
**Effort:** 4 hours

**Current State:**
```python
DBConfig, CursorAgentConfig, PathsConfig, LuminaConfig, MetadataConfig, AppConfig
```

**Changes Required:**

```python
# ADD: New config classes
class LLMConfig(BaseModel):
    provider: str = "cursor"
    model: str = "claude-3.5-sonnet"
    command: str = "agent"
    output_format: str = "json"
    debug: bool = False
    max_retries: int = 3
    interactive_on_failure: bool = True

class ReviewConfig(BaseModel):
    auto_approve_threshold: float = 0.95
    auto_approve_confidence: float = 0.80
    require_human_review: bool = True

class OntologyConfig(BaseModel):
    base_iri: str = "http://example.org/rigor#"
    format: str = "xml"
    naming: str = "standard"

class ProfilingConfig(BaseModel):
    sample_limit: int = 200_000
    match_rate_threshold: float = 0.90
    null_rate_warning: float = 0.20
    frequency_boost_5: float = 0.05
    frequency_boost_10: float = 0.10

class ValidationConfig(BaseModel):
    coverage_warn_threshold: float = 0.50
    coverage_pass_threshold: float = 0.90
    allow_duplicate_iris: bool = False

# UPDATE: AppConfig
class AppConfig(BaseModel):
    db: DBConfig
    llm: LLMConfig = LLMConfig()           # renamed from cursor_agent
    review: ReviewConfig = ReviewConfig()
    paths: PathsConfig = PathsConfig()
    metadata: MetadataConfig = MetadataConfig()
    profiling: ProfilingConfig = ProfilingConfig()
    ontology: OntologyConfig = OntologyConfig()
    validation: ValidationConfig = ValidationConfig()
```

**Tasks:**
- [x] Add LLMConfig class
- [x] Add ReviewConfig class
- [x] Add OntologyConfig class
- [x] Add ProfilingConfig class
- [x] Add ValidationConfig class
- [x] Update AppConfig to include new sections
- [x] Update PathsConfig with `runs_dir`
- [x] Update LuminaConfig with `timeout_seconds`, `retry_count`
- [x] Update config.example.yaml with new fields
- [x] Add migration note for cursor_agent → llm rename

---

### 1.2 Exit Codes

**File:** `rigor_sf/exit_codes.py` (NEW)
**Effort:** 1 hour

```python
from enum import IntEnum

class ExitCode(IntEnum):
    SUCCESS = 0
    CONFIG_ERROR = 1
    PREREQUISITE_NOT_MET = 2
    VALIDATION_FAILED = 3
    LLM_GENERATION_FAILED = 4
```

**File:** `rigor_sf/pipeline.py` (UPDATE)

```python
# Update main() to wrap phases with try/except and return exit codes
def main():
    args = parse_args()
    try:
        cfg = load_config(args.config)
    except Exception as e:
        logger.error(f"Config error: {e}")
        return ExitCode.CONFIG_ERROR

    try:
        run_phase(args.phase, cfg, args)
    except PrerequisiteError as e:
        logger.error(f"Prerequisite not met: {e}")
        return ExitCode.PREREQUISITE_NOT_MET
    except ValidationError as e:
        logger.error(f"Validation failed: {e}")
        return ExitCode.VALIDATION_FAILED
    except LLMError as e:
        logger.error(f"LLM generation failed: {e}")
        return ExitCode.LLM_GENERATION_FAILED

    return ExitCode.SUCCESS

if __name__ == "__main__":
    sys.exit(main())
```

**Tasks:**
- [x] Create exit_codes.py with ExitCode enum
- [x] Create custom exception classes (PrerequisiteError, ValidationError, LLMError)
- [x] Update pipeline.main() to catch exceptions and return codes
- [x] Update all phase functions to raise appropriate exceptions

---

### 1.3 LLM Provider Interface

**File:** `rigor_sf/llm_provider.py` (NEW, refactored from cursor_cli.py)
**Effort:** 5 hours

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional
import subprocess
import json

@dataclass
class LLMResponse:
    content: str
    raw_output: str
    success: bool
    error: Optional[str] = None

class LLMProvider(ABC):
    @abstractmethod
    def generate(self, prompt: str) -> LLMResponse:
        """Generate a response from the LLM."""
        pass

    @abstractmethod
    def name(self) -> str:
        """Return provider name for logging."""
        pass

class CursorProvider(LLMProvider):
    def __init__(self, config: "LLMConfig"):
        self.config = config
        self.command = config.command
        self.output_format = config.output_format
        self.debug = config.debug

    def name(self) -> str:
        return "cursor"

    def generate(self, prompt: str) -> LLMResponse:
        cmd = [self.command, "-p"]
        if self.output_format:
            cmd.extend(["--output-format", self.output_format])
        cmd.append(prompt)

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                return LLMResponse(
                    content="",
                    raw_output=result.stderr,
                    success=False,
                    error=f"Command failed with code {result.returncode}"
                )

            content = self._parse_output(result.stdout)
            return LLMResponse(content=content, raw_output=result.stdout, success=True)

        except subprocess.TimeoutExpired:
            return LLMResponse(content="", raw_output="", success=False, error="Timeout")
        except Exception as e:
            return LLMResponse(content="", raw_output="", success=False, error=str(e))

    def _parse_output(self, stdout: str) -> str:
        # Existing JSON parsing logic from cursor_cli.py
        ...

def get_provider(config: "LLMConfig") -> LLMProvider:
    """Factory function to get LLM provider by name."""
    providers = {
        "cursor": CursorProvider,
    }
    provider_class = providers.get(config.provider)
    if not provider_class:
        raise ValueError(f"Unknown LLM provider: {config.provider}")
    return provider_class(config)
```

**Tasks:**
- [x] Create llm_provider.py with LLMProvider ABC
- [x] Implement CursorProvider class
- [x] Add LLMResponse dataclass
- [x] Add get_provider() factory function
- [x] Add retry logic with exponential backoff
- [x] Add interactive failure handling (S/R/H prompt)
- [x] Update pipeline.py to use new provider interface
- [ ] Delete or deprecate cursor_cli.py

---

### 1.4 Error Recovery & Retry Logic

**File:** `rigor_sf/llm_provider.py` (UPDATE)
**Effort:** 3 hours (included in 1.3)

```python
import time
from functools import wraps

def with_retry(max_retries: int, interactive: bool):
    """Decorator for LLM calls with retry and interactive recovery."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                response = func(*args, **kwargs)
                if response.success:
                    return response
                last_error = response.error
                if attempt < max_retries - 1:
                    wait = 2 ** attempt  # exponential backoff
                    logger.warning(f"Attempt {attempt+1} failed, retrying in {wait}s...")
                    time.sleep(wait)

            # All retries exhausted
            if interactive:
                choice = _prompt_user(f"LLM failed: {last_error}. (S)kip / (R)etry / (H)alt? ")
                if choice == 'r':
                    return wrapper(*args, **kwargs)  # retry
                elif choice == 'h':
                    raise LLMError(last_error)
                # else skip
            return response  # return failed response for skip
        return wrapper
    return decorator

def _prompt_user(message: str) -> str:
    """Prompt user for choice. Returns 's', 'r', or 'h'."""
    while True:
        choice = input(message).lower().strip()
        if choice in ('s', 'r', 'h'):
            return choice
        print("Invalid choice. Enter S, R, or H.")
```

**Tasks:**
- [x] Implement with_retry decorator
- [x] Implement _prompt_user function (prompt_user_recovery)
- [x] Add --non-interactive flag handling (auto-skip)
- [x] Integrate with pipeline.py phase_generate()

---

### 1.5 Timestamp Versioning

**File:** `rigor_sf/versioning.py` (NEW)
**Effort:** 4 hours

```python
from datetime import datetime
from pathlib import Path
import os

def version_artifact(path: Path, content: str) -> Path:
    """Write versioned artifact and update symlink."""
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    stem = path.stem
    suffix = path.suffix
    versioned_name = f"{stem}_{timestamp}{suffix}"
    versioned_path = path.parent / versioned_name

    # Write versioned file
    versioned_path.write_text(content)

    # Update symlink
    if path.is_symlink():
        path.unlink()
    elif path.exists():
        path.unlink()  # remove old file
    path.symlink_to(versioned_name)

    return versioned_path

def get_latest_version(path: Path) -> Optional[Path]:
    """Get the latest versioned artifact."""
    if path.is_symlink():
        return path.resolve()
    return path if path.exists() else None
```

**File:** `rigor_sf/pipeline.py` (UPDATE)

```python
# In phase_generate():
from versioning import version_artifact

# After merging all fragments:
owl_content = core.serialize(format=cfg.ontology.format)
versioned_path = version_artifact(Path(cfg.paths.core_out), owl_content)
logger.info(f"Wrote versioned ontology: {versioned_path}")

# In phase_validate():
report_json = json.dumps(validation_report, indent=2)
versioned_path = version_artifact(
    Path("data/validation_report.json"),
    report_json
)
```

**Tasks:**
- [x] Create versioning.py with version_artifact()
- [x] Add get_latest_version() helper
- [x] Update phase_generate() to use versioning
- [x] Update phase_validate() to use versioning
- [x] Ensure symlinks work cross-platform (or use copy on Windows)

---

### 1.6 Auto-Approve Logic

**File:** `rigor_sf/pipeline.py` (UPDATE phase_infer)
**Effort:** 3 hours

```python
def phase_infer(cfg: AppConfig, args: argparse.Namespace):
    # ... existing code ...

    # After merging profiling stats:
    df = run_loader.merge_relationships(raw_edges)

    # Apply auto-approve
    if cfg.review.auto_approve_threshold and cfg.review.auto_approve_confidence:
        auto_approved = (
            (df['match_rate'] >= cfg.review.auto_approve_threshold) &
            (df['confidence_sql'] >= cfg.review.auto_approve_confidence) &
            (df['status'] == 'proposed')
        )
        df.loc[auto_approved, 'status'] = 'approved'
        df.loc[auto_approved, 'evidence'] = df.loc[auto_approved, 'evidence'] + ' [auto-approved]'
        logger.info(f"Auto-approved {auto_approved.sum()} edges (match_rate >= {cfg.review.auto_approve_threshold}, confidence >= {cfg.review.auto_approve_confidence})")

    write_relationships_csv(df, cfg.paths.inferred_relationships_csv)
```

**Tasks:**
- [x] Add auto-approve logic to phase_infer()
- [x] Update evidence field to indicate auto-approval
- [x] Log auto-approved count
- [ ] Update UI to show auto-approved badge (optional)

---

### 1.7 Profiling Prerequisite Check

**File:** `rigor_sf/pipeline.py` (UPDATE phase_generate)
**Effort:** 2 hours

```python
def _check_profiling_exists(cfg: AppConfig) -> bool:
    """Check if profiling CSVs exist."""
    rel_path = Path(cfg.paths.inferred_relationships_csv)
    if not rel_path.exists():
        return False

    df = read_relationships_csv(str(rel_path))
    # Check if any profiling stats are present
    has_profiling = df['match_rate'].notna().any()
    return has_profiling

def phase_generate(cfg: AppConfig, args: argparse.Namespace):
    # Prerequisite check
    if not _check_profiling_exists(cfg):
        raise PrerequisiteError(
            "Profiling data not found. Please complete:\n"
            "  1. Phase 0: rigor --phase query-gen --sql-dir ...\n"
            "  2. Execute SQL in Snowflake, export CSVs to runs/<ts>/results/\n"
            "  3. Phase 1: rigor --phase infer --run-dir ...\n"
        )

    # ... rest of generate logic ...
```

**Tasks:**
- [x] Add _check_profiling_exists() function
- [x] Add PrerequisiteError exception class
- [x] Update phase_generate() entry point
- [x] Test with missing profiling data

---

### 1.8 SPARQL-Based Coverage Validation

**File:** `rigor_sf/sparql_validation.py` (NEW)
**Effort:** 5 hours

```python
from rdflib import Graph, Namespace, URIRef
from typing import List, Tuple, Dict, Any
from dataclasses import dataclass

OWL = Namespace("http://www.w3.org/2002/07/owl#")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")

@dataclass
class CoverageResult:
    approved_edges: int
    covered_edges: int
    coverage_rate: float
    missing_edges: List[Dict[str, str]]

def table_to_class(table_name: str) -> str:
    """Convert table name to class IRI local name. CUSTOMERS → Customer"""
    # Remove common prefixes/suffixes, singularize, PascalCase
    name = table_name.upper()
    if name.endswith('S') and not name.endswith('SS'):
        name = name[:-1]  # naive singularize
    return name.title().replace('_', '')

def check_edge_coverage(
    graph: Graph,
    from_table: str,
    to_table: str,
    base_iri: str
) -> bool:
    """Check if an ObjectProperty exists with correct domain/range."""
    from_class = URIRef(f"{base_iri}{table_to_class(from_table)}")
    to_class = URIRef(f"{base_iri}{table_to_class(to_table)}")

    query = """
    ASK {
        ?prop a owl:ObjectProperty .
        ?prop rdfs:domain ?from .
        ?prop rdfs:range ?to .
    }
    """
    # Bind variables
    result = graph.query(query, initNs={'owl': OWL, 'rdfs': RDFS},
                         initBindings={'from': from_class, 'to': to_class})
    return bool(result)

def compute_coverage(
    graph: Graph,
    approved_edges: List[Tuple[str, str]],
    base_iri: str
) -> CoverageResult:
    """Compute coverage of approved edges in ontology."""
    covered = 0
    missing = []

    for from_table, to_table in approved_edges:
        if check_edge_coverage(graph, from_table, to_table, base_iri):
            covered += 1
        else:
            missing.append({
                "from": from_table,
                "to": to_table,
                "reason": "ObjectProperty not found"
            })

    return CoverageResult(
        approved_edges=len(approved_edges),
        covered_edges=covered,
        coverage_rate=covered / len(approved_edges) if approved_edges else 1.0,
        missing_edges=missing
    )

def check_duplicate_iris(graph: Graph) -> List[str]:
    """Find duplicate class definitions."""
    query = """
    SELECT ?class (COUNT(?class) AS ?count)
    WHERE {
        ?class a owl:Class .
    }
    GROUP BY ?class
    HAVING (COUNT(?class) > 1)
    """
    results = graph.query(query, initNs={'owl': OWL})
    return [str(row[0]) for row in results]

def validate_bridge_tables(graph: Graph, base_iri: str) -> List[Dict[str, Any]]:
    """Check bridge tables have exactly 2 outgoing ObjectProperties."""
    rigor = Namespace(base_iri)
    query = """
    SELECT ?bridgeClass (COUNT(?prop) AS ?count)
    WHERE {
        ?bridgeClass a owl:Class .
        ?bridgeClass rigor:classification "bridge" .
        ?prop rdfs:domain ?bridgeClass .
        ?prop a owl:ObjectProperty .
    }
    GROUP BY ?bridgeClass
    HAVING (COUNT(?prop) != 2)
    """
    results = graph.query(query, initNs={'owl': OWL, 'rdfs': RDFS, 'rigor': rigor})
    return [{"class": str(row[0]), "property_count": int(row[1])} for row in results]
```

**Tasks:**
- [x] Create sparql_validation.py
- [x] Implement table_to_class() conversion
- [x] Implement check_edge_coverage() with SPARQL ASK
- [x] Implement compute_coverage() for full coverage report
- [x] Implement check_duplicate_iris()
- [x] Implement validate_bridge_tables()
- [x] Integrate with phase_validate()

---

### 1.9 Validation Report Schema

**File:** `rigor_sf/pipeline.py` (UPDATE phase_validate)
**Effort:** 3 hours

```python
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
from datetime import datetime

@dataclass
class ValidationReport:
    timestamp: str
    owl_parse: Dict[str, Any]
    duplicate_iris: Dict[str, Any]
    coverage: Dict[str, Any]
    relation_names: Dict[str, Any]
    classifications: Dict[str, Any]
    gates: Dict[str, str]

def phase_validate(cfg: AppConfig, args: argparse.Namespace):
    core_path = Path(cfg.paths.core_out)

    # 1. Parse OWL
    try:
        graph = Graph()
        graph.parse(str(core_path), format=cfg.ontology.format)
        owl_parse = {"success": True, "triple_count": len(graph)}
    except Exception as e:
        owl_parse = {"success": False, "error": str(e)}
        # Early exit with failure
        raise ValidationError(f"OWL parse failed: {e}")

    # 2. Duplicate IRIs
    duplicates = check_duplicate_iris(graph)
    duplicate_iris = {"count": len(duplicates), "duplicates": duplicates}

    # 3. Coverage
    approved_edges = _load_approved_edges(cfg.paths.inferred_relationships_csv)
    coverage_result = compute_coverage(graph, approved_edges, cfg.ontology.base_iri)
    coverage = asdict(coverage_result)

    # 4. Relation names
    relation_mismatches = _check_relation_names(graph, cfg.paths.overrides_yaml, cfg.ontology.base_iri)
    relation_names = {
        "expected": len(relation_mismatches.get("expected", [])),
        "matched": relation_mismatches.get("matched", 0),
        "mismatches": relation_mismatches.get("mismatches", [])
    }

    # 5. Classifications
    classifications = _check_classifications(graph, cfg.ontology.base_iri)

    # 6. Gates
    gates = {
        "parse": "pass" if owl_parse["success"] else "fail",
        "duplicates": "pass" if duplicate_iris["count"] == 0 else "fail",
        "coverage": "pass" if coverage_result.coverage_rate >= cfg.validation.coverage_pass_threshold else (
            "warn" if coverage_result.coverage_rate >= cfg.validation.coverage_warn_threshold else "fail"
        ),
        "overall": "pass"  # computed below
    }
    gates["overall"] = "pass" if all(v in ("pass", "warn") for v in gates.values()) else "fail"

    report = ValidationReport(
        timestamp=datetime.utcnow().isoformat() + "Z",
        owl_parse=owl_parse,
        duplicate_iris=duplicate_iris,
        coverage=coverage,
        relation_names=relation_names,
        classifications=classifications,
        gates=gates
    )

    # Write report
    report_json = json.dumps(asdict(report), indent=2)
    version_artifact(Path("data/validation_report.json"), report_json)

    # Exit code
    if gates["overall"] == "fail":
        raise ValidationError(f"Validation failed. Coverage: {coverage_result.coverage_rate:.1%}")

    logger.info(f"Validation passed. Coverage: {coverage_result.coverage_rate:.1%}")
```

**Tasks:**
- [x] Create ValidationReport dataclass
- [x] Update phase_validate() to build full report
- [x] Implement _load_approved_edges() helper
- [x] Implement _check_relation_names() helper
- [x] Implement _check_classifications() helper
- [x] Add gates logic with thresholds from config
- [x] Raise ValidationError on failure

---

### 1.10 Update prompts.py for base_iri

**File:** `rigor_sf/prompts.py` (UPDATE)
**Effort:** 1 hour

```python
# Remove hardcoded BASE_IRI
# OLD: BASE_IRI = "http://example.org/rigor#"

def build_gen_prompt(
    table_name: str,
    schema_text: str,
    core_snippets: List[str],
    external_snippets: List[str],
    table_classification: Optional[str] = None,
    base_iri: str = "http://example.org/rigor#"  # NEW parameter
) -> str:
    # Use base_iri in prompt construction
    ...

def build_judge_prompt(
    schema_text: str,
    candidate_turtle: str,
    core_snippets: List[str],
    base_iri: str = "http://example.org/rigor#"  # NEW parameter
) -> str:
    # Use base_iri in prompt construction
    ...
```

**Tasks:**
- [x] Remove BASE_IRI constant
- [x] Add base_iri parameter to build_gen_prompt()
- [x] Add base_iri parameter to build_judge_prompt()
- [x] Update pipeline.py to pass cfg.ontology.base_iri

---

## Phase 2: Enhanced Features (P1 — Should Have)

### 2.1 Incremental Generation

**File:** `rigor_sf/pipeline.py` (UPDATE)
**Effort:** 6 hours

```python
import hashlib

def _compute_table_hash(table: TableInfo, relationships: pd.DataFrame) -> str:
    """Compute hash of table schema + related edges."""
    # Hash columns
    col_data = [(c.name, c.type, c.nullable) for c in table.columns]
    col_hash = hashlib.sha256(str(col_data).encode()).hexdigest()[:16]

    # Hash relationships involving this table
    related = relationships[
        (relationships['from_table'] == table.name) |
        (relationships['to_table'] == table.name)
    ]
    rel_hash = hashlib.sha256(related.to_csv().encode()).hexdigest()[:16]

    return f"{col_hash}_{rel_hash}"

def _load_generation_cache(cache_path: Path) -> Dict[str, str]:
    """Load table hashes from previous run."""
    if cache_path.exists():
        return json.loads(cache_path.read_text())
    return {}

def _save_generation_cache(cache_path: Path, cache: Dict[str, str]):
    """Save table hashes for future runs."""
    cache_path.write_text(json.dumps(cache, indent=2))

def phase_generate(cfg: AppConfig, args: argparse.Namespace):
    # ... prerequisite check ...

    cache_path = Path(cfg.paths.fragments_dir) / ".generation_cache.json"
    cache = _load_generation_cache(cache_path)
    force_tables = set(args.force_regenerate or [])

    for table in topo_sorted_tables:
        table_hash = _compute_table_hash(table, relationships)

        # Check cache
        if table.name not in force_tables and cache.get(table.name) == table_hash:
            logger.info(f"Skipping {table.name} (unchanged)")
            continue

        # Generate fragment
        ...

        # Update cache
        cache[table.name] = table_hash

    _save_generation_cache(cache_path, cache)
```

**Tasks:**
- [x] Implement _compute_table_hash() (as compute_fingerprint in generation_cache.py)
- [x] Implement _load_generation_cache() (as GenerationCache.load in generation_cache.py)
- [x] Implement _save_generation_cache() (as GenerationCache.save in generation_cache.py)
- [x] Add --force-regenerate argument to argparser
- [x] Integrate cache check in phase_generate() loop
- [x] Log skipped tables

---

### 2.2 CLI Argument Updates

**File:** `rigor_sf/pipeline.py` (UPDATE main)
**Effort:** 1 hour

```python
def main():
    parser = argparse.ArgumentParser(description="RIGOR-SF Ontology Pipeline")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--phase", required=True,
                        choices=["query-gen", "infer", "review", "generate", "validate", "all"])
    parser.add_argument("--sql-dir", help="Directory containing SQL worksheets")
    parser.add_argument("--run-dir", help="Directory containing profiling results")
    parser.add_argument("--force-regenerate", action="append", metavar="TABLE",
                        help="Force regeneration of specific table(s)")
    parser.add_argument("--non-interactive", action="store_true",
                        help="Skip interactive prompts; auto-skip on failure")

    args = parser.parse_args()
    # ...
```

**Tasks:**
- [x] Add --force-regenerate argument (action=append)
- [x] Add --non-interactive argument
- [x] Pass args to phase functions
- [x] Update error handlers to check non-interactive flag

---

### 2.3 Structured Logging

**File:** `rigor_sf/logging_config.py` (NEW)
**Effort:** 2 hours

```python
import logging
import sys
from pathlib import Path
from datetime import datetime

def setup_logging(run_dir: Optional[Path] = None, debug: bool = False):
    """Configure structured logging."""
    level = logging.DEBUG if debug else logging.INFO
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(formatter)

    # File handler (if run_dir provided)
    handlers = [console]
    if run_dir:
        log_path = run_dir / f"pipeline_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    # Configure root logger
    logging.basicConfig(level=level, handlers=handlers)

    return logging.getLogger("rigor")
```

**Tasks:**
- [x] Create logging_config.py
- [x] Replace all print() with logger.info/warning/error/debug
- [x] Add file logging to run directory
- [x] Configure log levels via config.yaml

---

### 2.4 Lumina Error Handling

**File:** `rigor_sf/metadata/lumina_mcp.py` (UPDATE)
**Effort:** 2 hours

```python
import logging
from dataclasses import dataclass
from typing import Optional
import requests
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)

@dataclass
class CircuitBreaker:
    failure_count: int = 0
    failure_threshold: int = 3
    is_open: bool = False

    def record_failure(self):
        self.failure_count += 1
        if self.failure_count >= self.failure_threshold:
            self.is_open = True
            logger.warning("Lumina circuit breaker OPEN after %d failures", self.failure_count)

    def record_success(self):
        self.failure_count = 0
        self.is_open = False

class LuminaMCPClient:
    def __init__(self, config: LuminaConfig):
        self.config = config
        self.circuit_breaker = CircuitBreaker()

    def fetch_metadata(self, table_names: List[str]) -> Tuple[Dict, Dict]:
        if self.circuit_breaker.is_open:
            logger.warning("Lumina circuit breaker open, returning empty metadata")
            return {}, {}

        for attempt in range(self.config.retry_count + 1):
            try:
                response = requests.post(
                    f"{self.config.base_url}{self.config.chat_path}",
                    json={"tables": table_names},
                    headers=self._headers(),
                    timeout=self.config.timeout_seconds
                )
                response.raise_for_status()
                self.circuit_breaker.record_success()
                return self._parse_response(response.json())

            except RequestException as e:
                logger.warning(f"Lumina request failed (attempt {attempt+1}): {e}")
                if attempt == self.config.retry_count:
                    self.circuit_breaker.record_failure()
                    return {}, {}

        return {}, {}
```

**Tasks:**
- [x] Add CircuitBreaker class
- [x] Add retry logic with configurable count
- [x] Add configurable timeout
- [x] Add structured logging
- [x] Update config with timeout_seconds, retry_count

---

## Phase 3: Testing (P1 — Required)

### 3.1 Test Structure

**Directory:** `rigor_sf/tests/` (NEW)
**Effort:** 12 hours

```
tests/
├── conftest.py              # Shared fixtures
├── fixtures/
│   ├── worksheets/          # Sample SQL files
│   ├── profiling/           # Sample profiling CSVs
│   ├── schemas/             # Mock table definitions
│   └── expected/            # Expected outputs
├── unit/
│   ├── test_sql_ingest.py
│   ├── test_query_gen.py
│   ├── test_run_loader.py
│   ├── test_overrides.py
│   ├── test_prompts.py
│   ├── test_owl.py
│   ├── test_traverse.py
│   └── test_sparql_validation.py
└── integration/
    ├── test_pipeline_phases.py
    ├── test_incremental.py
    └── test_error_recovery.py
```

**Tasks:**
- [x] Create tests/ directory structure
- [x] Create conftest.py with fixtures
- [x] Create sample SQL worksheets
- [x] Create sample profiling CSVs
- [x] Implement unit tests per module (coverage targets from spec)
- [x] Implement integration tests for phase isolation
- [x] Implement error recovery tests
- [x] Add pytest.ini configuration (pyproject.toml)

---

## Phase 4: Documentation & Polish (P2)

### 4.1 Update README.md

**Effort:** 2 hours

- [x] Update Quick Start with new phases
- [x] Document CLI arguments
- [x] Add troubleshooting section
- [x] Add examples for incremental workflow

### 4.2 Update config.example.yaml

**Effort:** 1 hour

- [x] Add all new config sections
- [x] Add inline comments explaining each field
- [x] Provide sensible defaults

### 4.3 Migration Guide

**Effort:** 1 hour

- [x] Create MIGRATION.md with v0→v2 upgrade instructions
- [x] Document config changes
- [x] Document CLI changes
- [x] Document breaking changes

### 4.4 UI Polish (Optional)

**Effort:** 3 hours

- [ ] Add auto-approved badge to relationships tab
- [ ] Improve table classification suggestions
- [ ] Add keyboard shortcuts
- [ ] Add progress indicators

---

## Implementation Order

```
Week 1 (30h):
├── Day 1-2: Config refactor (1.1) + Exit codes (1.2)
├── Day 3-4: LLM provider interface (1.3) + Error recovery (1.4)
└── Day 5: Versioning (1.5) + Auto-approve (1.6)

Week 2 (30h):
├── Day 1: Profiling check (1.7) + prompts.py update (1.10)
├── Day 2-3: SPARQL validation (1.8) + Validation report (1.9)
├── Day 4: Incremental generation (2.1) + CLI args (2.2)
└── Day 5: Logging (2.3) + Lumina (2.4)

Week 3 (if needed):
├── Testing (3.1)
└── Documentation (4.1-4.3)
```

---

## Verification Checklist

After implementation, verify:

- [ ] `rigor --phase query-gen` exits with code 0
- [ ] `rigor --phase generate` exits with code 2 if no profiling
- [ ] `rigor --phase validate` exits with code 3 if coverage < 50%
- [ ] `core.owl` is symlink to `core_<timestamp>.owl`
- [ ] Auto-approved edges show in relationships CSV with status=approved
- [ ] Incremental run skips unchanged tables
- [ ] `--force-regenerate TABLE` regenerates specific table
- [ ] `--non-interactive` auto-skips on LLM failure
- [ ] `validation_report.json` matches schema in SPEC_V2 §17.6
- [ ] All unit tests pass with coverage targets met

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| LLM provider changes | Abstract interface allows easy swap |
| SPARQL performance | Cache ASK results, limit to approved edges |
| Symlink issues on Windows | Fall back to copy if symlink fails |
| Test flakiness | Mock LLM calls in tests |
| Config migration | Document breaking changes, provide migration script |

---

## Success Criteria

v2 is complete when:

1. All P0 features implemented and tested
2. All P1 features implemented and tested
3. Unit test coverage meets targets (§13.1)
4. Integration tests pass
5. Documentation updated
6. Config migration guide available
7. Exit codes work as specified
8. Validation report matches schema
