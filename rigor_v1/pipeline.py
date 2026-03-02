"""RIGOR-SF Pipeline Orchestrator.

Main orchestrator for the RIGOR-SF ontology generation pipeline.
Per SPEC_V2.md, phases are:

  Phase 0 (query-gen)  — generate Snowflake profiling SQL from SQL worksheets
  Phase 1 (infer)      — ingest SQL worksheets + merge profiling results
  Phase 2 (review)     — launch Streamlit review UI
  Phase 3 (generate)   — RIGOR loop: generate OWL fragments per table
  Phase 4 (validate)   — OWL parse + SPARQL coverage check

Exit codes per SPEC_V2.md §16.5:
  0: Success
  1: Configuration error
  2: Phase prerequisite not met
  3: Validation failed
  4: LLM generation failed
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import warnings
from pathlib import Path

from rdflib import Graph

from .config import load_config
from .logging_config import get_logger, PhaseLogger, setup_logging

# Module logger
logger = get_logger(__name__)
from .exit_codes import (
    ExitCode,
    ConfigError,
    PrerequisiteError,
    ValidationError,
    LLMError,
)
from .llm_provider import create_provider, LLMResponse, with_retry, prompt_user_recovery
from .versioning import (
    create_versioned_artifact,
    RunDirectory,
    compute_content_hash,
)
from .generation_cache import (
    GenerationCache,
    create_cache,
    compute_fingerprint,
)
from .overrides import load_overrides
from .relationships import write_inferred_relationships_csv, read_relationships_csv, write_relationships_csv
from .sql_ingest import ingest_sql_dir, edges_to_inferred_fks
from .metadata.csv_meta import load_table_comments, load_column_comments
from .metadata.lumina_mcp import LuminaMCPClient, LuminaMCPConfig
from .retrieval.schema_docs import schema_context
from .retrieval.core_ontology import load_core, core_snippets
from .retrieval.external_ontologies import external_ontology_candidates
from .prompts import build_gen_prompt, build_judge_prompt
from .owl import merge_fragment
from .query_gen import generate_run
from .run_loader import RunLoader


# ── Schema loading ─────────────────────────────────────────────────────────────

def _load_schema_online(cfg):
    from .db_introspect import introspect_schema
    tables = introspect_schema(cfg.db.url, schema=cfg.db.schema)
    if cfg.db.include_tables:
        tables = [t for t in tables if t.name in set(cfg.db.include_tables)]
    if cfg.db.exclude_tables:
        tables = [t for t in tables if t.name not in set(cfg.db.exclude_tables)]
    return tables


def _load_schema_offline(cfg):
    try:
        from .csv_schema import load_schema_from_csv
        src = getattr(cfg, "source", None)
        offline_dir = getattr(src, "offline_dir", "inputs") if src else "inputs"
        return load_schema_from_csv(offline_dir)
    except (ImportError, FileNotFoundError) as e:
        logger.warning("Offline schema load failed (%s). Using empty table list.", e)
        return []


def _get_source_mode(cfg) -> str:
    src = getattr(cfg, "source", None)
    return getattr(src, "mode", "snowflake") if src else "snowflake"


# ── Metadata helpers ───────────────────────────────────────────────────────────

def _apply_metadata(tables, table_comments, column_comments):
    for t in tables:
        if t.name in table_comments:
            t.comment = table_comments[t.name]
        for c in t.columns:
            key = (t.name, c.name)
            if key in column_comments:
                c.comment = column_comments[key]


def _apply_overrides_to_tables(tables, overrides):
    table_class_map = getattr(overrides, "table_classification", {}) or {}
    for t in tables:
        cls = table_class_map.get(str(t.name).upper()) or table_class_map.get(str(t.name))
        if cls:
            setattr(t, "classification", str(cls))


# ── Phases ────────────────────────────────────────────────────────────────────

def phase_query_gen(cfg, sql_dir: str, run_label=None):
    """Phase 0 — generate profiling SQL run package."""
    log = PhaseLogger("query-gen")
    log.info("Generating Snowflake profiling queries from SQL worksheets...")
    profiling_cfg = getattr(cfg, "profiling", None)
    sample_limit = getattr(profiling_cfg, "sample_limit", 200_000) if profiling_cfg else 200_000
    run_dir = generate_run(
        sql_dir=sql_dir,
        runs_dir="runs",
        run_label=run_label,
        sample_limit=sample_limit,
    )
    log.info("Done. Run folder: %s", run_dir)
    log.info("Next: execute the 3 SQL files in Snowflake,")
    log.info("  export each result CSV to %s/results/", run_dir)
    log.info("  then run: python -m rigor.pipeline --config <cfg> --run-dir %s --phase infer", run_dir)


def phase_infer(cfg, sql_dir, run_dir, relationships_csv):
    """Phase A — ingest SQL worksheets, merge profiling results, write relationships CSV."""
    log = PhaseLogger("infer")
    log.info("Ingesting SQL worksheets...")
    if not sql_dir:
        raise ValueError("--sql-dir is required for phase infer")

    raw_edges = ingest_sql_dir(sql_dir)
    log.info("%d raw join edges found", len(raw_edges))

    write_inferred_relationships_csv(raw_edges, relationships_csv)

    if run_dir:
        log.info("Merging profiling results from: %s", run_dir)
        loader = RunLoader(run_dir)
        log.info(loader.summary())

        raw_df = read_relationships_csv(relationships_csv)
        # Load overrides for status application
        overrides = load_overrides(cfg.paths.overrides_yaml)
        approved = []
        rejected = []
        if hasattr(overrides, "approve") and overrides.approve:
            for e in overrides.approve:
                f = e if isinstance(e, dict) else {}
                approved.append({"from": {"table": f.get("from_table",""), "columns": [f.get("from_column","")]},
                                  "to":   {"table": f.get("to_table",""),   "columns": [f.get("to_column","")]}})
        if hasattr(overrides, "reject") and overrides.reject:
            for e in overrides.reject:
                f = e if isinstance(e, dict) else {}
                rejected.append({"from": {"table": f.get("from_table",""), "columns": [f.get("from_column","")]},
                                   "to":  {"table": f.get("to_table",""),   "columns": [f.get("to_column","")]}})

        merged_df = loader.merge_relationships(raw_df, approved, rejected)

        # Apply auto-approve logic per SPEC_V2.md §9
        # Auto-approve edges that meet both match_rate and confidence thresholds
        if cfg.review.auto_approve_threshold > 0 and cfg.review.auto_approve_confidence > 0:
            import pandas as pd

            # Ensure numeric columns for comparison
            match_rate = pd.to_numeric(merged_df.get("match_rate", pd.Series(dtype=float)), errors="coerce").fillna(0)
            confidence_sql = pd.to_numeric(merged_df.get("confidence_sql", pd.Series(dtype=float)), errors="coerce").fillna(0)

            # Find edges that qualify for auto-approval
            auto_approved_mask = (
                (match_rate >= cfg.review.auto_approve_threshold) &
                (confidence_sql >= cfg.review.auto_approve_confidence) &
                (merged_df["status"] == "proposed")
            )

            auto_approved_count = auto_approved_mask.sum()
            if auto_approved_count > 0:
                merged_df.loc[auto_approved_mask, "status"] = "approved"
                # Append [auto-approved] marker to evidence field
                merged_df.loc[auto_approved_mask, "evidence"] = (
                    merged_df.loc[auto_approved_mask, "evidence"].fillna("").astype(str) + " [auto-approved]"
                ).str.strip()
                log.info("Auto-approved %d edges (match_rate >= %s, confidence >= %s)",
                         auto_approved_count, cfg.review.auto_approve_threshold,
                         cfg.review.auto_approve_confidence)

        write_relationships_csv(merged_df, relationships_csv)

        dqr = loader.data_quality_report()
        dqr_path = Path(relationships_csv).parent / "data_quality_report.json"
        dqr_path.write_text(json.dumps(dqr, indent=2), encoding="utf-8")
        log.info("Data quality report: %s (%d errors, %d warnings)", dqr_path, dqr['errors'], dqr['warnings'])

    log.info("Done → %s", relationships_csv)


def phase_review(cfg):
    """Phase B — launch Streamlit review UI."""
    log = PhaseLogger("review")
    log.info("Launching Streamlit review UI...")
    config_path = getattr(cfg, "_config_path", "rigor/config.yaml")
    try:
        subprocess.run(
            [sys.executable, "-m", "streamlit", "run",
             str(Path(__file__).parent / "ui" / "app.py"),
             "--", "--config", config_path],
            check=False,
        )
    except FileNotFoundError:
        log.error("streamlit not found. Install with: pip install streamlit")
        sys.exit(1)


def phase_generate(cfg, force_regenerate: list[str] | None = None):
    """Phase 3 — RIGOR loop: LLM generates OWL fragments per table.

    Uses LLM provider abstraction per SPEC_V2.md §9.
    Implements retry logic with exponential backoff.
    Creates versioned artifacts per SPEC_V2.md §6.

    Args:
        cfg: Application configuration
        force_regenerate: Optional list of table names to force regenerate
    """
    log = PhaseLogger("generate")
    log.info("Starting RIGOR ontology generation loop...")
    force_tables = set(t.upper() for t in (force_regenerate or []))

    # Check prerequisite: profiling data should exist
    rel_csv = cfg.paths.inferred_relationships_csv
    if not Path(rel_csv).exists():
        raise PrerequisiteError(
            "Inferred relationships CSV not found",
            details=f"Expected: {rel_csv}\nRun phase 'infer' first.",
        )

    source_mode = _get_source_mode(cfg)
    from .traverse import topo_sort_tables

    if source_mode == "snowflake":
        tables = _load_schema_online(cfg)
    else:
        tables = _load_schema_offline(cfg)
    tables = topo_sort_tables(tables)

    # Inject SQL-inferred FKs from relationships CSV
    import pandas as pd
    from .db_introspect import ForeignKeyInfo
    rel_df = pd.read_csv(rel_csv)

    # Use configurable auto-approve threshold
    match_threshold = cfg.review.auto_approve_threshold

    # Include approved OR high-confidence proposed
    include = rel_df[
        (rel_df["status"] == "approved") |
        (
            (rel_df["status"] == "proposed") &
            (pd.to_numeric(rel_df.get("match_rate", pd.Series(dtype=float)), errors="coerce").fillna(0) >= match_threshold)
        )
    ]
    by_name = {t.name.upper(): t for t in tables}
    for _, row in include.iterrows():
        t = by_name.get(str(row["from_table"]).upper())
        if t:
            t.foreign_keys.append(ForeignKeyInfo(
                constrained_columns=[str(row["from_column"])],
                referred_table=str(row["to_table"]),
                referred_columns=[str(row["to_column"])],
                confidence=float(row.get("confidence_sql", 0.7)),
                evidence=str(row.get("evidence", "")),
            ))

    # Metadata enrichment
    t_comments = load_table_comments(cfg.metadata.tables_csv)
    c_comments = load_column_comments(cfg.metadata.columns_csv)
    if cfg.metadata.lumina.enabled:
        client = LuminaMCPClient(LuminaMCPConfig(
            base_url=cfg.metadata.lumina.base_url,
            bearer_token=cfg.metadata.lumina.bearer_token,
            chat_path=cfg.metadata.lumina.chat_path,
            extra_headers=cfg.metadata.lumina.extra_headers,
            strict_json=cfg.metadata.lumina.strict_json,
        ))
        lum_t, lum_c = client.fetch_metadata([t.name for t in tables])
        t_comments.update(lum_t)
        c_comments.update(lum_c)
    _apply_metadata(tables, t_comments, c_comments)

    # Apply overrides classification
    overrides = load_overrides(cfg.paths.overrides_yaml)
    _apply_overrides_to_tables(tables, overrides)

    # Core ontology
    core_in = Path(cfg.paths.core_in)
    core_in.parent.mkdir(parents=True, exist_ok=True)
    core = load_core(str(core_in))
    Path(cfg.paths.fragments_dir).mkdir(parents=True, exist_ok=True)
    Path(cfg.paths.provenance_jsonl).parent.mkdir(parents=True, exist_ok=True)

    # Create LLM provider per SPEC_V2.md §9
    llm_provider = create_provider(cfg.llm)
    max_retries = cfg.llm.max_retries
    interactive_on_failure = cfg.llm.interactive_on_failure
    base_iri = cfg.ontology.base_iri

    # Load generation cache for incremental generation per SPEC_V2.md §6
    cache = create_cache(cfg.paths.fragments_dir)
    cache_hits = 0
    cache_misses = 0

    # Load expected relation names from overrides for validation during generation
    expected_relation_names = _load_overrides_relation_names(cfg)
    relation_mismatches: list[tuple[str, str, str]] = []  # (edge, expected, actual)

    for t in tables:
        classification = getattr(t, "classification", None)

        # Compute fingerprint for incremental generation
        fingerprint = compute_fingerprint(t, classification)

        # Check cache (skip if unchanged and not forced)
        if t.name.upper() not in force_tables and cache.is_valid(t.name, fingerprint):
            cached_entry = cache.get(t.name)
            if cached_entry:
                cache_hits += 1
                log.debug("  table: %s (cached, skipping)", t.name)
                # Load cached fragment and merge
                frag_path = Path(cfg.paths.fragments_dir) / f"{t.name}.ttl"
                if frag_path.exists():
                    try:
                        core = merge_fragment(core, frag_path.read_text(encoding="utf-8"))
                    except Exception as e:
                        log.warning("  cached merge failed for %s: %s", t.name, e)
                continue

        cache_misses += 1
        log.info("  table: %s", t.name)
        schema_text = schema_context(t)[0].text
        terms = [t.name] + [c.name for c in t.columns]
        core_snips = core_snippets(core, terms)
        ext_snips = external_ontology_candidates(t.name, [c.name for c in t.columns])

        gen_prompt = build_gen_prompt(
            table_name=t.name,
            schema_text=schema_text,
            core_snips=core_snips,
            external_snips=ext_snips,
            table_classification=classification,
            base_iri=base_iri,
        )

        # LLM generation with exponential backoff retry logic
        gen_out = None
        attempt = 0
        while attempt < max_retries:
            attempt += 1
            response = llm_provider.generate(gen_prompt)
            if response.success:
                gen_out = response.content.strip()
                break

            # Calculate exponential backoff delay
            delay = min(1.0 * (2.0 ** (attempt - 1)), 60.0)
            log.warning("  LLM attempt %d/%d failed: %s", attempt, max_retries, response.error)

            if attempt < max_retries:
                log.info("  Retrying in %.1fs...", delay)
                import time
                time.sleep(delay)
            else:
                # All retries exhausted
                if interactive_on_failure:
                    choice = prompt_user_recovery(response.error or "Unknown error", t.name)
                    if choice == "skip":
                        break  # gen_out remains None
                    elif choice == "retry":
                        attempt = 0  # Reset retry counter
                        continue
                    elif choice == "halt":
                        raise LLMError(
                            f"LLM generation halted by user for table {t.name}",
                            table=t.name,
                            attempt=attempt,
                            details=response.error,
                        )
                    else:
                        # User provided TTL content
                        gen_out = choice
                else:
                    # Non-interactive mode: auto-skip
                    log.info("  Auto-skipping %s (non-interactive mode)", t.name)
                    break

        if gen_out is None:
            continue

        try:
            header_line, ttl = gen_out.split("\n", 1)
            header = json.loads(header_line)
        except (ValueError, json.JSONDecodeError) as e:
            log.warning("  parse error for %s: %s", t.name, e)
            header = {"table": t.name, "created_entities": {}, "assumptions": [f"parse_error: {e}"]}
            ttl = gen_out

        judge_prompt = build_judge_prompt(
            schema_text=schema_text,
            candidate_ttl=ttl,
            core_snips=core_snips,
        )

        # Judge with exponential backoff retry logic
        ttl_fixed = ttl  # fallback
        judge_attempt = 0
        while judge_attempt < max_retries:
            judge_attempt += 1
            response = llm_provider.generate(judge_prompt)
            if response.success:
                ttl_fixed = response.content.strip()
                break

            delay = min(1.0 * (2.0 ** (judge_attempt - 1)), 60.0)
            log.warning("  Judge attempt %d/%d failed", judge_attempt, max_retries)
            if judge_attempt < max_retries:
                log.info("  Retrying judge in %.1fs...", delay)
                import time
                time.sleep(delay)

        frag_path = Path(cfg.paths.fragments_dir) / f"{t.name}.ttl"
        frag_path.write_text(ttl_fixed, encoding="utf-8")

        # Validate relation names against overrides per SPEC_V2.md §6
        if expected_relation_names:
            mismatches = _validate_relation_names(
                ttl_fixed, t.name, expected_relation_names, base_iri, log
            )
            relation_mismatches.extend(mismatches)

        try:
            core = merge_fragment(core, ttl_fixed)
        except Exception as e:
            log.warning("  merge failed for %s: %s", t.name, e)

        # Update cache with generated fragment
        cache.put(
            table_name=t.name,
            fingerprint=fingerprint,
            ttl_content=ttl_fixed,
            header=header,
            llm_model=cfg.llm.model,
        )

        with open(cfg.paths.provenance_jsonl, "a", encoding="utf-8") as f:
            f.write(json.dumps({**header, "table": t.name, "classification": classification}) + "\n")

    # Save generation cache
    cache.save()
    log.info("Cache stats: %d hits, %d misses", cache_hits, cache_misses)

    # Report relation name mismatches summary
    if relation_mismatches:
        log.warning("Relation name mismatches found: %d", len(relation_mismatches))
        for edge, expected, actual in relation_mismatches:
            log.warning("  %s: expected '%s', got '%s'", edge, expected, actual)

    # Serialize with versioning per SPEC_V2.md §6
    out_path = Path(cfg.paths.core_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Serialize to string first for versioning
    owl_content = core.serialize(format=cfg.ontology.format)
    if isinstance(owl_content, bytes):
        owl_content = owl_content.decode("utf-8")

    # Create versioned artifact with symlink
    artifact = create_versioned_artifact(
        content=owl_content,
        base_path=str(out_path),
        create_symlink=True,
    )
    log.info("Core OWL written → %s", artifact.path)
    log.info("Symlink updated → %s", out_path)


def _load_approved_edges(cfg) -> list[tuple[str, str]]:
    """Load approved edges from relationships CSV.

    Returns:
        List of (from_table, to_table) tuples for approved edges
    """
    rel_path = Path(cfg.paths.inferred_relationships_csv)
    if not rel_path.exists():
        return []

    df = read_relationships_csv(str(rel_path))
    approved = df[df["status"] == "approved"]
    return [(row["from_table"], row["to_table"]) for _, row in approved.iterrows()]


def _load_overrides_relation_names(cfg) -> dict[tuple[str, str], str]:
    """Load relation name overrides from overrides.yaml.

    Returns:
        Dict mapping (from_table, to_table) to expected relation name
    """
    overrides_path = cfg.paths.overrides_yaml
    overrides = load_overrides(overrides_path)

    relation_names: dict[tuple[str, str], str] = {}
    for edge in overrides.get("approve", []):
        from_table = edge.get("from", {}).get("table", "")
        to_table = edge.get("to", {}).get("table", "")
        relation = edge.get("relation")
        if relation and from_table and to_table:
            relation_names[(from_table.upper(), to_table.upper())] = relation

    return relation_names


def _extract_relation_names_from_ttl(ttl_content: str, base_iri: str) -> list[tuple[str, str, str]]:
    """Extract relation names (ObjectProperties) from TTL content.

    Args:
        ttl_content: Turtle content to parse
        base_iri: Base IRI for the ontology

    Returns:
        List of (property_name, domain_class, range_class) tuples
    """
    try:
        from rdflib import Graph
        from rdflib.namespace import RDF, RDFS, OWL

        g = Graph()
        g.parse(data=ttl_content, format="turtle")

        relations = []
        for prop in g.subjects(RDF.type, OWL.ObjectProperty):
            prop_name = str(prop).replace(base_iri, "")

            # Get domain and range
            domain = None
            range_ = None
            for d in g.objects(prop, RDFS.domain):
                domain = str(d).replace(base_iri, "")
            for r in g.objects(prop, RDFS.range):
                range_ = str(r).replace(base_iri, "")

            if domain and range_:
                relations.append((prop_name, domain, range_))

        return relations
    except Exception:
        return []


def _validate_relation_names(
    ttl_content: str,
    table_name: str,
    expected_relations: dict[tuple[str, str], str],
    base_iri: str,
    log,
) -> list[tuple[str, str, str]]:
    """Validate generated relation names against expected names from overrides.

    Args:
        ttl_content: Generated Turtle content
        table_name: Current table being processed
        expected_relations: Dict of (from_table, to_table) -> expected relation name
        base_iri: Base IRI for the ontology
        log: Logger instance

    Returns:
        List of (edge, expected_name, actual_name) tuples for mismatches
    """
    if not expected_relations:
        return []

    mismatches = []
    generated_relations = _extract_relation_names_from_ttl(ttl_content, base_iri)

    for prop_name, domain, range_ in generated_relations:
        # Check if this edge has an expected relation name
        key = (domain.upper(), range_.upper())
        expected_name = expected_relations.get(key)

        if expected_name and prop_name.lower() != expected_name.lower():
            edge = f"{domain}→{range_}"
            mismatches.append((edge, expected_name, prop_name))
            log.warning(
                "  Relation name mismatch for %s: expected '%s', got '%s'",
                edge, expected_name, prop_name
            )

    return mismatches


def _load_table_classifications(cfg) -> dict[str, str]:
    """Load table classifications from overrides.yaml.

    Returns:
        Dict mapping table_name to classification
    """
    overrides_path = cfg.paths.overrides_yaml
    overrides = load_overrides(overrides_path)
    return overrides.get("table_classification", {}) or {}


def phase_validate(cfg):
    """Phase 4 — OWL parse + SPARQL coverage check.

    Uses SPARQL validation per SPEC_V2.md §17.
    Produces validation_report.json per SPEC §17.6.
    Returns exit code 3 if validation fails per §16.5.
    """
    from dataclasses import asdict

    log = PhaseLogger("validate")
    log.info("Running validation...")

    core_path = Path(cfg.paths.core_out)
    if not core_path.exists():
        raise PrerequisiteError(
            "Core ontology not found",
            details=f"Expected: {core_path}\nRun phase 'generate' first.",
        )

    # Load context for validation
    approved_edges = _load_approved_edges(cfg)
    relation_names = _load_overrides_relation_names(cfg)
    table_classifications = _load_table_classifications(cfg)

    log.info("Approved edges: %d", len(approved_edges))
    log.info("Relation name overrides: %d", len(relation_names))

    # Build SPEC §17.6 validation report
    try:
        from .sparql_validation import build_validation_report

        report = build_validation_report(
            ontology_path=str(core_path),
            base_iri=cfg.ontology.base_iri,
            config=cfg.validation,
            approved_edges=approved_edges,
            overrides_relation_names=relation_names,
            table_classifications=table_classifications,
        )

        # Print summary
        log.info("OWL parse: %s", 'PASS' if report.owl_parse.success else 'FAIL')
        if report.owl_parse.success:
            log.info("Triple count: %d", report.owl_parse.triple_count)
        else:
            log.error("Parse error: %s", report.owl_parse.error)

        log.info("Duplicate IRIs: %d", report.duplicate_iris.count)
        log.info("Edge coverage: %.1f%% (%d/%d)", report.coverage.coverage_rate * 100,
                 report.coverage.covered_edges, report.coverage.approved_edges)
        log.info("Missing edges: %d", len(report.coverage.missing_edges))
        log.info("Relation names: %d/%d matched", report.relation_names.matched, report.relation_names.expected)
        log.info("Classifications: %d/%d", report.classifications.classified, report.classifications.total_classes)
        log.info("Bridge tables: %d/%d valid", report.bridge_tables.valid_bridge_classes,
                 report.bridge_tables.total_bridge_classes)
        if report.bridge_tables.issues:
            for issue in report.bridge_tables.issues:
                log.warning("  Bridge issue: %s has %d ObjectProperties (expected 2)",
                            issue.class_name, issue.actual_properties)

        # Print gates
        log.info("Gates: parse=%s, duplicates=%s, coverage=%s, overall=%s",
                 report.gates.parse, report.gates.duplicates, report.gates.coverage, report.gates.overall)

        # Convert report to dict for JSON serialization
        report_dict = asdict(report)
        passed = report.gates.overall == "pass"

    except ImportError:
        # Fallback to basic validation without full SPARQL support
        log.warning("Full SPARQL validation unavailable, using basic checks...")
        from datetime import datetime, timezone

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        try:
            g = Graph()
            g.parse(str(core_path), format="xml")
            owl_parse = {"success": True, "triple_count": len(g)}
            log.info("OWL parse: PASS (%d triples)", len(g))
        except Exception as e:
            owl_parse = {"success": False, "triple_count": 0, "error": str(e)}
            log.error("OWL parse: FAIL — %s", e)

        # Basic duplicate check
        iri_counts: dict = {}
        for s in g.subjects():
            k = str(s)
            iri_counts[k] = iri_counts.get(k, 0) + 1
        dupes = [k for k, v in iri_counts.items() if v > 1]
        duplicate_iris = {"count": len(dupes), "duplicates": dupes}

        if dupes and not cfg.validation.allow_duplicate_iris:
            log.warning("Duplicate IRIs: FAIL (%d found)", len(dupes))
        else:
            log.info("Duplicate IRIs: PASS")

        # Build minimal report
        report_dict = {
            "timestamp": timestamp,
            "owl_parse": owl_parse,
            "duplicate_iris": duplicate_iris,
            "coverage": {
                "approved_edges": len(approved_edges),
                "covered_edges": 0,
                "coverage_rate": 0.0,
                "missing_edges": [],
            },
            "relation_names": {"expected": 0, "matched": 0, "mismatches": []},
            "classifications": {"total_classes": 0, "classified": 0, "unclassified": []},
            "gates": {
                "parse": "pass" if owl_parse["success"] else "fail",
                "duplicates": "pass" if len(dupes) == 0 or cfg.validation.allow_duplicate_iris else "fail",
                "coverage": "warn",  # Can't compute without full SPARQL
                "overall": "pass" if owl_parse["success"] and (len(dupes) == 0 or cfg.validation.allow_duplicate_iris) else "fail",
            },
        }
        passed = report_dict["gates"]["overall"] == "pass"

    # Write validation report with versioning
    report_content = json.dumps(report_dict, indent=2)
    report_path = Path(cfg.paths.validation_report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    create_versioned_artifact(
        content=report_content,
        base_path=str(report_path),
        create_symlink=True,
    )
    log.info("Report → %s", report_path)

    if not passed:
        raise ValidationError(
            "Validation failed",
            details=f"Gates: {report_dict['gates']}\nSee report: {report_path}",
        )
    else:
        log.info("All gates PASSED")


# ── Entry point ────────────────────────────────────────────────────────────────

PHASES = ["query-gen", "infer", "review", "generate", "validate", "all"]


def run(
    config_path: str,
    phase: str = "all",
    sql_dir: str | None = None,
    run_dir: str | None = None,
    run_label: str | None = None,
    non_interactive: bool = False,
    force_regenerate: list[str] | None = None,
) -> ExitCode:
    """Run the pipeline with specified phase.

    Args:
        config_path: Path to config.yaml
        phase: Phase to run (query-gen, infer, review, generate, validate, all)
        sql_dir: Path to SQL worksheets directory
        run_dir: Path to run directory (for infer phase)
        run_label: Optional label for run directory
        non_interactive: If True, skip interactive prompts and auto-skip on failure
        force_regenerate: List of table names to force regenerate (bypass cache)

    Returns:
        ExitCode indicating success or failure type
    """
    cfg = load_config(config_path)
    cfg._config_path = config_path  # type: ignore[attr-defined]
    rel_csv = cfg.paths.inferred_relationships_csv

    # Override interactive setting if --non-interactive is passed
    if non_interactive:
        cfg.llm.interactive_on_failure = False

    if phase == "query-gen":
        if not sql_dir:
            raise ConfigError("--sql-dir is required for phase query-gen")
        phase_query_gen(cfg, sql_dir=sql_dir, run_label=run_label)
    elif phase == "infer":
        phase_infer(cfg, sql_dir=sql_dir, run_dir=run_dir, relationships_csv=rel_csv)
    elif phase == "review":
        phase_review(cfg)
    elif phase == "generate":
        phase_generate(cfg, force_regenerate=force_regenerate)
    elif phase == "validate":
        phase_validate(cfg)
    elif phase == "all":
        if sql_dir:
            phase_infer(cfg, sql_dir=sql_dir, run_dir=run_dir, relationships_csv=rel_csv)
        phase_generate(cfg, force_regenerate=force_regenerate)
        phase_validate(cfg)
    else:
        raise ConfigError(f"Unknown phase: {phase!r}. Choose from: {PHASES}")

    return ExitCode.SUCCESS


def main():
    """CLI entry point with proper exit code handling per SPEC_V2.md §16.5."""
    ap = argparse.ArgumentParser(
        description="RIGOR-SF: Snowflake ontology compiler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Phases:
  query-gen  Phase 0 — generate Snowflake profiling SQL (no live DB access needed)
  infer      Phase 1 — ingest SQL worksheets + merge profiling CSVs
  review     Phase 2 — launch Streamlit human review UI
  generate   Phase 3 — run RIGOR OWL generation loop
  validate   Phase 4 — OWL parse + SPARQL coverage check
  all        Run infer + generate + validate (legacy mode)

Exit codes:
  0  Success
  1  Configuration error
  2  Phase prerequisite not met
  3  Validation failed
  4  LLM generation failed

Offline workflow:
  python -m rigor.pipeline --config rigor/config.yaml --phase query-gen --sql-dir sql_worksheets/
  [run queries in Snowflake, drop CSVs in runs/<run_id>/results/]
  python -m rigor.pipeline --config rigor/config.yaml --phase infer --sql-dir sql_worksheets/ --run-dir runs/<run_id>
  python -m rigor.pipeline --config rigor/config.yaml --phase review
  python -m rigor.pipeline --config rigor/config.yaml --phase generate
  python -m rigor.pipeline --config rigor/config.yaml --phase validate
        """,
    )
    ap.add_argument("--config", required=True, help="Path to config.yaml")
    ap.add_argument("--phase", default="all", choices=PHASES, help="Pipeline phase to run")
    ap.add_argument("--sql-dir", default=None, help="Path to SQL worksheets directory")
    ap.add_argument("--run-dir", default=None, help="Path to run directory (for infer phase)")
    ap.add_argument("--run-label", default=None, help="Optional label for run directory")
    ap.add_argument(
        "--non-interactive",
        action="store_true",
        help="Skip interactive prompts; auto-skip on LLM failure",
    )
    ap.add_argument(
        "--force-regenerate",
        action="append",
        metavar="TABLE",
        help="Force regeneration of specific table(s) (can be repeated)",
    )
    args = ap.parse_args()

    from .exit_codes import RigorError

    # Initialize logging (will be configured more fully in run() if needed)
    setup_logging(debug=False)

    try:
        exit_code = run(
            args.config,
            phase=args.phase,
            sql_dir=args.sql_dir,
            run_dir=args.run_dir,
            run_label=args.run_label,
            non_interactive=args.non_interactive,
            force_regenerate=args.force_regenerate,
        )
        sys.exit(exit_code)
    except RigorError as e:
        logger.error("%s", e)
        sys.exit(e.exit_code)
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
