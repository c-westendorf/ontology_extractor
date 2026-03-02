from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple
import re

from .logging_config import get_logger
from .sql_parser_ast import JoinEdge, ParserDiagnostics, parse_sql_file_ast, parse_sql_text_ast


logger = get_logger(__name__)

_IDENTIFIER = r"[A-Za-z_][A-Za-z0-9_\$]*"
_FROM_JOIN_RE = re.compile(
    rf"\b(?:from|join)\s+(?P<table>{_IDENTIFIER}(?:\.{_IDENTIFIER}){{0,2}})"
    rf"(?:\s+(?:as\s+)?(?P<alias>{_IDENTIFIER}))?",
    flags=re.IGNORECASE,
)
_ON_EQ_RE = re.compile(
    rf"\bON\b(?P<on>.*?)(?=\b(?:join|where|group\s+by|having|qualify|order\s+by|union|$)\b)",
    flags=re.IGNORECASE | re.DOTALL,
)
_COL_EQ_RE = re.compile(
    rf"(?P<a>{_IDENTIFIER}\.{_IDENTIFIER})\s*=\s*(?P<b>{_IDENTIFIER}\.{_IDENTIFIER})",
    flags=re.IGNORECASE,
)

_LAST_INGEST_DIAGNOSTICS: dict = {}


def _normalize_table(name: str) -> str:
    parts = name.split(".")
    return parts[-1].strip().strip('"').upper()


def _normalize_ident(name: str) -> str:
    return name.strip().strip('"').upper()


def _strip_sql_comments(sql: str) -> str:
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return sql


def _legacy_parse_sql_text(sql: str, evidence: str = "") -> List[JoinEdge]:
    """Legacy regex parser retained as internal fallback for malformed statements."""
    sql = _strip_sql_comments(sql)
    edges: List[JoinEdge] = []

    statements = [s for s in re.split(r";\s*", sql) if s.strip()]
    for stmt in statements:
        alias_map: Dict[str, str] = {}

        for match in _FROM_JOIN_RE.finditer(stmt):
            table = _normalize_table(match.group("table"))
            alias = match.group("alias")
            if alias:
                alias_map[_normalize_ident(alias)] = table

        for on_match in _ON_EQ_RE.finditer(stmt):
            on_block = on_match.group("on")
            for eq in _COL_EQ_RE.finditer(on_block):
                a = eq.group("a")
                b = eq.group("b")
                a_alias, a_col = a.split(".", 1)
                b_alias, b_col = b.split(".", 1)

                a_alias_n = _normalize_ident(a_alias)
                b_alias_n = _normalize_ident(b_alias)

                a_table = alias_map.get(a_alias_n, _normalize_table(a_alias))
                b_table = alias_map.get(b_alias_n, _normalize_table(b_alias))

                a_col_n = _normalize_ident(a_col)
                b_col_n = _normalize_ident(b_col)

                conf = 0.6
                if a_col_n.endswith("_ID") or a_col_n == "ID":
                    conf += 0.1
                if b_col_n.endswith("_ID") or b_col_n == "ID":
                    conf += 0.1
                if a_col_n == "ID" and b_col_n.endswith("_ID"):
                    conf += 0.15
                if b_col_n == "ID" and a_col_n.endswith("_ID"):
                    conf += 0.15
                conf = min(conf, 0.95)

                snippet = on_block.strip().replace("\n", " ")[:240]
                edges.append(
                    JoinEdge(
                        left_table=a_table,
                        left_column=a_col_n,
                        right_table=b_table,
                        right_column=b_col_n,
                        confidence=conf,
                        evidence=f"{evidence} | fallback_regex | ON {snippet}" if evidence else f"fallback_regex | ON {snippet}",
                        parser_dialect="fallback_regex",
                        predicate_type="eq_column_column",
                        confidence_reason="fallback_regex:column_equality",
                        ast_path="fallback_regex",
                        source_query_block="fallback_statement",
                    )
                )

    return edges


def _diag_to_dict(diag: ParserDiagnostics) -> dict:
    return {
        "file": diag.file,
        "statement_count": diag.statement_count,
        "parsed_statement_count": diag.parsed_statement_count,
        "failed_statement_count": diag.failed_statement_count,
        "fallback_statement_count": diag.fallback_statement_count,
        "unresolved_alias_count": diag.unresolved_alias_count,
        "skipped_predicate_count": diag.skipped_predicate_count,
        "predicate_type_counts": dict(diag.predicate_type_counts),
        "errors": list(diag.errors),
    }


def get_last_ingest_diagnostics() -> dict:
    return dict(_LAST_INGEST_DIAGNOSTICS)


def parse_sql_file(path: Path) -> List[JoinEdge]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    return parse_sql_text(text, evidence=str(path))


def parse_sql_text(sql: str, evidence: str = "") -> List[JoinEdge]:
    edges, diagnostics = parse_sql_text_ast(sql, evidence=evidence, dialect_order=["snowflake", ""])

    if not edges and diagnostics.statement_count > 0:
        fallback_edges = _legacy_parse_sql_text(sql, evidence=evidence)
        diagnostics.fallback_statement_count = diagnostics.statement_count
        if fallback_edges:
            diagnostics.errors.append("ast_parse_yielded_no_edges:used_fallback_regex")
            edges = fallback_edges

    _LAST_INGEST_DIAGNOSTICS[evidence or "<inline>"] = _diag_to_dict(diagnostics)
    return edges


def ingest_sql_dir(sql_dir: str) -> List[JoinEdge]:
    p = Path(sql_dir)
    if not p.exists():
        raise FileNotFoundError(f"SQL directory not found: {sql_dir}")

    edges: List[JoinEdge] = []
    file_diags: List[dict] = []

    for file_path in sorted(p.rglob("*.sql")):
        file_edges, diagnostics = parse_sql_file_ast(file_path, dialect_order=["snowflake", ""])

        if not file_edges and diagnostics.statement_count > 0:
            fallback_edges = _legacy_parse_sql_text(file_path.read_text(encoding="utf-8", errors="ignore"), evidence=str(file_path))
            if fallback_edges:
                diagnostics.fallback_statement_count = diagnostics.statement_count
                diagnostics.errors.append("ast_parse_yielded_no_edges:used_fallback_regex")
                file_edges = fallback_edges

        edges.extend(file_edges)
        file_diags.append(_diag_to_dict(diagnostics))

    predicate_totals: Dict[str, int] = {}
    unresolved_alias_total = 0
    fallback_total = 0
    failed_total = 0

    for diag in file_diags:
        unresolved_alias_total += int(diag.get("unresolved_alias_count", 0))
        fallback_total += int(diag.get("fallback_statement_count", 0))
        failed_total += int(diag.get("failed_statement_count", 0))
        for predicate_type, count in (diag.get("predicate_type_counts") or {}).items():
            predicate_totals[predicate_type] = predicate_totals.get(predicate_type, 0) + int(count)

    _LAST_INGEST_DIAGNOSTICS.clear()
    _LAST_INGEST_DIAGNOSTICS.update(
        {
            "sql_dir": str(p),
            "file_count": len(file_diags),
            "edge_count": len(edges),
            "unresolved_alias_count": unresolved_alias_total,
            "fallback_statement_count": fallback_total,
            "failed_statement_count": failed_total,
            "predicate_type_counts": predicate_totals,
            "files": file_diags,
        }
    )

    logger.info(
        "SQL ingest summary: files=%d edges=%d unresolved_aliases=%d fallbacks=%d failed_statements=%d",
        len(file_diags),
        len(edges),
        unresolved_alias_total,
        fallback_total,
        failed_total,
    )

    return edges


def edges_to_inferred_fks(edges: List[JoinEdge]) -> Dict[str, List[Tuple[List[str], str, List[str], float, str]]]:
    """Convert JoinEdges to a FK-like mapping keyed by constrained table.

    Returns dict:
      constrained_table -> list of (constrained_cols, referred_table, referred_cols, confidence, evidence)
    We infer direction when one side is ID and the other is *_ID.
    """
    out: Dict[str, List[Tuple[List[str], str, List[str], float, str]]] = {}

    def add(ct, cc, rt, rc, conf, ev):
        out.setdefault(ct, []).append((cc, rt, rc, conf, ev))

    for edge in edges:
        a_id = edge.left_column == "ID"
        b_id = edge.right_column == "ID"
        a_fkish = edge.left_column.endswith("_ID") and not a_id
        b_fkish = edge.right_column.endswith("_ID") and not b_id

        if a_fkish and b_id:
            add(edge.left_table, [edge.left_column], edge.right_table, [edge.right_column], edge.confidence, edge.evidence)
        elif b_fkish and a_id:
            add(edge.right_table, [edge.right_column], edge.left_table, [edge.left_column], edge.confidence, edge.evidence)
        else:
            add(edge.left_table, [edge.left_column], edge.right_table, [edge.right_column], max(0.4, edge.confidence - 0.15), edge.evidence)
            add(edge.right_table, [edge.right_column], edge.left_table, [edge.left_column], max(0.4, edge.confidence - 0.15), edge.evidence)

    for key, rows in out.items():
        seen = set()
        dedup: List[Tuple[List[str], str, List[str], float, str]] = []
        for cc, rt, rc, conf, ev in rows:
            dedup_key = (tuple(cc), rt, tuple(rc))
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            dedup.append((cc, rt, rc, conf, ev))
        out[key] = dedup

    return out
