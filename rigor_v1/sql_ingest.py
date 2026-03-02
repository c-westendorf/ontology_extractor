from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Set
import re

@dataclass
class JoinEdge:
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    confidence: float
    evidence: str  # snippet/file

_IDENTIFIER = r"[A-Za-z_][A-Za-z0-9_\$]*"
# Supports: db.schema.table or schema.table or table
TABLE_REF_RE = re.compile(rf"(?P<ref>{_IDENTIFIER}(?:\.{_IDENTIFIER}){{0,2}})")
# FROM/JOIN table [AS] alias
FROM_JOIN_RE = re.compile(
    rf"\b(?:from|join)\s+(?P<table>{_IDENTIFIER}(?:\.{_IDENTIFIER}){{0,2}})"
    rf"(?:\s+(?:as\s+)?(?P<alias>{_IDENTIFIER}))?",
    flags=re.IGNORECASE
)
# ON a.col = b.col (simple equality predicates; also captures with optional quoting)
ON_EQ_RE = re.compile(
    rf"\bON\b(?P<on>.*?)(?=\b(?:join|where|group\s+by|having|qualify|order\s+by|union|$)\b)",
    flags=re.IGNORECASE | re.DOTALL
)
COL_EQ_RE = re.compile(
    rf"(?P<a>{_IDENTIFIER}\.{_IDENTIFIER})\s*=\s*(?P<b>{_IDENTIFIER}\.{_IDENTIFIER})",
    flags=re.IGNORECASE
)

def _normalize_table(name: str) -> str:
    # Take last part (table) by default; Snowflake identifiers often uppercase.
    parts = name.split(".")
    return parts[-1].strip().strip('"').upper()

def _normalize_ident(name: str) -> str:
    return name.strip().strip('"').upper()

def _strip_sql_comments(sql: str) -> str:
    # Remove -- line comments and /* */ block comments
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return sql

def parse_sql_file(path: Path) -> List[JoinEdge]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    return parse_sql_text(text, evidence=str(path))

def parse_sql_text(sql: str, evidence: str = "") -> List[JoinEdge]:
    sql = _strip_sql_comments(sql)
    edges: List[JoinEdge] = []

    # Map aliases to tables within a statement-ish chunk.
    # We'll split on semicolons as a coarse statement boundary.
    statements = [s for s in re.split(r";\s*", sql) if s.strip()]
    for stmt in statements:
        alias_map: Dict[str, str] = {}
        tables: Set[str] = set()

        for m in FROM_JOIN_RE.finditer(stmt):
            t = _normalize_table(m.group("table"))
            a = m.group("alias")
            tables.add(t)
            if a:
                alias_map[_normalize_ident(a)] = t

        # Extract ON blocks and equality predicates
        for onm in ON_EQ_RE.finditer(stmt):
            on_block = onm.group("on")
            for eq in COL_EQ_RE.finditer(on_block):
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

                # Confidence heuristic:
                # - Higher if join looks like id/fk pattern
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

                snippet = (on_block.strip().replace("\n", " ")[:240])
                edges.append(JoinEdge(
                    left_table=a_table,
                    left_column=a_col_n,
                    right_table=b_table,
                    right_column=b_col_n,
                    confidence=conf,
                    evidence=f"{evidence} | ON {snippet}" if evidence else f"ON {snippet}"
                ))
    return edges

def ingest_sql_dir(sql_dir: str) -> List[JoinEdge]:
    p = Path(sql_dir)
    if not p.exists():
        raise FileNotFoundError(f"SQL directory not found: {sql_dir}")
    edges: List[JoinEdge] = []
    for f in sorted(p.rglob("*.sql")):
        edges.extend(parse_sql_file(f))
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

    for e in edges:
        a_id = (e.left_column == "ID")
        b_id = (e.right_column == "ID")
        a_fkish = e.left_column.endswith("_ID") and not a_id
        b_fkish = e.right_column.endswith("_ID") and not b_id

        # Prefer direction: fkish -> id
        if a_fkish and b_id:
            add(e.left_table, [e.left_column], e.right_table, [e.right_column], e.confidence, e.evidence)
        elif b_fkish and a_id:
            add(e.right_table, [e.right_column], e.left_table, [e.left_column], e.confidence, e.evidence)
        else:
            # Unknown direction: add both as undirected hints (lower confidence)
            add(e.left_table, [e.left_column], e.right_table, [e.right_column], max(0.4, e.confidence - 0.15), e.evidence)
            add(e.right_table, [e.right_column], e.left_table, [e.left_column], max(0.4, e.confidence - 0.15), e.evidence)

    # Deduplicate (same constrained->referred, same cols)
    for k, lst in out.items():
        seen = set()
        dedup = []
        for cc, rt, rc, conf, ev in lst:
            key = (tuple(cc), rt, tuple(rc))
            if key in seen:
                continue
            seen.add(key)
            dedup.append((cc, rt, rc, conf, ev))
        out[k] = dedup

    return out
