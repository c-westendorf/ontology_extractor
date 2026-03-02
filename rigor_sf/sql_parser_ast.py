from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

try:
    from sqlglot import exp, parse
    from sqlglot.errors import ParseError
    SQLGLOT_AVAILABLE = True
except Exception:  # pragma: no cover - environment-dependent import
    exp = None
    parse = None

    class ParseError(Exception):
        pass

    SQLGLOT_AVAILABLE = False


@dataclass
class JoinEdge:
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    confidence: float
    evidence: str
    parser_dialect: str = "snowflake"
    predicate_type: str = "eq_column_column"
    confidence_reason: str = "strict_column_equality"
    ast_path: str = ""
    source_query_block: str = "statement_1"


@dataclass
class ParserDiagnostics:
    file: str
    statement_count: int = 0
    parsed_statement_count: int = 0
    failed_statement_count: int = 0
    fallback_statement_count: int = 0
    unresolved_alias_count: int = 0
    skipped_predicate_count: int = 0
    predicate_type_counts: Dict[str, int] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)

    def mark_predicate(self, predicate_type: str) -> None:
        self.predicate_type_counts[predicate_type] = self.predicate_type_counts.get(predicate_type, 0) + 1


def _normalize_table(name: str) -> str:
    parts = [p.strip().strip('"') for p in name.split(".") if p.strip()]
    if not parts:
        return "UNKNOWN"
    return parts[-1].upper()


def _normalize_ident(name: str) -> str:
    return name.strip().strip('"').upper()


def _table_name(table_expr: exp.Table) -> str:
    catalog = table_expr.args.get("catalog")
    db = table_expr.args.get("db")
    table = table_expr.this
    pieces = []
    for node in (catalog, db, table):
        if node is None:
            continue
        if hasattr(node, "name"):
            pieces.append(node.name)
        else:
            pieces.append(str(node))
    return _normalize_table(".".join(pieces))


def _source_alias(table_expr: exp.Table) -> str:
    alias = table_expr.alias
    if alias:
        return _normalize_ident(alias)
    return _normalize_ident(table_expr.name)


def _collect_base_tables(node: exp.Expression, cte_map: Dict[str, Set[str]]) -> Set[str]:
    base: Set[str] = set()
    for table_expr in node.find_all(exp.Table):
        alias = _source_alias(table_expr)
        if alias in cte_map:
            base.update(cte_map[alias])
        else:
            base.add(_table_name(table_expr))
    return base


def _build_source_maps(statement: exp.Expression) -> Tuple[Dict[str, str], Dict[str, Set[str]]]:
    cte_map: Dict[str, Set[str]] = {}

    with_expr = statement.args.get("with_")
    if with_expr:
        for cte in with_expr.find_all(exp.CTE):
            cte_alias = _normalize_ident(cte.alias_or_name)
            cte_map[cte_alias] = _collect_base_tables(cte.this, cte_map)

    alias_map: Dict[str, str] = {}

    for table_expr in statement.find_all(exp.Table):
        resolved = _table_name(table_expr)
        alias_map[_source_alias(table_expr)] = resolved
        alias_map[_normalize_ident(table_expr.name)] = resolved

    for subquery in statement.find_all(exp.Subquery):
        if not subquery.alias:
            continue
        alias = _normalize_ident(subquery.alias)
        base = _collect_base_tables(subquery.this, cte_map)
        if len(base) == 1:
            alias_map[alias] = next(iter(base))
        elif base:
            cte_map[alias] = base

    for alias, bases in cte_map.items():
        if len(bases) == 1:
            alias_map[alias] = next(iter(bases))

    return alias_map, cte_map


def _flatten_predicates(node: exp.Expression) -> Iterable[exp.Expression]:
    if isinstance(node, exp.And):
        yield from _flatten_predicates(node.left)
        yield from _flatten_predicates(node.right)
        return
    if isinstance(node, exp.Paren):
        inner = node.this
        if isinstance(inner, exp.Expression):
            yield from _flatten_predicates(inner)
            return
    yield node


def _is_column(node: exp.Expression) -> bool:
    return isinstance(node, exp.Column)


def _is_casted_column(node: exp.Expression) -> bool:
    return isinstance(node, exp.Cast) and _is_column(node.this)


def _is_functional(node: exp.Expression) -> bool:
    return isinstance(node, (exp.Anonymous, exp.Func, exp.Substring, exp.Trim, exp.Lower, exp.Upper))


def _predicate_type(predicate: exp.Expression) -> str:
    if isinstance(predicate, exp.EQ):
        left = predicate.left
        right = predicate.right
        if _is_casted_column(left) or _is_casted_column(right):
            return "casted_eq"
        # Equality on functions/calls should be marked function_based even if both sides are expressions.
        if _is_functional(left) or _is_functional(right):
            return "function_based"
        if _is_column(left) and _is_column(right):
            return "eq_column_column"
        return "eq_expression"

    if isinstance(predicate, (exp.GT, exp.GTE, exp.LT, exp.LTE, exp.NEQ, exp.Like, exp.ILike)):
        return "range_or_inequality"

    if isinstance(predicate, (exp.Or, exp.In)):
        return "composite_predicate"

    if _is_functional(predicate):
        return "function_based"

    return "composite_predicate"


def _confidence_for(predicate_type: str, left_col: str, right_col: str) -> Tuple[float, str]:
    base_map = {
        "eq_column_column": 0.6,
        "casted_eq": 0.75,
        "eq_expression": 0.7,
        "range_or_inequality": 0.55,
        "function_based": 0.5,
        "composite_predicate": 0.65,
    }
    conf = base_map.get(predicate_type, 0.45)

    if predicate_type == "eq_column_column":
        if left_col.endswith("_ID") or left_col == "ID":
            conf += 0.1
        if right_col.endswith("_ID") or right_col == "ID":
            conf += 0.1
        if (left_col == "ID" and right_col.endswith("_ID")) or (right_col == "ID" and left_col.endswith("_ID")):
            conf += 0.15

    conf = min(conf, 0.95)
    reason = f"{predicate_type}:heuristic"
    return conf, reason


def _resolve_column_source(
    col_expr: exp.Expression,
    alias_map: Dict[str, str],
    cte_map: Dict[str, Set[str]],
    diagnostics: ParserDiagnostics,
) -> Tuple[str, str]:
    if isinstance(col_expr, exp.Cast):
        col_expr = col_expr.this

    if isinstance(col_expr, exp.Expression) and _is_functional(col_expr):
        for nested_col in col_expr.find_all(exp.Column):
            return _resolve_column_source(nested_col, alias_map, cte_map, diagnostics)

    if not isinstance(col_expr, exp.Column):
        return "UNKNOWN", "UNKNOWN"

    col_name = _normalize_ident(col_expr.name)
    qualifier = _normalize_ident(col_expr.table) if col_expr.table else ""

    if qualifier in alias_map:
        return alias_map[qualifier], col_name

    if qualifier in cte_map and cte_map[qualifier]:
        bases = sorted(cte_map[qualifier])
        if len(bases) > 1:
            diagnostics.unresolved_alias_count += 1
        return bases[0], col_name

    if qualifier:
        diagnostics.unresolved_alias_count += 1
        return _normalize_table(qualifier), col_name

    diagnostics.unresolved_alias_count += 1
    return "UNKNOWN", col_name


def _statement_block_name(idx: int) -> str:
    return f"statement_{idx + 1}"


def parse_sql_text_ast(
    sql: str,
    evidence: str = "",
    dialect_order: List[str] | None = None,
) -> Tuple[List[JoinEdge], ParserDiagnostics]:
    dialect_order = dialect_order or ["snowflake", ""]
    diagnostics = ParserDiagnostics(file=evidence or "<inline>")

    # Approx statement count before parse; parse may skip malformed statements.
    raw_statements = [s for s in sql.split(";") if s.strip()]
    diagnostics.statement_count = len(raw_statements)
    if not SQLGLOT_AVAILABLE:
        diagnostics.failed_statement_count = diagnostics.statement_count
        diagnostics.errors.append("sqlglot_not_installed")
        return [], diagnostics

    statements: List[exp.Expression] = []
    selected_dialect = "snowflake"
    parse_errors: List[str] = []

    for dialect in dialect_order:
        try:
            statements = parse(sql, read=dialect) if dialect else parse(sql)
            selected_dialect = dialect or "generic"
            break
        except ParseError as exc:
            parse_errors.append(f"dialect={dialect or 'generic'}: {exc}")

    if not statements:
        diagnostics.failed_statement_count = diagnostics.statement_count
        diagnostics.errors.extend(parse_errors)
        return [], diagnostics

    diagnostics.parsed_statement_count = len(statements)
    edges: List[JoinEdge] = []

    for idx, statement in enumerate(statements):
        try:
            alias_map, cte_map = _build_source_maps(statement)
            block_name = _statement_block_name(idx)

            for join in statement.find_all(exp.Join):
                on_expr = join.args.get("on")
                if on_expr is None:
                    continue

                for predicate in _flatten_predicates(on_expr):
                    if not isinstance(predicate, exp.Expression):
                        continue

                    ptype = _predicate_type(predicate)
                    diagnostics.mark_predicate(ptype)

                    if isinstance(predicate, exp.Binary):
                        left_table, left_col = _resolve_column_source(predicate.left, alias_map, cte_map, diagnostics)
                        right_table, right_col = _resolve_column_source(predicate.right, alias_map, cte_map, diagnostics)
                    else:
                        diagnostics.skipped_predicate_count += 1
                        continue

                    if left_col == "UNKNOWN" or right_col == "UNKNOWN":
                        diagnostics.skipped_predicate_count += 1
                        continue

                    if left_table in cte_map and cte_map[left_table]:
                        left_table = sorted(cte_map[left_table])[0]
                    if right_table in cte_map and cte_map[right_table]:
                        right_table = sorted(cte_map[right_table])[0]

                    conf, reason = _confidence_for(ptype, left_col, right_col)
                    pred_snip = predicate.sql(dialect=selected_dialect if selected_dialect != "generic" else None)
                    pred_snip = " ".join(pred_snip.split())[:220]
                    ev = f"{evidence} | {block_name} | {ptype} | {pred_snip}" if evidence else f"{block_name} | {ptype} | {pred_snip}"

                    edges.append(
                        JoinEdge(
                            left_table=left_table,
                            left_column=left_col,
                            right_table=right_table,
                            right_column=right_col,
                            confidence=conf,
                            evidence=ev,
                            parser_dialect=selected_dialect,
                            predicate_type=ptype,
                            confidence_reason=reason,
                            ast_path=predicate.key,
                            source_query_block=block_name,
                        )
                    )
        except Exception as exc:  # pragma: no cover - defensive guard
            diagnostics.failed_statement_count += 1
            diagnostics.errors.append(f"statement_{idx + 1}: {exc}")

    return edges, diagnostics


def parse_sql_file_ast(path: Path, dialect_order: List[str] | None = None) -> Tuple[List[JoinEdge], ParserDiagnostics]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    return parse_sql_text_ast(text, evidence=str(path), dialect_order=dialect_order)
