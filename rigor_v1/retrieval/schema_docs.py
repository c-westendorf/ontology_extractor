from __future__ import annotations
from ..types import RetrievalItem
from ..db_introspect import TableInfo

def schema_context(table: TableInfo) -> list[RetrievalItem]:
    lines = [f"TABLE {table.name}"]
    if getattr(table, "classification", None):
        lines.append(f"TABLE_CLASSIFICATION: {table.classification}")
    if table.comment:
        lines.append(f"TABLE_COMMENT: {table.comment}")
    lines.append("COLUMNS:")
    for c in table.columns:
        lines.append(
            f"- {c.name} : {c.type} {'NULL' if c.nullable else 'NOT NULL'}"
            + (f"  // {c.comment}" if c.comment else "")
        )
    if table.primary_key:
        lines.append(f"PRIMARY_KEY: {table.primary_key}")
    if table.foreign_keys:
        lines.append("FOREIGN_KEYS (declared or inferred):")
        for fk in table.foreign_keys:
            conf = getattr(fk, "confidence", 1.0)
            evidence = getattr(fk, "evidence", None)
            fk_line = f"- {fk.constrained_columns} -> {fk.referred_table}({fk.referred_columns})  [conf={conf:.2f}]"
            if evidence:
                fk_line += f"  // {evidence}"
            lines.append(fk_line)
    return [RetrievalItem(source="schema", text="\n".join(lines))]
