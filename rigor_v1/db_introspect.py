from __future__ import annotations
from dataclasses import dataclass
from sqlalchemy import create_engine, inspect
from typing import Optional, List, Dict

@dataclass
class ColumnInfo:
    name: str
    type: str
    nullable: bool
    comment: Optional[str] = None

@dataclass
class ForeignKeyInfo:
    constrained_columns: List[str]
    referred_table: str
    referred_columns: List[str]
    confidence: float = 1.0
    evidence: Optional[str] = None

@dataclass
class TableInfo:
    name: str
    columns: List[ColumnInfo]
    primary_key: List[str]
    foreign_keys: List[ForeignKeyInfo]
    comment: Optional[str] = None

def introspect_schema(db_url: str, schema: str | None = None) -> List[TableInfo]:
    eng = create_engine(db_url)
    insp = inspect(eng)

    tables = insp.get_table_names(schema=schema)
    out: List[TableInfo] = []

    for t in tables:
        cols: List[ColumnInfo] = []
        for c in insp.get_columns(t, schema=schema):
            cols.append(ColumnInfo(
                name=c["name"],
                type=str(c["type"]),
                nullable=bool(c.get("nullable", True)),
                comment=c.get("comment"),
            ))

        pk = insp.get_pk_constraint(t, schema=schema).get("constrained_columns", []) or []
        fks: List[ForeignKeyInfo] = []
        for fk in insp.get_foreign_keys(t, schema=schema):
            referred = fk.get("referred_table")
            if not referred:
                continue
            fks.append(ForeignKeyInfo(
                constrained_columns=fk.get("constrained_columns", []) or [],
                referred_table=referred,
                referred_columns=fk.get("referred_columns", []) or [],
            ))

        out.append(TableInfo(
            name=t,
            columns=cols,
            primary_key=pk,
            foreign_keys=fks,
            comment=None,
        ))
    return out
