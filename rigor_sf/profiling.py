from __future__ import annotations
from dataclasses import dataclass
from sqlalchemy import create_engine, text

@dataclass
class EdgeProfile:
    constrained_table: str
    constrained_column: str
    referred_table: str
    referred_column: str
    sample_rows: int
    fk_nonnull: int
    match_count: int
    match_rate: float
    pk_distinct: int
    pk_total: int
    pk_unique_rate: float
    fk_null_rate: float

def profile_edge(db_url: str,
                 constrained_table: str, constrained_column: str,
                 referred_table: str, referred_column: str,
                 sample_limit: int = 200000) -> EdgeProfile:
    eng = create_engine(db_url)
    ct, cc, rt, rc = constrained_table, constrained_column, referred_table, referred_column

    q_fk = text(f"""
        WITH s AS (
          SELECT {cc} AS fk
          FROM {ct}
          LIMIT :lim
        )
        SELECT
          COUNT(*) AS sample_rows,
          SUM(IFF(fk IS NULL, 0, 1)) AS fk_nonnull
        FROM s
    """)
    q_match = text(f"""
        WITH s AS (
          SELECT {cc} AS fk
          FROM {ct}
          WHERE {cc} IS NOT NULL
          LIMIT :lim
        ),
        p AS (
          SELECT {rc} AS pk
          FROM {rt}
        )
        SELECT COUNT(*) AS match_count
        FROM s
        JOIN p ON s.fk = p.pk
    """)
    q_pk = text(f"""
        SELECT
          COUNT(*) AS pk_total,
          COUNT(DISTINCT {rc}) AS pk_distinct
        FROM {rt}
    """)

    with eng.connect() as conn:
        fk_row = conn.execute(q_fk, {"lim": sample_limit}).mappings().one()
        match_row = conn.execute(q_match, {"lim": sample_limit}).mappings().one()
        pk_row = conn.execute(q_pk).mappings().one()

    sample_rows = int(fk_row["sample_rows"] or 0)
    fk_nonnull = int(fk_row["fk_nonnull"] or 0)
    match_count = int(match_row["match_count"] or 0)
    pk_total = int(pk_row["pk_total"] or 0)
    pk_distinct = int(pk_row["pk_distinct"] or 0)

    match_rate = (match_count / fk_nonnull) if fk_nonnull else 0.0
    pk_unique_rate = (pk_distinct / pk_total) if pk_total else 0.0
    fk_null_rate = 1.0 - (fk_nonnull / sample_rows) if sample_rows else 1.0

    return EdgeProfile(
        constrained_table=ct, constrained_column=cc,
        referred_table=rt, referred_column=rc,
        sample_rows=sample_rows, fk_nonnull=fk_nonnull,
        match_count=match_count, match_rate=match_rate,
        pk_distinct=pk_distinct, pk_total=pk_total, pk_unique_rate=pk_unique_rate,
        fk_null_rate=fk_null_rate,
    )
