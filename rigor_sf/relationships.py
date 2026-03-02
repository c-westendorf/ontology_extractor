from __future__ import annotations
from typing import List
from pathlib import Path
import pandas as pd
from .sql_ingest import JoinEdge, edges_to_inferred_fks

def write_inferred_relationships_csv(edges: List[JoinEdge], out_path: str) -> None:
    rows = []
    inferred = edges_to_inferred_fks(edges)
    edge_index = {
        (e.left_table, e.left_column, e.right_table, e.right_column): e
        for e in edges
    }
    for ct, lst in inferred.items():
        for cc, rt, rc, conf, ev in lst:
            edge = edge_index.get((ct, cc[0] if cc else "", rt, rc[0] if rc else ""))
            rows.append({
                "from_table": ct,
                "from_column": cc[0] if cc else "",
                "to_table": rt,
                "to_column": rc[0] if rc else "",
                "confidence_sql": conf,
                "evidence": ev,
                "parser_dialect": getattr(edge, "parser_dialect", ""),
                "predicate_type": getattr(edge, "predicate_type", ""),
                "confidence_reason": getattr(edge, "confidence_reason", ""),
                "ast_path": getattr(edge, "ast_path", ""),
                "source_query_block": getattr(edge, "source_query_block", ""),
                "status": "proposed",
                "match_rate": "",
                "pk_unique_rate": "",
                "fk_null_rate": "",
            })
    df = pd.DataFrame(rows)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

def read_relationships_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def write_relationships_csv(df: pd.DataFrame, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
