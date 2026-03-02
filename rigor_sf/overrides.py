from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import yaml

@dataclass
class OverrideEdge:
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    relation_name: Optional[str] = None
    status: str = "approved"  # approved | rejected

def _norm(s: str) -> str:
    return str(s).strip().strip('"').upper()

def _norm_cols(cols):
    if cols is None:
        return []
    if isinstance(cols, list):
        return [_norm(c) for c in cols if str(c).strip()]
    return [_norm(cols)]

def load_overrides(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {"approve": [], "reject": [], "rename": []}
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    data.setdefault("approve", [])
    data.setdefault("reject", [])
    data.setdefault("rename", [])
    return data

def save_overrides(path: str, data: dict) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False)

def upsert_edge_override(data: dict, edge: OverrideEdge) -> dict:
    def same(e):
        return (_norm(e.get("from", {}).get("table","")) == _norm(edge.from_table) and
                _norm_cols(e.get("from", {}).get("columns") or e.get("from", {}).get("column","")) == _norm_cols(edge.from_column) and
                _norm(e.get("to", {}).get("table","")) == _norm(edge.to_table) and
                _norm_cols(e.get("to", {}).get("columns") or e.get("to", {}).get("column","")) == _norm_cols(edge.to_column))
    data["approve"] = [e for e in data.get("approve", []) if not same(e)]
    data["reject"]  = [e for e in data.get("reject",  []) if not same(e)]

    item = {
        "from": {"table": _norm(edge.from_table), "columns": _norm_cols(edge.from_column)},
        "to":   {"table": _norm(edge.to_table),   "columns": _norm_cols(edge.to_column)},
    }
    if edge.relation_name:
        item["relation"] = edge.relation_name

    if edge.status == "rejected":
        data["reject"].append(item)
    else:
        data["approve"].append(item)
    return data

def is_rejected(data: dict, from_t: str, from_c: str, to_t: str, to_c: str) -> bool:
    ft, fc, tt, tc = map(_norm, (from_t, from_c, to_t, to_c))
    for e in data.get("reject", []):
        if (_norm(e.get("from", {}).get("table","")) == ft and
            _norm_cols(e.get("from", {}).get("columns") or e.get("from", {}).get("column","")) == [fc] and
            _norm(e.get("to", {}).get("table","")) == tt and
            _norm_cols(e.get("to", {}).get("columns") or e.get("to", {}).get("column","")) == [tc]):
            return True
    return False

def is_approved(data: dict, from_t: str, from_c: str, to_t: str, to_c: str) -> bool:
    ft, fc, tt, tc = map(_norm, (from_t, from_c, to_t, to_c))
    for e in data.get("approve", []):
        if (_norm(e.get("from", {}).get("table","")) == ft and
            _norm_cols(e.get("from", {}).get("columns") or e.get("from", {}).get("column","")) == [fc] and
            _norm(e.get("to", {}).get("table","")) == tt and
            _norm_cols(e.get("to", {}).get("columns") or e.get("to", {}).get("column","")) == [tc]):
            return True
    return False
