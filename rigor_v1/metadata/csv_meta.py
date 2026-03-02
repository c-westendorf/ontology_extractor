from __future__ import annotations
import csv
from pathlib import Path
from typing import Dict, Optional, Tuple

def load_table_comments(path: str) -> Dict[str, str]:
    p = Path(path)
    if not p.exists():
        return {}
    out: Dict[str, str] = {}
    with p.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            t = (row.get("table") or "").strip()
            c = (row.get("comment") or "").strip()
            if t and c:
                out[t] = c
    return out

def load_column_comments(path: str) -> Dict[Tuple[str, str], str]:
    p = Path(path)
    if not p.exists():
        return {}
    out: Dict[Tuple[str, str], str] = {}
    with p.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            t = (row.get("table") or "").strip()
            col = (row.get("column") or "").strip()
            c = (row.get("comment") or "").strip()
            if t and col and c:
                out[(t, col)] = c
    return out
