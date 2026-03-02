from __future__ import annotations
import re
from rdflib import Graph
from pathlib import Path

def load_core(path: str) -> Graph:
    g = Graph()
    p = Path(path)
    if p.exists() and p.stat().st_size > 0:
        g.parse(str(p))
    return g

def core_snippets(core: Graph, query_terms: list[str], k: int = 10) -> list[str]:
    ttl = core.serialize(format="turtle")
    hits = []
    for line in str(ttl).splitlines():
        if any(re.search(rf"\b{re.escape(t)}\b", line, flags=re.I) for t in query_terms if t):
            hits.append(line)
    return hits[:k]
