from __future__ import annotations
from rdflib import Graph

def merge_fragment(core: Graph, fragment_ttl: str) -> Graph:
    frag = Graph()
    frag.parse(data=fragment_ttl, format="turtle")
    for triple in frag:
        core.add(triple)
    return core
