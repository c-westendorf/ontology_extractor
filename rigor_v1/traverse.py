from __future__ import annotations
from collections import defaultdict, deque
from .db_introspect import TableInfo

def topo_sort_tables(tables: list[TableInfo]) -> list[TableInfo]:
    by_name = {t.name: t for t in tables}
    indeg = {t.name: 0 for t in tables}
    adj = defaultdict(list)

    for t in tables:
        for fk in t.foreign_keys:
            if fk.referred_table in by_name:
                adj[fk.referred_table].append(t.name)
                indeg[t.name] += 1

    q = deque([n for n, d in indeg.items() if d == 0])
    order = []
    while q:
        n = q.popleft()
        order.append(by_name[n])
        for m in adj[n]:
            indeg[m] -= 1
            if indeg[m] == 0:
                q.append(m)

    if len(order) != len(tables):
        remaining = [by_name[n] for n, d in indeg.items() if d > 0]
        order.extend(remaining)
    return order
