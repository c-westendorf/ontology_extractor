from __future__ import annotations
from dataclasses import dataclass

@dataclass
class RetrievalItem:
    source: str
    text: str
