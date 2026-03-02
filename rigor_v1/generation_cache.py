"""Generation cache for incremental OWL fragment generation.

Implements caching of LLM-generated fragments per SPEC_V2.md §6 to enable
incremental generation. Tables are only regenerated when their inputs change.

Cache entries track:
- Table schema fingerprint (columns, types, nullability)
- FK relationships fingerprint
- Classification fingerprint
- Metadata (comments) fingerprint

When any input changes, the table is regenerated; otherwise, cached fragment is reused.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .db_introspect import TableInfo


@dataclass
class TableFingerprint:
    """Fingerprint of all inputs for a table's generation."""

    schema_hash: str  # Hash of columns, types, nullability
    fk_hash: str  # Hash of foreign key relationships
    classification: str | None  # Table classification (fact/dimension/entity/bridge/staging)
    comment_hash: str  # Hash of table and column comments


@dataclass
class CacheEntry:
    """A cached generation result for a table."""

    table_name: str
    fingerprint: TableFingerprint
    ttl_content: str
    header: dict  # created_entities, assumptions
    timestamp: str  # When generated
    llm_model: str  # Which model generated it

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            "table_name": self.table_name,
            "fingerprint": asdict(self.fingerprint),
            "ttl_content": self.ttl_content,
            "header": self.header,
            "timestamp": self.timestamp,
            "llm_model": self.llm_model,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CacheEntry":
        """Create from JSON dict."""
        fp_data = data["fingerprint"]
        fingerprint = TableFingerprint(
            schema_hash=fp_data["schema_hash"],
            fk_hash=fp_data["fk_hash"],
            classification=fp_data.get("classification"),
            comment_hash=fp_data["comment_hash"],
        )
        return cls(
            table_name=data["table_name"],
            fingerprint=fingerprint,
            ttl_content=data["ttl_content"],
            header=data["header"],
            timestamp=data["timestamp"],
            llm_model=data["llm_model"],
        )


@dataclass
class GenerationCache:
    """Cache for incremental generation.

    Stores and retrieves cached OWL fragments based on input fingerprints.
    """

    cache_dir: Path
    entries: dict[str, CacheEntry] = field(default_factory=dict)
    _dirty: bool = False

    @property
    def cache_file(self) -> Path:
        """Path to the cache JSON file."""
        return self.cache_dir / ".generation_cache.json"

    def load(self) -> None:
        """Load cache from disk."""
        if self.cache_file.exists():
            try:
                data = json.loads(self.cache_file.read_text(encoding="utf-8"))
                self.entries = {
                    name: CacheEntry.from_dict(entry)
                    for name, entry in data.get("entries", {}).items()
                }
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                # Corrupt cache, start fresh
                print(f"[cache] Warning: Could not load cache ({e}), starting fresh")
                self.entries = {}
        else:
            self.entries = {}
        self._dirty = False

    def save(self) -> None:
        """Save cache to disk if modified."""
        if not self._dirty:
            return

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        data = {
            "version": "1.0",
            "updated": datetime.now().isoformat(),
            "entries": {name: entry.to_dict() for name, entry in self.entries.items()},
        }
        self.cache_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
        self._dirty = False

    def get(self, table_name: str) -> CacheEntry | None:
        """Get cached entry for a table."""
        return self.entries.get(table_name.upper())

    def put(
        self,
        table_name: str,
        fingerprint: TableFingerprint,
        ttl_content: str,
        header: dict,
        llm_model: str,
    ) -> CacheEntry:
        """Store a cache entry for a table."""
        entry = CacheEntry(
            table_name=table_name.upper(),
            fingerprint=fingerprint,
            ttl_content=ttl_content,
            header=header,
            timestamp=datetime.now().isoformat(),
            llm_model=llm_model,
        )
        self.entries[table_name.upper()] = entry
        self._dirty = True
        return entry

    def invalidate(self, table_name: str) -> bool:
        """Remove a table from the cache.

        Returns:
            True if entry was removed, False if not found
        """
        key = table_name.upper()
        if key in self.entries:
            del self.entries[key]
            self._dirty = True
            return True
        return False

    def clear(self) -> int:
        """Clear all cache entries.

        Returns:
            Number of entries cleared
        """
        count = len(self.entries)
        self.entries = {}
        self._dirty = True
        return count

    def is_valid(self, table_name: str, fingerprint: TableFingerprint) -> bool:
        """Check if cached entry is valid for given fingerprint.

        Args:
            table_name: Table name to check
            fingerprint: Current fingerprint to compare against

        Returns:
            True if cache hit (fingerprint matches), False otherwise
        """
        entry = self.get(table_name)
        if entry is None:
            return False

        cached_fp = entry.fingerprint
        return (
            cached_fp.schema_hash == fingerprint.schema_hash
            and cached_fp.fk_hash == fingerprint.fk_hash
            and cached_fp.classification == fingerprint.classification
            and cached_fp.comment_hash == fingerprint.comment_hash
        )

    def stats(self) -> dict:
        """Get cache statistics."""
        return {
            "total_entries": len(self.entries),
            "tables": list(self.entries.keys()),
        }


def compute_schema_hash(table: "TableInfo") -> str:
    """Compute hash of table schema (columns, types, nullability).

    Args:
        table: TableInfo to hash

    Returns:
        SHA-256 hex digest (first 16 chars)
    """
    col_data = []
    for c in sorted(table.columns, key=lambda x: x.name):
        col_data.append((c.name, c.type, c.nullable))

    content = json.dumps(
        {
            "name": table.name,
            "columns": col_data,
            "primary_key": sorted(table.primary_key) if table.primary_key else [],
        },
        sort_keys=True,
    )
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


def compute_fk_hash(table: "TableInfo") -> str:
    """Compute hash of foreign key relationships.

    Args:
        table: TableInfo with foreign_keys populated

    Returns:
        SHA-256 hex digest (first 16 chars)
    """
    fk_data = []
    for fk in sorted(table.foreign_keys, key=lambda x: (x.referred_table, str(x.constrained_columns))):
        fk_data.append(
            {
                "constrained_columns": sorted(fk.constrained_columns),
                "referred_table": fk.referred_table,
                "referred_columns": sorted(fk.referred_columns),
            }
        )

    content = json.dumps(fk_data, sort_keys=True)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


def compute_comment_hash(table: "TableInfo") -> str:
    """Compute hash of table and column comments.

    Args:
        table: TableInfo with comments

    Returns:
        SHA-256 hex digest (first 16 chars)
    """
    comment_data = {
        "table_comment": table.comment or "",
        "column_comments": {
            c.name: c.comment or "" for c in sorted(table.columns, key=lambda x: x.name)
        },
    }

    content = json.dumps(comment_data, sort_keys=True)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


def compute_fingerprint(table: "TableInfo", classification: str | None = None) -> TableFingerprint:
    """Compute full fingerprint for a table.

    Args:
        table: TableInfo with all data populated
        classification: Optional table classification

    Returns:
        TableFingerprint for cache comparison
    """
    return TableFingerprint(
        schema_hash=compute_schema_hash(table),
        fk_hash=compute_fk_hash(table),
        classification=classification,
        comment_hash=compute_comment_hash(table),
    )


def create_cache(fragments_dir: str | Path) -> GenerationCache:
    """Create and load a generation cache.

    Args:
        fragments_dir: Directory where fragments are stored

    Returns:
        Loaded GenerationCache instance
    """
    cache = GenerationCache(cache_dir=Path(fragments_dir))
    cache.load()
    return cache
