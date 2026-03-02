"""Tests for generation_cache.py module."""

import pytest
import tempfile
import json
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, List

from rigor_sf.generation_cache import (
    TableFingerprint,
    CacheEntry,
    GenerationCache,
    compute_schema_hash,
    compute_fk_hash,
    compute_comment_hash,
    compute_fingerprint,
    create_cache,
)


# Mock TableInfo and related classes for testing
@dataclass
class MockColumnInfo:
    name: str
    type: str
    nullable: bool
    comment: Optional[str] = None


@dataclass
class MockForeignKeyInfo:
    constrained_columns: List[str]
    referred_table: str
    referred_columns: List[str]
    confidence: float = 1.0
    evidence: Optional[str] = None


@dataclass
class MockTableInfo:
    name: str
    columns: List[MockColumnInfo]
    primary_key: List[str]
    foreign_keys: List[MockForeignKeyInfo]
    comment: Optional[str] = None


class TestTableFingerprint:
    """Tests for TableFingerprint dataclass."""

    def test_creation(self):
        """Create a TableFingerprint."""
        fp = TableFingerprint(
            schema_hash="abc123",
            fk_hash="def456",
            classification="fact",
            comment_hash="ghi789",
        )
        assert fp.schema_hash == "abc123"
        assert fp.fk_hash == "def456"
        assert fp.classification == "fact"
        assert fp.comment_hash == "ghi789"

    def test_equality(self):
        """Fingerprints with same values are equal."""
        fp1 = TableFingerprint("a", "b", "c", "d")
        fp2 = TableFingerprint("a", "b", "c", "d")
        assert fp1 == fp2

    def test_inequality(self):
        """Fingerprints with different values are not equal."""
        fp1 = TableFingerprint("a", "b", "c", "d")
        fp2 = TableFingerprint("a", "b", "c", "e")  # Different comment_hash
        assert fp1 != fp2


class TestCacheEntry:
    """Tests for CacheEntry dataclass."""

    def test_to_dict(self):
        """Convert CacheEntry to dict."""
        fp = TableFingerprint("hash1", "hash2", "fact", "hash3")
        entry = CacheEntry(
            table_name="CUSTOMER",
            fingerprint=fp,
            ttl_content="@prefix : <http://example.org#> .",
            header={"created_entities": {"classes": ["Customer"]}},
            timestamp="2024-03-15T14:30:00",
            llm_model="claude-3.5-sonnet",
        )

        d = entry.to_dict()
        assert d["table_name"] == "CUSTOMER"
        assert d["fingerprint"]["schema_hash"] == "hash1"
        assert d["fingerprint"]["classification"] == "fact"
        assert d["ttl_content"] == "@prefix : <http://example.org#> ."
        assert d["llm_model"] == "claude-3.5-sonnet"

    def test_from_dict(self):
        """Create CacheEntry from dict."""
        d = {
            "table_name": "ORDER",
            "fingerprint": {
                "schema_hash": "abc",
                "fk_hash": "def",
                "classification": "dimension",
                "comment_hash": "ghi",
            },
            "ttl_content": "# TTL content",
            "header": {"assumptions": []},
            "timestamp": "2024-03-15T14:30:00",
            "llm_model": "gpt-4",
        }

        entry = CacheEntry.from_dict(d)
        assert entry.table_name == "ORDER"
        assert entry.fingerprint.schema_hash == "abc"
        assert entry.fingerprint.classification == "dimension"
        assert entry.ttl_content == "# TTL content"
        assert entry.llm_model == "gpt-4"

    def test_round_trip(self):
        """to_dict and from_dict are inverse operations."""
        fp = TableFingerprint("a", "b", None, "c")
        original = CacheEntry(
            table_name="TEST",
            fingerprint=fp,
            ttl_content="content",
            header={"key": "value"},
            timestamp="2024-01-01T00:00:00",
            llm_model="test-model",
        )

        d = original.to_dict()
        restored = CacheEntry.from_dict(d)

        assert restored.table_name == original.table_name
        assert restored.fingerprint == original.fingerprint
        assert restored.ttl_content == original.ttl_content
        assert restored.header == original.header
        assert restored.timestamp == original.timestamp
        assert restored.llm_model == original.llm_model


class TestGenerationCache:
    """Tests for GenerationCache class."""

    def test_create_empty_cache(self):
        """Create an empty cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = GenerationCache(cache_dir=Path(tmpdir))
            assert len(cache.entries) == 0
            assert cache.stats()["total_entries"] == 0

    def test_put_and_get(self):
        """Store and retrieve a cache entry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = GenerationCache(cache_dir=Path(tmpdir))
            fp = TableFingerprint("a", "b", "fact", "c")

            entry = cache.put(
                table_name="customer",
                fingerprint=fp,
                ttl_content="turtle content",
                header={"entities": []},
                llm_model="test",
            )

            assert entry.table_name == "CUSTOMER"  # Uppercased
            retrieved = cache.get("customer")
            assert retrieved is not None
            assert retrieved.ttl_content == "turtle content"

    def test_get_nonexistent(self):
        """Get returns None for missing entry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = GenerationCache(cache_dir=Path(tmpdir))
            assert cache.get("nonexistent") is None

    def test_is_valid_cache_hit(self):
        """is_valid returns True when fingerprint matches."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = GenerationCache(cache_dir=Path(tmpdir))
            fp = TableFingerprint("hash1", "hash2", "entity", "hash3")

            cache.put(
                table_name="test_table",
                fingerprint=fp,
                ttl_content="content",
                header={},
                llm_model="model",
            )

            # Same fingerprint should be valid
            same_fp = TableFingerprint("hash1", "hash2", "entity", "hash3")
            assert cache.is_valid("test_table", same_fp) is True

    def test_is_valid_cache_miss_different_schema(self):
        """is_valid returns False when schema hash differs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = GenerationCache(cache_dir=Path(tmpdir))
            fp = TableFingerprint("hash1", "hash2", "entity", "hash3")

            cache.put(
                table_name="test_table",
                fingerprint=fp,
                ttl_content="content",
                header={},
                llm_model="model",
            )

            # Different schema hash
            different_fp = TableFingerprint("CHANGED", "hash2", "entity", "hash3")
            assert cache.is_valid("test_table", different_fp) is False

    def test_is_valid_cache_miss_different_fk(self):
        """is_valid returns False when FK hash differs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = GenerationCache(cache_dir=Path(tmpdir))
            fp = TableFingerprint("hash1", "hash2", "entity", "hash3")

            cache.put(
                table_name="test_table",
                fingerprint=fp,
                ttl_content="content",
                header={},
                llm_model="model",
            )

            # Different FK hash
            different_fp = TableFingerprint("hash1", "CHANGED", "entity", "hash3")
            assert cache.is_valid("test_table", different_fp) is False

    def test_is_valid_cache_miss_different_classification(self):
        """is_valid returns False when classification differs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = GenerationCache(cache_dir=Path(tmpdir))
            fp = TableFingerprint("hash1", "hash2", "entity", "hash3")

            cache.put(
                table_name="test_table",
                fingerprint=fp,
                ttl_content="content",
                header={},
                llm_model="model",
            )

            # Different classification
            different_fp = TableFingerprint("hash1", "hash2", "fact", "hash3")
            assert cache.is_valid("test_table", different_fp) is False

    def test_is_valid_cache_miss_different_comment(self):
        """is_valid returns False when comment hash differs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = GenerationCache(cache_dir=Path(tmpdir))
            fp = TableFingerprint("hash1", "hash2", "entity", "hash3")

            cache.put(
                table_name="test_table",
                fingerprint=fp,
                ttl_content="content",
                header={},
                llm_model="model",
            )

            # Different comment hash
            different_fp = TableFingerprint("hash1", "hash2", "entity", "CHANGED")
            assert cache.is_valid("test_table", different_fp) is False

    def test_is_valid_no_entry(self):
        """is_valid returns False when no entry exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = GenerationCache(cache_dir=Path(tmpdir))
            fp = TableFingerprint("a", "b", "c", "d")
            assert cache.is_valid("nonexistent", fp) is False

    def test_invalidate_existing(self):
        """Invalidate removes existing entry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = GenerationCache(cache_dir=Path(tmpdir))
            fp = TableFingerprint("a", "b", "c", "d")
            cache.put("table1", fp, "content", {}, "model")

            assert cache.get("table1") is not None
            result = cache.invalidate("table1")
            assert result is True
            assert cache.get("table1") is None

    def test_invalidate_nonexistent(self):
        """Invalidate returns False for missing entry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = GenerationCache(cache_dir=Path(tmpdir))
            result = cache.invalidate("nonexistent")
            assert result is False

    def test_clear(self):
        """Clear removes all entries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = GenerationCache(cache_dir=Path(tmpdir))
            fp = TableFingerprint("a", "b", "c", "d")
            cache.put("table1", fp, "content1", {}, "model")
            cache.put("table2", fp, "content2", {}, "model")

            assert cache.stats()["total_entries"] == 2
            count = cache.clear()
            assert count == 2
            assert cache.stats()["total_entries"] == 0

    def test_save_and_load(self):
        """Save cache to disk and load it back."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create and populate cache
            cache1 = GenerationCache(cache_dir=Path(tmpdir))
            fp = TableFingerprint("schema", "fk", "fact", "comment")
            cache1.put(
                table_name="my_table",
                fingerprint=fp,
                ttl_content="# Turtle content\n@prefix : <http://example.org#> .",
                header={"created_entities": {"classes": ["MyTable"]}},
                llm_model="claude-3.5-sonnet",
            )
            cache1.save()

            # Verify file was created
            assert cache1.cache_file.exists()

            # Load into new cache instance
            cache2 = GenerationCache(cache_dir=Path(tmpdir))
            cache2.load()

            entry = cache2.get("my_table")
            assert entry is not None
            assert entry.table_name == "MY_TABLE"
            assert entry.fingerprint.schema_hash == "schema"
            assert entry.fingerprint.classification == "fact"
            assert "# Turtle content" in entry.ttl_content
            assert entry.llm_model == "claude-3.5-sonnet"

    def test_load_missing_file(self):
        """Load with missing file starts empty."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = GenerationCache(cache_dir=Path(tmpdir))
            cache.load()  # Should not raise
            assert len(cache.entries) == 0

    def test_load_corrupt_file(self):
        """Load with corrupt file starts empty."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            cache_file = cache_dir / ".generation_cache.json"
            cache_file.write_text("not valid json {{{", encoding="utf-8")

            cache = GenerationCache(cache_dir=cache_dir)
            cache.load()  # Should not raise, should start fresh
            assert len(cache.entries) == 0

    def test_dirty_tracking(self):
        """Cache tracks dirty state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = GenerationCache(cache_dir=Path(tmpdir))
            assert cache._dirty is False

            fp = TableFingerprint("a", "b", "c", "d")
            cache.put("table", fp, "content", {}, "model")
            assert cache._dirty is True

            cache.save()
            assert cache._dirty is False

    def test_case_insensitive_lookup(self):
        """Cache keys are case-insensitive (uppercase normalized)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = GenerationCache(cache_dir=Path(tmpdir))
            fp = TableFingerprint("a", "b", "c", "d")

            cache.put("MyTable", fp, "content", {}, "model")

            # All variations should find the entry
            assert cache.get("MyTable") is not None
            assert cache.get("mytable") is not None
            assert cache.get("MYTABLE") is not None

    def test_stats(self):
        """Stats returns correct information."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = GenerationCache(cache_dir=Path(tmpdir))
            fp = TableFingerprint("a", "b", "c", "d")

            cache.put("table_a", fp, "content_a", {}, "model")
            cache.put("table_b", fp, "content_b", {}, "model")
            cache.put("table_c", fp, "content_c", {}, "model")

            stats = cache.stats()
            assert stats["total_entries"] == 3
            assert set(stats["tables"]) == {"TABLE_A", "TABLE_B", "TABLE_C"}


class TestComputeSchemaHash:
    """Tests for compute_schema_hash."""

    def test_basic_table(self):
        """Compute hash for a simple table."""
        table = MockTableInfo(
            name="customer",
            columns=[
                MockColumnInfo("id", "INTEGER", False),
                MockColumnInfo("name", "VARCHAR(100)", True),
            ],
            primary_key=["id"],
            foreign_keys=[],
        )

        h = compute_schema_hash(table)
        assert len(h) == 16  # SHA-256 truncated to 16 chars
        assert h.isalnum()

    def test_hash_deterministic(self):
        """Same table produces same hash."""
        table1 = MockTableInfo(
            name="test",
            columns=[MockColumnInfo("a", "INT", False)],
            primary_key=[],
            foreign_keys=[],
        )
        table2 = MockTableInfo(
            name="test",
            columns=[MockColumnInfo("a", "INT", False)],
            primary_key=[],
            foreign_keys=[],
        )

        assert compute_schema_hash(table1) == compute_schema_hash(table2)

    def test_hash_differs_with_column_change(self):
        """Different columns produce different hash."""
        table1 = MockTableInfo(
            name="test",
            columns=[MockColumnInfo("a", "INT", False)],
            primary_key=[],
            foreign_keys=[],
        )
        table2 = MockTableInfo(
            name="test",
            columns=[MockColumnInfo("b", "INT", False)],  # Different column name
            primary_key=[],
            foreign_keys=[],
        )

        assert compute_schema_hash(table1) != compute_schema_hash(table2)

    def test_hash_differs_with_type_change(self):
        """Different column type produces different hash."""
        table1 = MockTableInfo(
            name="test",
            columns=[MockColumnInfo("a", "INT", False)],
            primary_key=[],
            foreign_keys=[],
        )
        table2 = MockTableInfo(
            name="test",
            columns=[MockColumnInfo("a", "VARCHAR", False)],  # Different type
            primary_key=[],
            foreign_keys=[],
        )

        assert compute_schema_hash(table1) != compute_schema_hash(table2)

    def test_hash_differs_with_nullability_change(self):
        """Different nullability produces different hash."""
        table1 = MockTableInfo(
            name="test",
            columns=[MockColumnInfo("a", "INT", False)],
            primary_key=[],
            foreign_keys=[],
        )
        table2 = MockTableInfo(
            name="test",
            columns=[MockColumnInfo("a", "INT", True)],  # Different nullability
            primary_key=[],
            foreign_keys=[],
        )

        assert compute_schema_hash(table1) != compute_schema_hash(table2)

    def test_hash_column_order_independent(self):
        """Column order doesn't affect hash (sorted internally)."""
        table1 = MockTableInfo(
            name="test",
            columns=[
                MockColumnInfo("a", "INT", False),
                MockColumnInfo("b", "VARCHAR", True),
            ],
            primary_key=[],
            foreign_keys=[],
        )
        table2 = MockTableInfo(
            name="test",
            columns=[
                MockColumnInfo("b", "VARCHAR", True),
                MockColumnInfo("a", "INT", False),
            ],
            primary_key=[],
            foreign_keys=[],
        )

        assert compute_schema_hash(table1) == compute_schema_hash(table2)


class TestComputeFkHash:
    """Tests for compute_fk_hash."""

    def test_no_foreign_keys(self):
        """Hash for table with no FKs."""
        table = MockTableInfo(
            name="standalone",
            columns=[MockColumnInfo("id", "INT", False)],
            primary_key=["id"],
            foreign_keys=[],
        )

        h = compute_fk_hash(table)
        assert len(h) == 16

    def test_with_foreign_keys(self):
        """Hash for table with FKs."""
        table = MockTableInfo(
            name="order",
            columns=[MockColumnInfo("id", "INT", False)],
            primary_key=["id"],
            foreign_keys=[
                MockForeignKeyInfo(["customer_id"], "customer", ["id"]),
                MockForeignKeyInfo(["product_id"], "product", ["id"]),
            ],
        )

        h = compute_fk_hash(table)
        assert len(h) == 16

    def test_hash_differs_with_fk_change(self):
        """Adding FK changes hash."""
        table1 = MockTableInfo(
            name="order",
            columns=[MockColumnInfo("id", "INT", False)],
            primary_key=["id"],
            foreign_keys=[],
        )
        table2 = MockTableInfo(
            name="order",
            columns=[MockColumnInfo("id", "INT", False)],
            primary_key=["id"],
            foreign_keys=[
                MockForeignKeyInfo(["customer_id"], "customer", ["id"]),
            ],
        )

        assert compute_fk_hash(table1) != compute_fk_hash(table2)

    def test_fk_order_independent(self):
        """FK order doesn't affect hash."""
        table1 = MockTableInfo(
            name="order",
            columns=[],
            primary_key=[],
            foreign_keys=[
                MockForeignKeyInfo(["customer_id"], "customer", ["id"]),
                MockForeignKeyInfo(["product_id"], "product", ["id"]),
            ],
        )
        table2 = MockTableInfo(
            name="order",
            columns=[],
            primary_key=[],
            foreign_keys=[
                MockForeignKeyInfo(["product_id"], "product", ["id"]),
                MockForeignKeyInfo(["customer_id"], "customer", ["id"]),
            ],
        )

        assert compute_fk_hash(table1) == compute_fk_hash(table2)


class TestComputeCommentHash:
    """Tests for compute_comment_hash."""

    def test_no_comments(self):
        """Hash for table with no comments."""
        table = MockTableInfo(
            name="test",
            columns=[MockColumnInfo("id", "INT", False)],
            primary_key=[],
            foreign_keys=[],
        )

        h = compute_comment_hash(table)
        assert len(h) == 16

    def test_with_table_comment(self):
        """Hash includes table comment."""
        table1 = MockTableInfo(
            name="test",
            columns=[MockColumnInfo("id", "INT", False)],
            primary_key=[],
            foreign_keys=[],
            comment=None,
        )
        table2 = MockTableInfo(
            name="test",
            columns=[MockColumnInfo("id", "INT", False)],
            primary_key=[],
            foreign_keys=[],
            comment="Customer information",
        )

        assert compute_comment_hash(table1) != compute_comment_hash(table2)

    def test_with_column_comments(self):
        """Hash includes column comments."""
        table1 = MockTableInfo(
            name="test",
            columns=[MockColumnInfo("id", "INT", False, comment=None)],
            primary_key=[],
            foreign_keys=[],
        )
        table2 = MockTableInfo(
            name="test",
            columns=[MockColumnInfo("id", "INT", False, comment="Primary key")],
            primary_key=[],
            foreign_keys=[],
        )

        assert compute_comment_hash(table1) != compute_comment_hash(table2)


class TestComputeFingerprint:
    """Tests for compute_fingerprint."""

    def test_full_fingerprint(self):
        """Compute full fingerprint for a table."""
        table = MockTableInfo(
            name="customer",
            columns=[
                MockColumnInfo("id", "INT", False, "Primary key"),
                MockColumnInfo("name", "VARCHAR", True, "Customer name"),
            ],
            primary_key=["id"],
            foreign_keys=[],
            comment="Customer dimension table",
        )

        fp = compute_fingerprint(table, classification="dimension")

        assert fp.schema_hash is not None and len(fp.schema_hash) == 16
        assert fp.fk_hash is not None and len(fp.fk_hash) == 16
        assert fp.comment_hash is not None and len(fp.comment_hash) == 16
        assert fp.classification == "dimension"

    def test_fingerprint_with_no_classification(self):
        """Fingerprint works with None classification."""
        table = MockTableInfo(
            name="test",
            columns=[MockColumnInfo("id", "INT", False)],
            primary_key=[],
            foreign_keys=[],
        )

        fp = compute_fingerprint(table, classification=None)
        assert fp.classification is None

    def test_fingerprint_deterministic(self):
        """Same inputs produce same fingerprint."""
        table = MockTableInfo(
            name="test",
            columns=[MockColumnInfo("id", "INT", False)],
            primary_key=["id"],
            foreign_keys=[],
        )

        fp1 = compute_fingerprint(table, "fact")
        fp2 = compute_fingerprint(table, "fact")

        assert fp1 == fp2


class TestCreateCache:
    """Tests for create_cache factory function."""

    def test_creates_cache(self):
        """create_cache returns a loaded cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = create_cache(tmpdir)
            assert isinstance(cache, GenerationCache)
            assert cache.cache_dir == Path(tmpdir)

    def test_loads_existing_cache(self):
        """create_cache loads existing cache file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Pre-create a cache file
            cache_file = Path(tmpdir) / ".generation_cache.json"
            cache_data = {
                "version": "1.0",
                "updated": "2024-01-01T00:00:00",
                "entries": {
                    "TEST_TABLE": {
                        "table_name": "TEST_TABLE",
                        "fingerprint": {
                            "schema_hash": "abc",
                            "fk_hash": "def",
                            "classification": "fact",
                            "comment_hash": "ghi",
                        },
                        "ttl_content": "# content",
                        "header": {},
                        "timestamp": "2024-01-01T00:00:00",
                        "llm_model": "test",
                    }
                },
            }
            cache_file.write_text(json.dumps(cache_data), encoding="utf-8")

            # create_cache should load it
            cache = create_cache(tmpdir)
            assert cache.get("test_table") is not None
            assert cache.stats()["total_entries"] == 1


class TestIntegration:
    """Integration tests for the cache workflow."""

    def test_full_workflow(self):
        """Test complete cache workflow: compute fingerprint, cache, invalidate."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create table
            table = MockTableInfo(
                name="order_items",
                columns=[
                    MockColumnInfo("id", "BIGINT", False),
                    MockColumnInfo("order_id", "BIGINT", False),
                    MockColumnInfo("product_id", "BIGINT", False),
                    MockColumnInfo("quantity", "INTEGER", True),
                ],
                primary_key=["id"],
                foreign_keys=[
                    MockForeignKeyInfo(["order_id"], "orders", ["id"]),
                    MockForeignKeyInfo(["product_id"], "products", ["id"]),
                ],
                comment="Line items in an order",
            )

            # Create cache and compute fingerprint
            cache = create_cache(tmpdir)
            fp = compute_fingerprint(table, "bridge")

            # First time - cache miss
            assert cache.is_valid("order_items", fp) is False

            # Add to cache
            cache.put(
                table_name="order_items",
                fingerprint=fp,
                ttl_content="@prefix : <http://example.org#> .\n:OrderItem a owl:Class .",
                header={"created_entities": {"classes": ["OrderItem"]}},
                llm_model="claude-3.5-sonnet",
            )

            # Now - cache hit
            assert cache.is_valid("order_items", fp) is True

            # Save and reload
            cache.save()
            cache2 = create_cache(tmpdir)

            # Still valid after reload
            assert cache2.is_valid("order_items", fp) is True

            # Modify table (add column)
            table.columns.append(MockColumnInfo("discount", "DECIMAL", True))
            new_fp = compute_fingerprint(table, "bridge")

            # Now invalid - schema changed
            assert cache2.is_valid("order_items", new_fp) is False

    def test_classification_change_invalidates(self):
        """Changing classification invalidates cache."""
        with tempfile.TemporaryDirectory() as tmpdir:
            table = MockTableInfo(
                name="product",
                columns=[MockColumnInfo("id", "INT", False)],
                primary_key=["id"],
                foreign_keys=[],
            )

            cache = create_cache(tmpdir)
            fp1 = compute_fingerprint(table, "dimension")
            cache.put("product", fp1, "content", {}, "model")

            # Same table, different classification
            fp2 = compute_fingerprint(table, "entity")
            assert cache.is_valid("product", fp2) is False
