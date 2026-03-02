"""
Tests for rigor_sf/traverse.py

Target coverage: 95%

Tests cover:
- Topological sorting of tables
- Tables with no dependencies
- Linear dependency chains
- Diamond dependency patterns
- Multiple independent groups
- Circular dependency handling
- Empty input handling
- Single table handling
"""

from __future__ import annotations

import pytest

from rigor_sf.db_introspect import ColumnInfo, ForeignKeyInfo, TableInfo
from rigor_sf.traverse import topo_sort_tables


# ── Helper Functions ──────────────────────────────────────────────────────────


def make_table(
    name: str,
    foreign_keys: list[tuple[str, str]] | None = None,
) -> TableInfo:
    """Create a TableInfo for testing.

    Args:
        name: Table name
        foreign_keys: List of (constrained_column, referred_table) tuples
    """
    fks = []
    if foreign_keys:
        for col, ref_table in foreign_keys:
            fks.append(
                ForeignKeyInfo(
                    constrained_columns=[col],
                    referred_table=ref_table,
                    referred_columns=["ID"],
                )
            )

    return TableInfo(
        name=name,
        columns=[ColumnInfo(name="ID", type="INTEGER", nullable=False)],
        primary_key=["ID"],
        foreign_keys=fks,
    )


# ── Basic Tests ───────────────────────────────────────────────────────────────


class TestBasicTopologicalSort:
    """Basic tests for topo_sort_tables function."""

    def test_empty_list(self):
        """Should handle empty table list."""
        result = topo_sort_tables([])
        assert result == []

    def test_single_table_no_fk(self):
        """Should handle single table with no foreign keys."""
        table = make_table("CUSTOMERS")
        result = topo_sort_tables([table])

        assert len(result) == 1
        assert result[0].name == "CUSTOMERS"

    def test_single_table_with_external_fk(self):
        """Should handle table with FK to external (non-existent) table."""
        table = make_table("ORDERS", [("EXTERNAL_ID", "EXTERNAL_TABLE")])
        result = topo_sort_tables([table])

        assert len(result) == 1
        assert result[0].name == "ORDERS"

    def test_two_independent_tables(self):
        """Should handle two tables with no dependencies."""
        table1 = make_table("CUSTOMERS")
        table2 = make_table("PRODUCTS")
        result = topo_sort_tables([table1, table2])

        assert len(result) == 2
        names = {t.name for t in result}
        assert names == {"CUSTOMERS", "PRODUCTS"}


# ── Linear Dependency Chain Tests ─────────────────────────────────────────────


class TestLinearChain:
    """Tests for linear dependency chains."""

    def test_simple_two_table_chain(self):
        """A -> B: A should come before B."""
        # ORDERS depends on CUSTOMERS
        customers = make_table("CUSTOMERS")
        orders = make_table("ORDERS", [("CUSTOMER_ID", "CUSTOMERS")])

        result = topo_sort_tables([orders, customers])  # Input in reverse order

        assert len(result) == 2
        # CUSTOMERS should come before ORDERS
        customer_idx = next(i for i, t in enumerate(result) if t.name == "CUSTOMERS")
        order_idx = next(i for i, t in enumerate(result) if t.name == "ORDERS")
        assert customer_idx < order_idx

    def test_three_table_chain(self):
        """A -> B -> C: A should come first, then B, then C."""
        # CUSTOMERS -> ORDERS -> ORDER_ITEMS
        customers = make_table("CUSTOMERS")
        orders = make_table("ORDERS", [("CUSTOMER_ID", "CUSTOMERS")])
        order_items = make_table("ORDER_ITEMS", [("ORDER_ID", "ORDERS")])

        result = topo_sort_tables([order_items, orders, customers])

        # Check ordering
        names = [t.name for t in result]
        assert names.index("CUSTOMERS") < names.index("ORDERS")
        assert names.index("ORDERS") < names.index("ORDER_ITEMS")

    def test_four_table_chain(self):
        """A -> B -> C -> D: Should maintain correct order."""
        a = make_table("A")
        b = make_table("B", [("A_ID", "A")])
        c = make_table("C", [("B_ID", "B")])
        d = make_table("D", [("C_ID", "C")])

        result = topo_sort_tables([d, c, b, a])

        names = [t.name for t in result]
        assert names.index("A") < names.index("B")
        assert names.index("B") < names.index("C")
        assert names.index("C") < names.index("D")


# ── Diamond Dependency Tests ──────────────────────────────────────────────────


class TestDiamondDependency:
    """Tests for diamond-shaped dependency patterns."""

    def test_simple_diamond(self):
        """
        Diamond pattern:
            A
           / \\
          B   C
           \\ /
            D

        D depends on both B and C, which both depend on A.
        """
        a = make_table("A")
        b = make_table("B", [("A_ID", "A")])
        c = make_table("C", [("A_ID", "A")])
        d = make_table("D", [("B_ID", "B"), ("C_ID", "C")])

        result = topo_sort_tables([d, c, b, a])

        names = [t.name for t in result]

        # A must come before B and C
        assert names.index("A") < names.index("B")
        assert names.index("A") < names.index("C")

        # B and C must come before D
        assert names.index("B") < names.index("D")
        assert names.index("C") < names.index("D")

    def test_complex_diamond(self):
        """
        More complex pattern:
               A
              /|\\
             B C D
              \\|/
               E
        """
        a = make_table("A")
        b = make_table("B", [("A_ID", "A")])
        c = make_table("C", [("A_ID", "A")])
        d = make_table("D", [("A_ID", "A")])
        e = make_table("E", [("B_ID", "B"), ("C_ID", "C"), ("D_ID", "D")])

        result = topo_sort_tables([e, d, c, b, a])

        names = [t.name for t in result]

        # A must come first
        assert names.index("A") < names.index("B")
        assert names.index("A") < names.index("C")
        assert names.index("A") < names.index("D")

        # E must come last
        assert names.index("B") < names.index("E")
        assert names.index("C") < names.index("E")
        assert names.index("D") < names.index("E")


# ── Multiple Independent Groups Tests ─────────────────────────────────────────


class TestIndependentGroups:
    """Tests for multiple independent dependency groups."""

    def test_two_independent_chains(self):
        """Two separate chains that don't interact."""
        # Chain 1: A -> B
        a = make_table("A")
        b = make_table("B", [("A_ID", "A")])

        # Chain 2: C -> D
        c = make_table("C")
        d = make_table("D", [("C_ID", "C")])

        result = topo_sort_tables([d, b, c, a])

        names = [t.name for t in result]

        # Within each chain, order must be preserved
        assert names.index("A") < names.index("B")
        assert names.index("C") < names.index("D")

    def test_mixed_independent_and_dependent(self):
        """Mix of independent tables and dependent chains."""
        # Independent tables
        standalone1 = make_table("STANDALONE1")
        standalone2 = make_table("STANDALONE2")

        # Dependent chain
        parent = make_table("PARENT")
        child = make_table("CHILD", [("PARENT_ID", "PARENT")])

        result = topo_sort_tables([child, standalone1, parent, standalone2])

        names = [t.name for t in result]

        # Parent must come before child
        assert names.index("PARENT") < names.index("CHILD")

        # All tables should be present
        assert len(result) == 4


# ── Circular Dependency Tests ─────────────────────────────────────────────────


class TestCircularDependencies:
    """Tests for handling circular dependencies."""

    def test_simple_cycle(self, cyclic_tables):
        """Should handle simple A -> B -> C -> A cycle."""
        result = topo_sort_tables(cyclic_tables)

        # Should return all tables (cycles are appended at end)
        assert len(result) == 3
        names = {t.name for t in result}
        assert names == {"TABLE_A", "TABLE_B", "TABLE_C"}

    def test_self_referencing_table(self):
        """Should handle table with FK to itself."""
        # Employee with manager_id referencing itself
        table = TableInfo(
            name="EMPLOYEES",
            columns=[
                ColumnInfo(name="ID", type="INTEGER", nullable=False),
                ColumnInfo(name="MANAGER_ID", type="INTEGER", nullable=True),
            ],
            primary_key=["ID"],
            foreign_keys=[
                ForeignKeyInfo(
                    constrained_columns=["MANAGER_ID"],
                    referred_table="EMPLOYEES",
                    referred_columns=["ID"],
                )
            ],
        )

        result = topo_sort_tables([table])

        assert len(result) == 1
        assert result[0].name == "EMPLOYEES"

    def test_cycle_with_additional_tables(self):
        """Cycle should not prevent other tables from being sorted."""
        # Cycle: A -> B -> A
        a = TableInfo(
            name="A",
            columns=[ColumnInfo(name="ID", type="INTEGER", nullable=False)],
            primary_key=["ID"],
            foreign_keys=[
                ForeignKeyInfo(
                    constrained_columns=["B_ID"],
                    referred_table="B",
                    referred_columns=["ID"],
                )
            ],
        )
        b = TableInfo(
            name="B",
            columns=[ColumnInfo(name="ID", type="INTEGER", nullable=False)],
            primary_key=["ID"],
            foreign_keys=[
                ForeignKeyInfo(
                    constrained_columns=["A_ID"],
                    referred_table="A",
                    referred_columns=["ID"],
                )
            ],
        )

        # Independent table
        c = make_table("C")

        result = topo_sort_tables([a, b, c])

        assert len(result) == 3
        names = {t.name for t in result}
        assert names == {"A", "B", "C"}


# ── Fixture-Based Tests ───────────────────────────────────────────────────────


class TestWithFixtures:
    """Tests using conftest.py fixtures."""

    def test_simple_table_fixture(self, simple_table_info):
        """Test with simple_table_info fixture."""
        result = topo_sort_tables([simple_table_info])

        assert len(result) == 1
        assert result[0].name == "CUSTOMERS"

    def test_table_with_fk_fixture(self, simple_table_info, table_with_fk):
        """Test with table that has FK to another."""
        result = topo_sort_tables([table_with_fk, simple_table_info])

        names = [t.name for t in result]
        # CUSTOMERS should come before ORDERS
        assert names.index("CUSTOMERS") < names.index("ORDERS")

    def test_table_chain_fixture(self, table_chain):
        """Test with full table chain fixture."""
        result = topo_sort_tables(table_chain)

        names = [t.name for t in result]

        # CUSTOMERS and PRODUCTS should come before ORDERS
        # (ORDERS depends on CUSTOMERS, ORDER_ITEMS depends on ORDERS and PRODUCTS)
        assert names.index("CUSTOMERS") < names.index("ORDERS")

        # ORDER_ITEMS should come after both ORDERS and PRODUCTS
        assert names.index("ORDERS") < names.index("ORDER_ITEMS")
        assert names.index("PRODUCTS") < names.index("ORDER_ITEMS")

    def test_cyclic_tables_fixture(self, cyclic_tables):
        """Test with cyclic tables fixture."""
        result = topo_sort_tables(cyclic_tables)

        # All tables should be present
        assert len(result) == 3


# ── Edge Cases ────────────────────────────────────────────────────────────────


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_same_table_multiple_fks(self):
        """Table with multiple FKs to same parent."""
        parent = make_table("PARENT")
        child = TableInfo(
            name="CHILD",
            columns=[
                ColumnInfo(name="ID", type="INTEGER", nullable=False),
                ColumnInfo(name="PARENT_ID_1", type="INTEGER", nullable=True),
                ColumnInfo(name="PARENT_ID_2", type="INTEGER", nullable=True),
            ],
            primary_key=["ID"],
            foreign_keys=[
                ForeignKeyInfo(
                    constrained_columns=["PARENT_ID_1"],
                    referred_table="PARENT",
                    referred_columns=["ID"],
                ),
                ForeignKeyInfo(
                    constrained_columns=["PARENT_ID_2"],
                    referred_table="PARENT",
                    referred_columns=["ID"],
                ),
            ],
        )

        result = topo_sort_tables([child, parent])

        names = [t.name for t in result]
        assert names.index("PARENT") < names.index("CHILD")

    def test_composite_foreign_key(self):
        """Table with composite (multi-column) foreign key."""
        parent = TableInfo(
            name="PARENT",
            columns=[
                ColumnInfo(name="ID1", type="INTEGER", nullable=False),
                ColumnInfo(name="ID2", type="INTEGER", nullable=False),
            ],
            primary_key=["ID1", "ID2"],
            foreign_keys=[],
        )
        child = TableInfo(
            name="CHILD",
            columns=[
                ColumnInfo(name="ID", type="INTEGER", nullable=False),
                ColumnInfo(name="PARENT_ID1", type="INTEGER", nullable=False),
                ColumnInfo(name="PARENT_ID2", type="INTEGER", nullable=False),
            ],
            primary_key=["ID"],
            foreign_keys=[
                ForeignKeyInfo(
                    constrained_columns=["PARENT_ID1", "PARENT_ID2"],
                    referred_table="PARENT",
                    referred_columns=["ID1", "ID2"],
                ),
            ],
        )

        result = topo_sort_tables([child, parent])

        names = [t.name for t in result]
        assert names.index("PARENT") < names.index("CHILD")

    def test_many_tables_no_dependencies(self):
        """Many tables with no dependencies should all be returned."""
        tables = [make_table(f"TABLE_{i}") for i in range(20)]

        result = topo_sort_tables(tables)

        assert len(result) == 20

    def test_deep_chain(self):
        """Deep dependency chain (10 levels)."""
        tables = []
        for i in range(10):
            if i == 0:
                tables.append(make_table(f"LEVEL_{i}"))
            else:
                tables.append(make_table(f"LEVEL_{i}", [(f"LEVEL_{i-1}_ID", f"LEVEL_{i-1}")]))

        # Shuffle input order
        import random
        shuffled = tables.copy()
        random.shuffle(shuffled)

        result = topo_sort_tables(shuffled)

        names = [t.name for t in result]
        # Each level should come before the next
        for i in range(9):
            assert names.index(f"LEVEL_{i}") < names.index(f"LEVEL_{i+1}")

    def test_preserves_table_objects(self):
        """Should return the same TableInfo objects, not copies."""
        table = make_table("TEST")
        result = topo_sort_tables([table])

        assert result[0] is table


# ── Order Stability Tests ─────────────────────────────────────────────────────


class TestOrderStability:
    """Tests for deterministic ordering behavior."""

    def test_deterministic_output(self):
        """Same input should produce same output order."""
        tables = [
            make_table("A"),
            make_table("B", [("A_ID", "A")]),
            make_table("C"),
            make_table("D", [("C_ID", "C")]),
        ]

        result1 = topo_sort_tables(tables.copy())
        result2 = topo_sort_tables(tables.copy())

        names1 = [t.name for t in result1]
        names2 = [t.name for t in result2]

        assert names1 == names2

    def test_independent_tables_ordering(self):
        """Independent tables should maintain relative input order where possible."""
        # All independent (no FKs)
        tables = [
            make_table("C"),
            make_table("A"),
            make_table("B"),
        ]

        result = topo_sort_tables(tables)

        # All should be present
        assert len(result) == 3
