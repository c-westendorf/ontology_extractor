"""
Shared pytest fixtures and configuration for rigor_sf tests.

This module provides:
- Common fixtures for database mocking
- Test data generators for TableInfo, ColumnInfo, ForeignKeyInfo
- Temporary directory fixtures
- Sample SQL content fixtures
- Sample CSV data fixtures
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import pytest

from rigor_sf.db_introspect import ColumnInfo, ForeignKeyInfo, TableInfo


# ── Table/Schema Fixtures ─────────────────────────────────────────────────────


@pytest.fixture
def sample_column_info() -> ColumnInfo:
    """A basic column info for testing."""
    return ColumnInfo(
        name="ID",
        type="INTEGER",
        nullable=False,
        comment="Primary key",
    )


@pytest.fixture
def sample_foreign_key_info() -> ForeignKeyInfo:
    """A basic foreign key info for testing."""
    return ForeignKeyInfo(
        constrained_columns=["CUSTOMER_ID"],
        referred_table="CUSTOMERS",
        referred_columns=["ID"],
        confidence=1.0,
        evidence="DDL constraint",
    )


@pytest.fixture
def simple_table_info() -> TableInfo:
    """A single table with no foreign keys."""
    return TableInfo(
        name="CUSTOMERS",
        columns=[
            ColumnInfo(name="ID", type="INTEGER", nullable=False),
            ColumnInfo(name="NAME", type="VARCHAR(100)", nullable=True),
            ColumnInfo(name="EMAIL", type="VARCHAR(255)", nullable=True),
        ],
        primary_key=["ID"],
        foreign_keys=[],
        comment="Customer master table",
    )


@pytest.fixture
def table_with_fk() -> TableInfo:
    """A table with one foreign key."""
    return TableInfo(
        name="ORDERS",
        columns=[
            ColumnInfo(name="ID", type="INTEGER", nullable=False),
            ColumnInfo(name="CUSTOMER_ID", type="INTEGER", nullable=False),
            ColumnInfo(name="ORDER_DATE", type="DATE", nullable=False),
            ColumnInfo(name="TOTAL", type="DECIMAL(10,2)", nullable=True),
        ],
        primary_key=["ID"],
        foreign_keys=[
            ForeignKeyInfo(
                constrained_columns=["CUSTOMER_ID"],
                referred_table="CUSTOMERS",
                referred_columns=["ID"],
            )
        ],
        comment="Order transactions",
    )


@pytest.fixture
def table_chain() -> List[TableInfo]:
    """A chain of related tables: CUSTOMERS -> ORDERS -> ORDER_ITEMS -> PRODUCTS."""
    customers = TableInfo(
        name="CUSTOMERS",
        columns=[
            ColumnInfo(name="ID", type="INTEGER", nullable=False),
            ColumnInfo(name="NAME", type="VARCHAR(100)", nullable=True),
        ],
        primary_key=["ID"],
        foreign_keys=[],
    )
    orders = TableInfo(
        name="ORDERS",
        columns=[
            ColumnInfo(name="ID", type="INTEGER", nullable=False),
            ColumnInfo(name="CUSTOMER_ID", type="INTEGER", nullable=False),
            ColumnInfo(name="ORDER_DATE", type="DATE", nullable=False),
        ],
        primary_key=["ID"],
        foreign_keys=[
            ForeignKeyInfo(
                constrained_columns=["CUSTOMER_ID"],
                referred_table="CUSTOMERS",
                referred_columns=["ID"],
            )
        ],
    )
    products = TableInfo(
        name="PRODUCTS",
        columns=[
            ColumnInfo(name="ID", type="INTEGER", nullable=False),
            ColumnInfo(name="NAME", type="VARCHAR(100)", nullable=True),
            ColumnInfo(name="PRICE", type="DECIMAL(10,2)", nullable=True),
        ],
        primary_key=["ID"],
        foreign_keys=[],
    )
    order_items = TableInfo(
        name="ORDER_ITEMS",
        columns=[
            ColumnInfo(name="ID", type="INTEGER", nullable=False),
            ColumnInfo(name="ORDER_ID", type="INTEGER", nullable=False),
            ColumnInfo(name="PRODUCT_ID", type="INTEGER", nullable=False),
            ColumnInfo(name="QUANTITY", type="INTEGER", nullable=False),
        ],
        primary_key=["ID"],
        foreign_keys=[
            ForeignKeyInfo(
                constrained_columns=["ORDER_ID"],
                referred_table="ORDERS",
                referred_columns=["ID"],
            ),
            ForeignKeyInfo(
                constrained_columns=["PRODUCT_ID"],
                referred_table="PRODUCTS",
                referred_columns=["ID"],
            ),
        ],
    )
    return [customers, orders, products, order_items]


@pytest.fixture
def cyclic_tables() -> List[TableInfo]:
    """Tables with a circular reference: A -> B -> C -> A."""
    table_a = TableInfo(
        name="TABLE_A",
        columns=[
            ColumnInfo(name="ID", type="INTEGER", nullable=False),
            ColumnInfo(name="TABLE_C_ID", type="INTEGER", nullable=True),
        ],
        primary_key=["ID"],
        foreign_keys=[
            ForeignKeyInfo(
                constrained_columns=["TABLE_C_ID"],
                referred_table="TABLE_C",
                referred_columns=["ID"],
            )
        ],
    )
    table_b = TableInfo(
        name="TABLE_B",
        columns=[
            ColumnInfo(name="ID", type="INTEGER", nullable=False),
            ColumnInfo(name="TABLE_A_ID", type="INTEGER", nullable=True),
        ],
        primary_key=["ID"],
        foreign_keys=[
            ForeignKeyInfo(
                constrained_columns=["TABLE_A_ID"],
                referred_table="TABLE_A",
                referred_columns=["ID"],
            )
        ],
    )
    table_c = TableInfo(
        name="TABLE_C",
        columns=[
            ColumnInfo(name="ID", type="INTEGER", nullable=False),
            ColumnInfo(name="TABLE_B_ID", type="INTEGER", nullable=True),
        ],
        primary_key=["ID"],
        foreign_keys=[
            ForeignKeyInfo(
                constrained_columns=["TABLE_B_ID"],
                referred_table="TABLE_B",
                referred_columns=["ID"],
            )
        ],
    )
    return [table_a, table_b, table_c]


# ── SQL Content Fixtures ──────────────────────────────────────────────────────


@pytest.fixture
def sample_sql_simple() -> str:
    """A simple SQL query with one JOIN."""
    return """
    SELECT c.name, o.order_date, o.total
    FROM CUSTOMERS c
    JOIN ORDERS o ON o.CUSTOMER_ID = c.ID
    WHERE o.order_date >= '2024-01-01';
    """


@pytest.fixture
def sample_sql_multi_join() -> str:
    """SQL query with multiple JOINs."""
    return """
    SELECT
        c.name AS customer_name,
        o.order_date,
        p.name AS product_name,
        oi.quantity
    FROM CUSTOMERS c
    JOIN ORDERS o ON o.CUSTOMER_ID = c.ID
    JOIN ORDER_ITEMS oi ON oi.ORDER_ID = o.ID
    JOIN PRODUCTS p ON oi.PRODUCT_ID = p.ID
    WHERE c.ID = 123;
    """


@pytest.fixture
def sample_sql_with_alias() -> str:
    """SQL query using table aliases throughout."""
    return """
    SELECT cust.name, ord.total
    FROM schema.CUSTOMERS AS cust
    INNER JOIN schema.ORDERS AS ord ON ord.CUSTOMER_ID = cust.ID
    LEFT JOIN schema.ADDRESSES addr ON addr.CUSTOMER_ID = cust.ID;
    """


@pytest.fixture
def sample_sql_ambiguous() -> str:
    """SQL query with ambiguous direction (no _ID suffix pattern)."""
    return """
    SELECT a.value, b.value
    FROM TABLE_A a
    JOIN TABLE_B b ON a.REF_CODE = b.REF_CODE;
    """


@pytest.fixture
def sample_sql_with_comments() -> str:
    """SQL query containing comments to strip."""
    return """
    -- This is a line comment
    SELECT c.name, o.total
    FROM CUSTOMERS c
    /* This is a block comment
       spanning multiple lines */
    JOIN ORDERS o ON o.CUSTOMER_ID = c.ID;
    """


# ── Temporary Directory Fixtures ──────────────────────────────────────────────


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sql_worksheet_dir(temp_dir: Path) -> Path:
    """Create a directory with sample SQL worksheets."""
    sql_dir = temp_dir / "sql_worksheets"
    sql_dir.mkdir()

    # Create a few SQL files
    (sql_dir / "customers_orders.sql").write_text(
        """
        SELECT c.name, o.order_date
        FROM CUSTOMERS c
        JOIN ORDERS o ON o.CUSTOMER_ID = c.ID;
        """,
        encoding="utf-8",
    )

    (sql_dir / "order_details.sql").write_text(
        """
        SELECT o.id, p.name, oi.quantity
        FROM ORDERS o
        JOIN ORDER_ITEMS oi ON oi.ORDER_ID = o.ID
        JOIN PRODUCTS p ON oi.PRODUCT_ID = p.ID;
        """,
        encoding="utf-8",
    )

    return sql_dir


@pytest.fixture
def run_dir_with_results(temp_dir: Path) -> Path:
    """Create a run directory structure with sample profiling results."""
    run_dir = temp_dir / "runs" / "2026-03-01_001_test"
    (run_dir / "queries").mkdir(parents=True)
    (run_dir / "results").mkdir(parents=True)
    (run_dir / "artifacts").mkdir(parents=True)

    # Create run_meta.json
    meta = {
        "run_id": "2026-03-01_001_test",
        "generated_at": "2026-03-01T12:00:00Z",
        "generated_by": "test",
        "sql_worksheets_ingested": [],
        "worksheets_hash": "sha256:abc123",
        "candidate_edges_found": 2,
        "ambiguous_direction_edges": 0,
        "sample_limit": 200000,
        "queries_generated": {},
        "downstream_run": None,
        "notes": "",
    }
    (run_dir / "run_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    # Create profiling_edges.csv
    edges_df = pd.DataFrame(
        [
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "sample_rows": 10000,
                "fk_nonnull": 9800,
                "match_count": 9700,
                "match_rate": 0.99,
                "pk_distinct": 5000,
                "pk_total": 5000,
                "pk_unique_rate": 1.0,
                "fk_null_rate": 0.02,
                "confidence_sql": 0.85,
                "frequency": 5,
                "evidence": "customers_orders.sql",
            },
            {
                "from_table": "ORDER_ITEMS",
                "from_column": "ORDER_ID",
                "to_table": "ORDERS",
                "to_column": "ID",
                "sample_rows": 50000,
                "fk_nonnull": 50000,
                "match_count": 49500,
                "match_rate": 0.99,
                "pk_distinct": 10000,
                "pk_total": 10000,
                "pk_unique_rate": 1.0,
                "fk_null_rate": 0.0,
                "confidence_sql": 0.90,
                "frequency": 3,
                "evidence": "order_details.sql",
            },
        ]
    )
    edges_df.to_csv(run_dir / "results" / "profiling_edges.csv", index=False)

    # Create column_profiles.csv
    cols_df = pd.DataFrame(
        [
            {
                "table_name": "ORDERS",
                "column_name": "CUSTOMER_ID",
                "total_rows": 10000,
                "non_null_count": 9800,
                "null_rate": 0.02,
                "distinct_count": 5000,
                "cardinality_ratio": 0.51,
                "min_val": "1",
                "max_val": "9999",
                "inferred_type": "INTEGER",
            },
            {
                "table_name": "CUSTOMERS",
                "column_name": "ID",
                "total_rows": 5000,
                "non_null_count": 5000,
                "null_rate": 0.0,
                "distinct_count": 5000,
                "cardinality_ratio": 1.0,
                "min_val": "1",
                "max_val": "5000",
                "inferred_type": "INTEGER",
            },
        ]
    )
    cols_df.to_csv(run_dir / "results" / "column_profiles.csv", index=False)

    return run_dir


@pytest.fixture
def run_dir_no_results(temp_dir: Path) -> Path:
    """Create a run directory with meta but no result CSVs."""
    run_dir = temp_dir / "runs" / "2026-03-01_002_empty"
    (run_dir / "queries").mkdir(parents=True)
    (run_dir / "results").mkdir(parents=True)
    (run_dir / "artifacts").mkdir(parents=True)

    meta = {
        "run_id": "2026-03-01_002_empty",
        "generated_at": "2026-03-01T12:00:00Z",
        "generated_by": "test",
        "sql_worksheets_ingested": [],
        "worksheets_hash": "sha256:def456",
        "candidate_edges_found": 0,
        "ambiguous_direction_edges": 0,
        "sample_limit": 200000,
        "queries_generated": {},
        "downstream_run": None,
        "notes": "",
    }
    (run_dir / "run_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    return run_dir


# ── Override Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def sample_overrides_data() -> dict:
    """Sample overrides data structure."""
    return {
        "approve": [
            {
                "from": {"table": "ORDERS", "columns": ["CUSTOMER_ID"]},
                "to": {"table": "CUSTOMERS", "columns": ["ID"]},
                "relation": "hasCustomer",
            },
            {
                "from": {"table": "ORDER_ITEMS", "columns": ["PRODUCT_ID"]},
                "to": {"table": "PRODUCTS", "columns": ["ID"]},
            },
        ],
        "reject": [
            {
                "from": {"table": "TEMP_TABLE", "columns": ["REF_ID"]},
                "to": {"table": "OTHER_TABLE", "columns": ["ID"]},
            },
        ],
        "rename": [],
        "table_classification": {
            "CUSTOMERS": "dimension",
            "ORDERS": "fact",
            "PRODUCTS": "dimension",
            "ORDER_ITEMS": "bridge",
        },
    }


@pytest.fixture
def overrides_yaml_file(temp_dir: Path, sample_overrides_data: dict) -> Path:
    """Create an overrides.yaml file with sample data."""
    import yaml

    overrides_path = temp_dir / "golden" / "overrides.yaml"
    overrides_path.parent.mkdir(parents=True, exist_ok=True)
    overrides_path.write_text(yaml.safe_dump(sample_overrides_data, sort_keys=False))
    return overrides_path


# ── DataFrame Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def raw_relationships_df() -> pd.DataFrame:
    """A raw relationships DataFrame for testing merge operations."""
    return pd.DataFrame(
        [
            {
                "from_table": "ORDERS",
                "from_column": "CUSTOMER_ID",
                "to_table": "CUSTOMERS",
                "to_column": "ID",
                "confidence_sql": 0.85,
                "evidence": "join in query",
                "status": "proposed",
                "match_rate": None,
                "pk_unique_rate": None,
                "fk_null_rate": None,
            },
            {
                "from_table": "ORDER_ITEMS",
                "from_column": "ORDER_ID",
                "to_table": "ORDERS",
                "to_column": "ID",
                "confidence_sql": 0.90,
                "evidence": "join in query",
                "status": "proposed",
                "match_rate": None,
                "pk_unique_rate": None,
                "fk_null_rate": None,
            },
        ]
    )


# ── OWL/RDF Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def sample_turtle_fragment() -> str:
    """A sample Turtle fragment for testing OWL merging."""
    return """
    @prefix owl: <http://www.w3.org/2002/07/owl#> .
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    @prefix rigor: <http://example.org/rigor#> .

    rigor:Customer a owl:Class ;
        rdfs:label "Customer" ;
        rdfs:comment "A customer entity" .

    rigor:hasEmail a owl:DatatypeProperty ;
        rdfs:domain rigor:Customer ;
        rdfs:range xsd:string .
    """


@pytest.fixture
def core_ontology_graph():
    """A basic core ontology graph for testing."""
    from rdflib import Graph, Namespace, Literal, URIRef
    from rdflib.namespace import OWL, RDFS, RDF

    g = Graph()
    RIGOR = Namespace("http://example.org/rigor#")
    g.bind("rigor", RIGOR)
    g.bind("owl", OWL)
    g.bind("rdfs", RDFS)

    # Add a base class
    g.add((RIGOR.Entity, RDF.type, OWL.Class))
    g.add((RIGOR.Entity, RDFS.label, Literal("Entity")))

    return g
