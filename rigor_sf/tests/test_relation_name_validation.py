"""Tests for relation name validation during generate phase.

Tests verify:
- Relation names can be extracted from TTL content
- Generated relation names are validated against overrides
- Mismatches are detected and reported
"""

from __future__ import annotations

import pytest

from rigor_sf.pipeline import (
    _extract_relation_names_from_ttl,
    _validate_relation_names,
    _load_overrides_relation_names,
)


# ── Test TTL Content ──────────────────────────────────────────────────────────

SAMPLE_TTL_CONTENT = """
@prefix : <http://example.org/rigor#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

:Order a owl:Class ;
    rdfs:label "Order" .

:Customer a owl:Class ;
    rdfs:label "Customer" .

:hasCustomer a owl:ObjectProperty ;
    rdfs:domain :Order ;
    rdfs:range :Customer ;
    rdfs:label "has customer" .

:orderId a owl:DatatypeProperty ;
    rdfs:domain :Order ;
    rdfs:range xsd:string .
"""

SAMPLE_TTL_MULTIPLE_RELATIONS = """
@prefix : <http://example.org/rigor#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

:Order a owl:Class .
:Customer a owl:Class .
:Product a owl:Class .
:OrderItem a owl:Class .

:hasCustomer a owl:ObjectProperty ;
    rdfs:domain :Order ;
    rdfs:range :Customer .

:hasProduct a owl:ObjectProperty ;
    rdfs:domain :OrderItem ;
    rdfs:range :Product .

:belongsToOrder a owl:ObjectProperty ;
    rdfs:domain :OrderItem ;
    rdfs:range :Order .
"""


# ── Extract Relation Names Tests ──────────────────────────────────────────────


class TestExtractRelationNames:
    """Tests for _extract_relation_names_from_ttl function."""

    def test_extract_single_relation(self):
        """Should extract a single ObjectProperty with domain and range."""
        base_iri = "http://example.org/rigor#"
        relations = _extract_relation_names_from_ttl(SAMPLE_TTL_CONTENT, base_iri)

        assert len(relations) == 1
        prop_name, domain, range_ = relations[0]
        assert prop_name == "hasCustomer"
        assert domain == "Order"
        assert range_ == "Customer"

    def test_extract_multiple_relations(self):
        """Should extract multiple ObjectProperties."""
        base_iri = "http://example.org/rigor#"
        relations = _extract_relation_names_from_ttl(SAMPLE_TTL_MULTIPLE_RELATIONS, base_iri)

        assert len(relations) == 3

        # Convert to set for easier assertion
        relation_set = {(p, d, r) for p, d, r in relations}
        assert ("hasCustomer", "Order", "Customer") in relation_set
        assert ("hasProduct", "OrderItem", "Product") in relation_set
        assert ("belongsToOrder", "OrderItem", "Order") in relation_set

    def test_extract_handles_empty_content(self):
        """Should return empty list for empty TTL content."""
        base_iri = "http://example.org/rigor#"
        relations = _extract_relation_names_from_ttl("", base_iri)
        assert relations == []

    def test_extract_handles_invalid_ttl(self):
        """Should return empty list for invalid TTL content."""
        base_iri = "http://example.org/rigor#"
        relations = _extract_relation_names_from_ttl("not valid turtle", base_iri)
        assert relations == []

    def test_extract_ignores_datatype_properties(self):
        """Should only extract ObjectProperties, not DatatypeProperties."""
        base_iri = "http://example.org/rigor#"
        relations = _extract_relation_names_from_ttl(SAMPLE_TTL_CONTENT, base_iri)

        # Should only find hasCustomer, not orderId
        assert len(relations) == 1
        assert relations[0][0] == "hasCustomer"

    def test_extract_handles_property_without_domain_range(self):
        """Should skip properties without both domain and range."""
        ttl = """
        @prefix : <http://example.org/rigor#> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

        :partialProp a owl:ObjectProperty ;
            rdfs:domain :SomeClass .
        """
        base_iri = "http://example.org/rigor#"
        relations = _extract_relation_names_from_ttl(ttl, base_iri)
        assert relations == []


# ── Validate Relation Names Tests ─────────────────────────────────────────────


class MockLogger:
    """Mock logger for testing."""

    def __init__(self):
        self.warnings = []

    def warning(self, msg, *args):
        self.warnings.append(msg % args)


class TestValidateRelationNames:
    """Tests for _validate_relation_names function."""

    def test_validate_with_matching_names(self):
        """Should return empty list when names match."""
        expected = {("ORDER", "CUSTOMER"): "hasCustomer"}
        log = MockLogger()

        mismatches = _validate_relation_names(
            SAMPLE_TTL_CONTENT,
            "ORDER",
            expected,
            "http://example.org/rigor#",
            log,
        )

        assert len(mismatches) == 0
        assert len(log.warnings) == 0

    def test_validate_with_mismatched_names(self):
        """Should detect mismatched relation names."""
        expected = {("ORDER", "CUSTOMER"): "orderedBy"}  # Different name
        log = MockLogger()

        mismatches = _validate_relation_names(
            SAMPLE_TTL_CONTENT,
            "ORDER",
            expected,
            "http://example.org/rigor#",
            log,
        )

        assert len(mismatches) == 1
        edge, expected_name, actual_name = mismatches[0]
        assert "Order" in edge
        assert "Customer" in edge
        assert expected_name == "orderedBy"
        assert actual_name == "hasCustomer"
        assert len(log.warnings) == 1

    def test_validate_case_insensitive(self):
        """Should compare names case-insensitively."""
        expected = {("ORDER", "CUSTOMER"): "HasCustomer"}  # Different case
        log = MockLogger()

        mismatches = _validate_relation_names(
            SAMPLE_TTL_CONTENT,
            "ORDER",
            expected,
            "http://example.org/rigor#",
            log,
        )

        assert len(mismatches) == 0

    def test_validate_with_no_expected_relations(self):
        """Should return empty list when no expected relations."""
        expected = {}
        log = MockLogger()

        mismatches = _validate_relation_names(
            SAMPLE_TTL_CONTENT,
            "ORDER",
            expected,
            "http://example.org/rigor#",
            log,
        )

        assert len(mismatches) == 0

    def test_validate_ignores_edges_without_override(self):
        """Should only validate edges that have expected names in overrides."""
        # Only specify expectation for one of three relations
        expected = {("ORDER", "CUSTOMER"): "hasCustomer"}
        log = MockLogger()

        mismatches = _validate_relation_names(
            SAMPLE_TTL_MULTIPLE_RELATIONS,
            "ORDER",
            expected,
            "http://example.org/rigor#",
            log,
        )

        assert len(mismatches) == 0

    def test_validate_multiple_mismatches(self):
        """Should detect multiple mismatches."""
        expected = {
            ("ORDER", "CUSTOMER"): "orderedBy",
            ("ORDERITEM", "PRODUCT"): "contains",
        }
        log = MockLogger()

        mismatches = _validate_relation_names(
            SAMPLE_TTL_MULTIPLE_RELATIONS,
            "ORDER",
            expected,
            "http://example.org/rigor#",
            log,
        )

        assert len(mismatches) == 2
        assert len(log.warnings) == 2


# ── Load Overrides Relation Names Tests ───────────────────────────────────────


class TestLoadOverridesRelationNames:
    """Tests for _load_overrides_relation_names function."""

    def test_load_with_relation_names(self, tmp_path):
        """Should load relation names from overrides."""
        import yaml

        overrides = {
            "approve": [
                {
                    "from": {"table": "ORDERS", "columns": ["CUSTOMER_ID"]},
                    "to": {"table": "CUSTOMERS", "columns": ["ID"]},
                    "relation": "hasCustomer",
                },
                {
                    "from": {"table": "ORDER_ITEMS", "columns": ["PRODUCT_ID"]},
                    "to": {"table": "PRODUCTS", "columns": ["ID"]},
                    "relation": "hasProduct",
                },
            ],
            "reject": [],
            "table_classification": {},
        }

        overrides_path = tmp_path / "overrides.yaml"
        overrides_path.write_text(yaml.safe_dump(overrides))

        # Create a mock config
        class MockCfg:
            class Paths:
                overrides_yaml = str(overrides_path)
            paths = Paths()

        cfg = MockCfg()
        result = _load_overrides_relation_names(cfg)

        assert len(result) == 2
        assert result[("ORDERS", "CUSTOMERS")] == "hasCustomer"
        assert result[("ORDER_ITEMS", "PRODUCTS")] == "hasProduct"

    def test_load_without_relation_names(self, tmp_path):
        """Should handle edges without relation names."""
        import yaml

        overrides = {
            "approve": [
                {
                    "from": {"table": "ORDERS", "columns": ["CUSTOMER_ID"]},
                    "to": {"table": "CUSTOMERS", "columns": ["ID"]},
                    # No relation key
                },
            ],
            "reject": [],
            "table_classification": {},
        }

        overrides_path = tmp_path / "overrides.yaml"
        overrides_path.write_text(yaml.safe_dump(overrides))

        class MockCfg:
            class Paths:
                overrides_yaml = str(overrides_path)
            paths = Paths()

        cfg = MockCfg()
        result = _load_overrides_relation_names(cfg)

        assert len(result) == 0

    def test_load_empty_overrides(self, tmp_path):
        """Should handle empty overrides file."""
        import yaml

        overrides = {"approve": [], "reject": []}

        overrides_path = tmp_path / "overrides.yaml"
        overrides_path.write_text(yaml.safe_dump(overrides))

        class MockCfg:
            class Paths:
                overrides_yaml = str(overrides_path)
            paths = Paths()

        cfg = MockCfg()
        result = _load_overrides_relation_names(cfg)

        assert len(result) == 0

    def test_load_missing_file(self, tmp_path):
        """Should handle missing overrides file."""
        overrides_path = tmp_path / "nonexistent.yaml"

        class MockCfg:
            class Paths:
                overrides_yaml = str(overrides_path)
            paths = Paths()

        cfg = MockCfg()
        result = _load_overrides_relation_names(cfg)

        assert len(result) == 0
