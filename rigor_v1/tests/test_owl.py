"""
Tests for rigor_v1/owl.py

Target coverage: 90%

Tests cover:
- merge_fragment function
- Graph creation and parsing
- Triple addition
- Multiple fragment merging
- Invalid Turtle handling
- Empty fragment handling
"""

from __future__ import annotations

import pytest
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import OWL, RDF, RDFS

from rigor_v1.owl import merge_fragment


# ── Basic Merge Tests ─────────────────────────────────────────────────────────


class TestMergeFragment:
    """Tests for merge_fragment function."""

    def test_merge_simple_class(self):
        """Should merge a simple class definition."""
        core = Graph()
        fragment = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:Customer a owl:Class ;
            rdfs:label "Customer" .
        """

        result = merge_fragment(core, fragment)

        # Should have the class triple
        RIGOR = Namespace("http://example.org/rigor#")
        assert (RIGOR.Customer, RDF.type, OWL.Class) in result

    def test_merge_preserves_existing(self):
        """Should preserve existing triples in core graph."""
        core = Graph()
        RIGOR = Namespace("http://example.org/rigor#")
        core.add((RIGOR.Entity, RDF.type, OWL.Class))

        fragment = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:Customer a owl:Class .
        """

        result = merge_fragment(core, fragment)

        # Should have both classes
        assert (RIGOR.Entity, RDF.type, OWL.Class) in result
        assert (RIGOR.Customer, RDF.type, OWL.Class) in result

    def test_merge_object_property(self):
        """Should merge object property definitions."""
        core = Graph()
        fragment = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:hasCustomer a owl:ObjectProperty ;
            rdfs:domain rigor:Order ;
            rdfs:range rigor:Customer .
        """

        result = merge_fragment(core, fragment)

        RIGOR = Namespace("http://example.org/rigor#")
        assert (RIGOR.hasCustomer, RDF.type, OWL.ObjectProperty) in result
        assert (RIGOR.hasCustomer, RDFS.domain, RIGOR.Order) in result
        assert (RIGOR.hasCustomer, RDFS.range, RIGOR.Customer) in result

    def test_merge_datatype_property(self):
        """Should merge datatype property definitions."""
        core = Graph()
        fragment = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:customerName a owl:DatatypeProperty ;
            rdfs:domain rigor:Customer ;
            rdfs:range xsd:string .
        """

        result = merge_fragment(core, fragment)

        RIGOR = Namespace("http://example.org/rigor#")
        assert (RIGOR.customerName, RDF.type, OWL.DatatypeProperty) in result

    def test_merge_multiple_fragments(self):
        """Should correctly merge multiple fragments sequentially."""
        core = Graph()

        fragment1 = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:Customer a owl:Class .
        """

        fragment2 = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:Order a owl:Class .
        """

        fragment3 = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:hasCustomer a owl:ObjectProperty ;
            rdfs:domain rigor:Order ;
            rdfs:range rigor:Customer .
        """

        core = merge_fragment(core, fragment1)
        core = merge_fragment(core, fragment2)
        core = merge_fragment(core, fragment3)

        RIGOR = Namespace("http://example.org/rigor#")
        assert (RIGOR.Customer, RDF.type, OWL.Class) in core
        assert (RIGOR.Order, RDF.type, OWL.Class) in core
        assert (RIGOR.hasCustomer, RDF.type, OWL.ObjectProperty) in core

    def test_merge_returns_same_graph(self):
        """Should return the same graph object (mutated)."""
        core = Graph()
        fragment = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:Test a owl:Class .
        """

        result = merge_fragment(core, fragment)

        assert result is core


# ── Edge Cases ────────────────────────────────────────────────────────────────


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_merge_empty_fragment(self):
        """Should handle empty fragment."""
        core = Graph()
        RIGOR = Namespace("http://example.org/rigor#")
        core.add((RIGOR.Existing, RDF.type, OWL.Class))
        initial_len = len(core)

        fragment = ""

        result = merge_fragment(core, fragment)

        # Should still have original triple
        assert len(result) == initial_len

    def test_merge_whitespace_only_fragment(self):
        """Should handle whitespace-only fragment."""
        core = Graph()
        fragment = "   \n\t\n   "

        result = merge_fragment(core, fragment)

        assert len(result) == 0

    def test_merge_invalid_turtle_raises(self):
        """Should raise error for invalid Turtle syntax."""
        core = Graph()
        fragment = "this is not valid turtle syntax !!!"

        with pytest.raises(Exception):  # rdflib raises various exceptions
            merge_fragment(core, fragment)

    def test_merge_duplicate_triples(self):
        """Should not duplicate triples that already exist."""
        core = Graph()
        RIGOR = Namespace("http://example.org/rigor#")
        core.add((RIGOR.Customer, RDF.type, OWL.Class))
        initial_len = len(core)

        fragment = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:Customer a owl:Class .
        """

        result = merge_fragment(core, fragment)

        # rdflib Graph is a set, so duplicates are not added
        assert len(result) == initial_len

    def test_merge_with_blank_nodes(self):
        """Should handle fragments with blank nodes."""
        core = Graph()
        fragment = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:Customer a owl:Class ;
            rdfs:subClassOf [
                a owl:Restriction ;
                owl:onProperty rigor:hasName ;
                owl:cardinality 1
            ] .
        """

        result = merge_fragment(core, fragment)

        RIGOR = Namespace("http://example.org/rigor#")
        assert (RIGOR.Customer, RDF.type, OWL.Class) in result
        # Should have the restriction
        assert any((None, RDF.type, OWL.Restriction) in result for _ in [1])

    def test_merge_with_literals(self):
        """Should handle literals correctly."""
        core = Graph()
        fragment = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:Customer a owl:Class ;
            rdfs:label "Customer"@en ;
            rdfs:comment "A customer entity"^^xsd:string .
        """

        result = merge_fragment(core, fragment)

        RIGOR = Namespace("http://example.org/rigor#")
        # Check label exists
        labels = list(result.objects(RIGOR.Customer, RDFS.label))
        assert len(labels) >= 1

    def test_merge_unicode_content(self):
        """Should handle Unicode content in fragments."""
        core = Graph()
        fragment = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:客户 a owl:Class ;
            rdfs:label "客户"@zh .
        """

        result = merge_fragment(core, fragment)

        # Should have parsed the Unicode class
        assert len(result) >= 2


# ── Graph Operations Tests ────────────────────────────────────────────────────


class TestGraphOperations:
    """Tests for graph operation correctness."""

    def test_triple_count_increases(self):
        """Triple count should increase after merge."""
        core = Graph()
        initial_len = len(core)

        fragment = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:Customer a owl:Class ;
            rdfs:label "Customer" ;
            rdfs:comment "A customer" .
        """

        result = merge_fragment(core, fragment)

        assert len(result) > initial_len
        assert len(result) >= 3  # At least 3 triples

    def test_serialization_after_merge(self):
        """Should be able to serialize graph after merge."""
        core = Graph()
        fragment = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:Customer a owl:Class .
        """

        result = merge_fragment(core, fragment)

        # Should be able to serialize to different formats
        turtle_output = result.serialize(format="turtle")
        assert "Customer" in turtle_output

        xml_output = result.serialize(format="xml")
        assert "Customer" in xml_output

    def test_query_after_merge(self):
        """Should be able to query graph after merge."""
        core = Graph()
        fragment = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:Customer a owl:Class ;
            rdfs:label "Customer" .
        rigor:Order a owl:Class ;
            rdfs:label "Order" .
        """

        result = merge_fragment(core, fragment)

        # SPARQL query to find all classes
        query = """
        SELECT ?class WHERE {
            ?class a owl:Class .
        }
        """
        results = list(result.query(query))
        assert len(results) >= 2


# ── Integration Tests ─────────────────────────────────────────────────────────


class TestIntegration:
    """Integration tests for realistic scenarios."""

    def test_realistic_ontology_fragment(self):
        """Test with a realistic ontology fragment."""
        core = Graph()

        # Add base ontology metadata
        base_fragment = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor: a owl:Ontology ;
            rdfs:label "RIGOR-SF Ontology" .

        rigor:Entity a owl:Class ;
            rdfs:label "Entity" ;
            rdfs:comment "Base class for all entities" .
        """
        core = merge_fragment(core, base_fragment)

        # Add domain classes
        domain_fragment = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:Customer a owl:Class ;
            rdfs:subClassOf rigor:Entity ;
            rdfs:label "Customer" ;
            rigor:classification "dimension" .

        rigor:Order a owl:Class ;
            rdfs:subClassOf rigor:Entity ;
            rdfs:label "Order" ;
            rigor:classification "fact" .
        """
        core = merge_fragment(core, domain_fragment)

        # Add relationships
        relationships_fragment = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:hasCustomer a owl:ObjectProperty ;
            rdfs:domain rigor:Order ;
            rdfs:range rigor:Customer ;
            rdfs:label "has customer" .
        """
        core = merge_fragment(core, relationships_fragment)

        # Verify complete ontology
        RIGOR = Namespace("http://example.org/rigor#")

        # Check classes
        assert (RIGOR.Entity, RDF.type, OWL.Class) in core
        assert (RIGOR.Customer, RDF.type, OWL.Class) in core
        assert (RIGOR.Order, RDF.type, OWL.Class) in core

        # Check hierarchy
        assert (RIGOR.Customer, RDFS.subClassOf, RIGOR.Entity) in core
        assert (RIGOR.Order, RDFS.subClassOf, RIGOR.Entity) in core

        # Check property
        assert (RIGOR.hasCustomer, RDF.type, OWL.ObjectProperty) in core
        assert (RIGOR.hasCustomer, RDFS.domain, RIGOR.Order) in core
        assert (RIGOR.hasCustomer, RDFS.range, RIGOR.Customer) in core

    def test_merge_with_core_ontology_fixture(self, core_ontology_graph):
        """Test merge with the fixture core ontology."""
        fragment = """
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix rigor: <http://example.org/rigor#> .

        rigor:NewClass a owl:Class ;
            rdfs:subClassOf rigor:Entity ;
            rdfs:label "New Class" .
        """

        result = merge_fragment(core_ontology_graph, fragment)

        RIGOR = Namespace("http://example.org/rigor#")
        assert (RIGOR.Entity, RDF.type, OWL.Class) in result
        assert (RIGOR.NewClass, RDF.type, OWL.Class) in result
        assert (RIGOR.NewClass, RDFS.subClassOf, RIGOR.Entity) in result

    def test_merge_sample_turtle_fixture(self, sample_turtle_fragment):
        """Test merge with the sample turtle fixture."""
        core = Graph()
        result = merge_fragment(core, sample_turtle_fragment)

        RIGOR = Namespace("http://example.org/rigor#")
        assert (RIGOR.Customer, RDF.type, OWL.Class) in result
