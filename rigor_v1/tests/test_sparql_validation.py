"""Tests for sparql_validation.py module."""

import pytest
import tempfile
from pathlib import Path

# Check if rdflib is available
try:
    from rdflib import Graph, Namespace, URIRef
    from rdflib.namespace import OWL, RDF, RDFS
    RDFLIB_AVAILABLE = True
except ImportError:
    RDFLIB_AVAILABLE = False

from rigor_v1.config import ValidationConfig


# Skip all tests if rdflib not available
pytestmark = pytest.mark.skipif(
    not RDFLIB_AVAILABLE,
    reason="rdflib not installed"
)


# Sample ontology for testing
SAMPLE_ONTOLOGY = """<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
         xmlns:owl="http://www.w3.org/2002/07/owl#"
         xmlns:rigor="http://example.org/rigor#">

  <owl:Ontology rdf:about="http://example.org/rigor"/>

  <owl:Class rdf:about="http://example.org/rigor#Customer">
    <rdfs:label>Customer</rdfs:label>
  </owl:Class>

  <owl:Class rdf:about="http://example.org/rigor#Order">
    <rdfs:label>Order</rdfs:label>
  </owl:Class>

  <owl:ObjectProperty rdf:about="http://example.org/rigor#hasCustomer">
    <rdfs:label>has customer</rdfs:label>
    <rdfs:domain rdf:resource="http://example.org/rigor#Order"/>
    <rdfs:range rdf:resource="http://example.org/rigor#Customer"/>
  </owl:ObjectProperty>

  <owl:DatatypeProperty rdf:about="http://example.org/rigor#customerName">
    <rdfs:label>customer name</rdfs:label>
    <rdfs:domain rdf:resource="http://example.org/rigor#Customer"/>
  </owl:DatatypeProperty>

  <owl:DatatypeProperty rdf:about="http://example.org/rigor#orderDate">
    <rdfs:label>order date</rdfs:label>
    <rdfs:domain rdf:resource="http://example.org/rigor#Order"/>
  </owl:DatatypeProperty>

</rdf:RDF>
"""

# Sample ontology with bridge tables for testing
SAMPLE_ONTOLOGY_WITH_BRIDGES = """<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
         xmlns:owl="http://www.w3.org/2002/07/owl#"
         xmlns:rigor="http://example.org/rigor#">

  <owl:Ontology rdf:about="http://example.org/rigor"/>

  <owl:Class rdf:about="http://example.org/rigor#Order">
    <rdfs:label>Order</rdfs:label>
  </owl:Class>

  <owl:Class rdf:about="http://example.org/rigor#Product">
    <rdfs:label>Product</rdfs:label>
  </owl:Class>

  <!-- Valid bridge table with exactly 2 ObjectProperties -->
  <owl:Class rdf:about="http://example.org/rigor#OrderItem">
    <rdfs:label>Order Item</rdfs:label>
    <rigor:classification>bridge</rigor:classification>
  </owl:Class>

  <owl:ObjectProperty rdf:about="http://example.org/rigor#orderItemHasOrder">
    <rdfs:domain rdf:resource="http://example.org/rigor#OrderItem"/>
    <rdfs:range rdf:resource="http://example.org/rigor#Order"/>
  </owl:ObjectProperty>

  <owl:ObjectProperty rdf:about="http://example.org/rigor#orderItemHasProduct">
    <rdfs:domain rdf:resource="http://example.org/rigor#OrderItem"/>
    <rdfs:range rdf:resource="http://example.org/rigor#Product"/>
  </owl:ObjectProperty>

</rdf:RDF>
"""

# Sample ontology with invalid bridge table (only 1 ObjectProperty)
SAMPLE_ONTOLOGY_INVALID_BRIDGE = """<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
         xmlns:owl="http://www.w3.org/2002/07/owl#"
         xmlns:rigor="http://example.org/rigor#">

  <owl:Ontology rdf:about="http://example.org/rigor"/>

  <owl:Class rdf:about="http://example.org/rigor#Order">
    <rdfs:label>Order</rdfs:label>
  </owl:Class>

  <owl:Class rdf:about="http://example.org/rigor#Product">
    <rdfs:label>Product</rdfs:label>
  </owl:Class>

  <!-- Invalid bridge table with only 1 ObjectProperty -->
  <owl:Class rdf:about="http://example.org/rigor#OrderItem">
    <rdfs:label>Order Item</rdfs:label>
    <rigor:classification>bridge</rigor:classification>
  </owl:Class>

  <owl:ObjectProperty rdf:about="http://example.org/rigor#orderItemHasOrder">
    <rdfs:domain rdf:resource="http://example.org/rigor#OrderItem"/>
    <rdfs:range rdf:resource="http://example.org/rigor#Order"/>
  </owl:ObjectProperty>

</rdf:RDF>
"""

# Sample ontology with bridge table having too many ObjectProperties
SAMPLE_ONTOLOGY_BRIDGE_TOO_MANY = """<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
         xmlns:owl="http://www.w3.org/2002/07/owl#"
         xmlns:rigor="http://example.org/rigor#">

  <owl:Ontology rdf:about="http://example.org/rigor"/>

  <owl:Class rdf:about="http://example.org/rigor#Order"/>
  <owl:Class rdf:about="http://example.org/rigor#Product"/>
  <owl:Class rdf:about="http://example.org/rigor#Customer"/>

  <!-- Invalid bridge table with 3 ObjectProperties -->
  <owl:Class rdf:about="http://example.org/rigor#OrderItem">
    <rigor:classification>bridge</rigor:classification>
  </owl:Class>

  <owl:ObjectProperty rdf:about="http://example.org/rigor#prop1">
    <rdfs:domain rdf:resource="http://example.org/rigor#OrderItem"/>
    <rdfs:range rdf:resource="http://example.org/rigor#Order"/>
  </owl:ObjectProperty>

  <owl:ObjectProperty rdf:about="http://example.org/rigor#prop2">
    <rdfs:domain rdf:resource="http://example.org/rigor#OrderItem"/>
    <rdfs:range rdf:resource="http://example.org/rigor#Product"/>
  </owl:ObjectProperty>

  <owl:ObjectProperty rdf:about="http://example.org/rigor#prop3">
    <rdfs:domain rdf:resource="http://example.org/rigor#OrderItem"/>
    <rdfs:range rdf:resource="http://example.org/rigor#Customer"/>
  </owl:ObjectProperty>

</rdf:RDF>
"""


@pytest.fixture
def sample_ontology_file():
    """Create a temporary file with sample ontology."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".owl", delete=False
    ) as f:
        f.write(SAMPLE_ONTOLOGY)
        f.flush()
        yield f.name
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def sample_ontology_with_bridges():
    """Create a temporary file with ontology containing valid bridge tables."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".owl", delete=False
    ) as f:
        f.write(SAMPLE_ONTOLOGY_WITH_BRIDGES)
        f.flush()
        yield f.name
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def sample_ontology_invalid_bridge():
    """Create a temporary file with ontology containing invalid bridge table (too few props)."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".owl", delete=False
    ) as f:
        f.write(SAMPLE_ONTOLOGY_INVALID_BRIDGE)
        f.flush()
        yield f.name
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def sample_ontology_bridge_too_many():
    """Create a temporary file with ontology containing invalid bridge table (too many props)."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".owl", delete=False
    ) as f:
        f.write(SAMPLE_ONTOLOGY_BRIDGE_TOO_MANY)
        f.flush()
        yield f.name
    Path(f.name).unlink(missing_ok=True)


class TestCoverageMetrics:
    """Tests for CoverageMetrics dataclass."""

    def test_table_coverage(self):
        """Calculate table coverage ratio."""
        from rigor_v1.sparql_validation import CoverageMetrics

        metrics = CoverageMetrics(
            table_count=10,
            table_covered=8,
            column_count=50,
            column_covered=40,
        )
        assert metrics.table_coverage == 0.8

    def test_column_coverage(self):
        """Calculate column coverage ratio."""
        from rigor_v1.sparql_validation import CoverageMetrics

        metrics = CoverageMetrics(
            table_count=10,
            table_covered=8,
            column_count=50,
            column_covered=40,
        )
        assert metrics.column_coverage == 0.8

    def test_zero_division(self):
        """Handle zero counts."""
        from rigor_v1.sparql_validation import CoverageMetrics

        metrics = CoverageMetrics(
            table_count=0,
            table_covered=0,
            column_count=0,
            column_covered=0,
        )
        assert metrics.table_coverage == 0.0
        assert metrics.column_coverage == 0.0


class TestValidationIssue:
    """Tests for ValidationIssue dataclass."""

    def test_create_issue(self):
        """Create validation issue."""
        from rigor_v1.sparql_validation import ValidationIssue

        issue = ValidationIssue(
            severity="error",
            category="duplicate_iri",
            message="Duplicate IRI found",
            entity="http://example.org/rigor#Customer",
        )
        assert issue.severity == "error"
        assert issue.category == "duplicate_iri"


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_error_count(self):
        """Count error-level issues."""
        from rigor_v1.sparql_validation import (
            ValidationResult,
            ValidationIssue,
            CoverageMetrics,
        )

        result = ValidationResult(
            coverage=CoverageMetrics(),
            issues=[
                ValidationIssue("error", "cat", "msg1"),
                ValidationIssue("warning", "cat", "msg2"),
                ValidationIssue("error", "cat", "msg3"),
            ],
        )
        assert result.error_count == 2
        assert result.warning_count == 1


class TestSPARQLValidator:
    """Tests for SPARQLValidator class."""

    def test_load_ontology(self, sample_ontology_file):
        """Load ontology from file."""
        from rigor_v1.sparql_validation import SPARQLValidator

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_file)

        assert validator.graph is not None
        assert len(validator.graph) > 0

    def test_compute_coverage(self, sample_ontology_file):
        """Compute coverage metrics."""
        from rigor_v1.sparql_validation import SPARQLValidator

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_file)

        coverage = validator.compute_coverage(
            table_count=5,
            column_count=20,
        )

        # Sample ontology has 2 classes (Customer, Order)
        assert coverage.table_covered == 2
        # Has 3 properties (hasCustomer, customerName, orderDate)
        # But column_covered counts DatatypeProperty + ObjectProperty
        assert coverage.column_covered >= 2

    def test_validate_basic(self, sample_ontology_file):
        """Run basic validation."""
        from rigor_v1.sparql_validation import SPARQLValidator

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_file)

        result = validator.validate(
            table_count=5,
            column_count=20,
        )

        # Sample ontology is valid
        assert result.passed is True
        assert result.error_count == 0

    def test_validate_with_config(self, sample_ontology_file):
        """Validation with custom config."""
        from rigor_v1.sparql_validation import SPARQLValidator

        config = ValidationConfig(
            coverage_warn_threshold=0.50,
            coverage_pass_threshold=0.90,
        )
        validator = SPARQLValidator(
            "http://example.org/rigor#",
            config=config,
        )
        validator.load_ontology(sample_ontology_file)

        # With high threshold, coverage check will fail
        result = validator.validate(table_count=10, column_count=50)

        # 2 classes / 10 tables = 20% < 90%
        assert result.passed is False

    def test_get_query(self):
        """Get formatted SPARQL query."""
        from rigor_v1.sparql_validation import SPARQLValidator

        validator = SPARQLValidator("http://test.org/onto#")
        query = validator.get_query("table_coverage")

        assert "http://test.org/onto#" in query
        assert "SELECT" in query


class TestValidateOntology:
    """Tests for validate_ontology convenience function."""

    def test_validate_file(self, sample_ontology_file):
        """Validate ontology file."""
        from rigor_v1.sparql_validation import validate_ontology

        result = validate_ontology(
            ontology_path=sample_ontology_file,
            base_iri="http://example.org/rigor#",
            table_count=5,
            column_count=20,
        )

        assert result.coverage.table_count == 5
        assert result.coverage.table_covered >= 1


class TestSPARQLQueries:
    """Tests for SPARQL query templates."""

    def test_queries_exist(self):
        """All expected queries are defined."""
        from rigor_v1.sparql_validation import SPARQL_QUERIES

        expected = [
            "table_coverage",
            "column_coverage",
            "relationship_coverage",
            "orphan_classes",
            "duplicate_iris",
            "missing_labels",
            "invalid_domains",
        ]
        for name in expected:
            assert name in SPARQL_QUERIES, f"Missing query: {name}"

    def test_queries_have_base_iri_placeholder(self):
        """All queries use base_iri placeholder."""
        from rigor_v1.sparql_validation import SPARQL_QUERIES

        for name, query in SPARQL_QUERIES.items():
            assert "{base_iri}" in query, f"Query {name} missing base_iri"


# ── SPEC §17.6 Validation Report Tests ────────────────────────────────────────


class TestMissingEdge:
    """Tests for MissingEdge dataclass."""

    def test_create_missing_edge(self):
        """Create a missing edge record."""
        from rigor_v1.sparql_validation import MissingEdge

        edge = MissingEdge(
            from_table="ORDERS",
            to_table="CUSTOMERS",
            reason="ObjectProperty not found",
        )
        assert edge.from_table == "ORDERS"
        assert edge.to_table == "CUSTOMERS"
        assert edge.reason == "ObjectProperty not found"


class TestEdgeCoverage:
    """Tests for EdgeCoverage dataclass."""

    def test_full_coverage(self):
        """Edge coverage with no missing edges."""
        from rigor_v1.sparql_validation import EdgeCoverage

        coverage = EdgeCoverage(
            approved_edges=5,
            covered_edges=5,
            coverage_rate=1.0,
            missing_edges=[],
        )
        assert coverage.approved_edges == 5
        assert coverage.covered_edges == 5
        assert coverage.coverage_rate == 1.0

    def test_partial_coverage(self):
        """Edge coverage with some missing edges."""
        from rigor_v1.sparql_validation import EdgeCoverage, MissingEdge

        coverage = EdgeCoverage(
            approved_edges=10,
            covered_edges=8,
            coverage_rate=0.8,
            missing_edges=[
                MissingEdge("A", "B", "reason1"),
                MissingEdge("C", "D", "reason2"),
            ],
        )
        assert len(coverage.missing_edges) == 2
        assert coverage.coverage_rate == 0.8


class TestRelationMismatch:
    """Tests for RelationMismatch dataclass."""

    def test_create_mismatch(self):
        """Create a relation mismatch record."""
        from rigor_v1.sparql_validation import RelationMismatch

        mismatch = RelationMismatch(
            expected="placedBy",
            actual="hasCustomer",
            edge="ORDERS→CUSTOMERS",
        )
        assert mismatch.expected == "placedBy"
        assert mismatch.actual == "hasCustomer"
        assert "ORDERS" in mismatch.edge


class TestRelationNames:
    """Tests for RelationNames dataclass."""

    def test_all_matched(self):
        """All relation names matched."""
        from rigor_v1.sparql_validation import RelationNames

        result = RelationNames(
            expected=5,
            matched=5,
            mismatches=[],
        )
        assert result.expected == result.matched
        assert len(result.mismatches) == 0


class TestClassificationResult:
    """Tests for ClassificationResult dataclass."""

    def test_full_classification(self):
        """All classes classified."""
        from rigor_v1.sparql_validation import ClassificationResult

        result = ClassificationResult(
            total_classes=10,
            classified=10,
            unclassified=[],
        )
        assert result.total_classes == result.classified
        assert len(result.unclassified) == 0

    def test_partial_classification(self):
        """Some classes unclassified."""
        from rigor_v1.sparql_validation import ClassificationResult

        result = ClassificationResult(
            total_classes=10,
            classified=8,
            unclassified=["TMP_TABLE", "BACKUP_DATA"],
        )
        assert len(result.unclassified) == 2


class TestValidationGates:
    """Tests for ValidationGates dataclass."""

    def test_all_pass(self):
        """All gates pass."""
        from rigor_v1.sparql_validation import ValidationGates

        gates = ValidationGates(
            parse="pass",
            duplicates="pass",
            coverage="pass",
            overall="pass",
        )
        assert gates.overall == "pass"

    def test_coverage_warn(self):
        """Coverage gate can be 'warn'."""
        from rigor_v1.sparql_validation import ValidationGates

        gates = ValidationGates(
            parse="pass",
            duplicates="pass",
            coverage="warn",
            overall="pass",
        )
        assert gates.coverage == "warn"
        assert gates.overall == "pass"

    def test_fail_overall(self):
        """Any failure causes overall fail."""
        from rigor_v1.sparql_validation import ValidationGates

        gates = ValidationGates(
            parse="pass",
            duplicates="fail",
            coverage="pass",
            overall="fail",
        )
        assert gates.overall == "fail"


class TestOWLParseResult:
    """Tests for OWLParseResult dataclass."""

    def test_success(self):
        """Successful parse."""
        from rigor_v1.sparql_validation import OWLParseResult

        result = OWLParseResult(
            success=True,
            triple_count=1523,
        )
        assert result.success is True
        assert result.triple_count == 1523
        assert result.error is None

    def test_failure(self):
        """Failed parse."""
        from rigor_v1.sparql_validation import OWLParseResult

        result = OWLParseResult(
            success=False,
            triple_count=0,
            error="Invalid XML syntax",
        )
        assert result.success is False
        assert result.error is not None


class TestDuplicateIRIs:
    """Tests for DuplicateIRIs dataclass."""

    def test_no_duplicates(self):
        """No duplicate IRIs."""
        from rigor_v1.sparql_validation import DuplicateIRIs

        result = DuplicateIRIs(count=0, duplicates=[])
        assert result.count == 0
        assert len(result.duplicates) == 0

    def test_with_duplicates(self):
        """Some duplicate IRIs."""
        from rigor_v1.sparql_validation import DuplicateIRIs

        result = DuplicateIRIs(
            count=2,
            duplicates=["http://example.org/A", "http://example.org/B"],
        )
        assert result.count == 2
        assert len(result.duplicates) == 2


class TestValidationReport:
    """Tests for ValidationReport dataclass."""

    def test_create_full_report(self):
        """Create a complete validation report."""
        from rigor_v1.sparql_validation import (
            ValidationReport,
            OWLParseResult,
            DuplicateIRIs,
            EdgeCoverage,
            RelationNames,
            ClassificationResult,
            BridgeTableValidation,
            ValidationGates,
        )

        report = ValidationReport(
            timestamp="2026-03-01T12:34:56Z",
            owl_parse=OWLParseResult(success=True, triple_count=100),
            duplicate_iris=DuplicateIRIs(count=0, duplicates=[]),
            coverage=EdgeCoverage(
                approved_edges=10,
                covered_edges=10,
                coverage_rate=1.0,
                missing_edges=[],
            ),
            relation_names=RelationNames(expected=5, matched=5, mismatches=[]),
            classifications=ClassificationResult(
                total_classes=8, classified=8, unclassified=[]
            ),
            bridge_tables=BridgeTableValidation(
                total_bridge_classes=2, valid_bridge_classes=2, issues=[]
            ),
            gates=ValidationGates(
                parse="pass",
                duplicates="pass",
                coverage="pass",
                overall="pass",
            ),
        )

        assert report.timestamp == "2026-03-01T12:34:56Z"
        assert report.owl_parse.success is True
        assert report.gates.overall == "pass"
        assert report.bridge_tables.all_valid is True


class TestTableToClassName:
    """Tests for _table_to_class_name helper."""

    def test_singularize_s(self):
        """Singularize table ending in S."""
        from rigor_v1.sparql_validation import _table_to_class_name

        assert _table_to_class_name("CUSTOMERS") == "Customer"
        assert _table_to_class_name("ORDERS") == "Order"

    def test_singularize_ies(self):
        """Singularize table ending in IES."""
        from rigor_v1.sparql_validation import _table_to_class_name

        assert _table_to_class_name("CATEGORIES") == "Category"
        assert _table_to_class_name("ENTRIES") == "Entry"

    def test_singularize_es(self):
        """Singularize table ending in ES."""
        from rigor_v1.sparql_validation import _table_to_class_name

        assert _table_to_class_name("BOXES") == "Box"

    def test_preserve_ss(self):
        """Don't singularize words ending in SS."""
        from rigor_v1.sparql_validation import _table_to_class_name

        # ADDRESS stays as ADDRESS (not ADDRES)
        assert _table_to_class_name("ADDRESS") == "Address"

    def test_underscore_to_pascal(self):
        """Convert underscores to PascalCase."""
        from rigor_v1.sparql_validation import _table_to_class_name

        assert _table_to_class_name("ORDER_ITEMS") == "OrderItem"
        assert _table_to_class_name("CUSTOMER_ADDRESSES") == "CustomerAddress"


class TestComputeEdgeCoverage:
    """Tests for compute_edge_coverage function."""

    def test_empty_edges(self, sample_ontology_file):
        """Coverage with no approved edges."""
        from rigor_v1.sparql_validation import (
            SPARQLValidator,
            compute_edge_coverage,
        )

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_file)

        coverage = compute_edge_coverage(validator, [])
        assert coverage.approved_edges == 0
        assert coverage.covered_edges == 0
        assert coverage.coverage_rate == 1.0

    def test_covered_edge(self, sample_ontology_file):
        """Coverage when edge exists in ontology."""
        from rigor_v1.sparql_validation import (
            SPARQLValidator,
            compute_edge_coverage,
        )

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_file)

        # Sample ontology has Order->Customer via hasCustomer
        approved = [("ORDERS", "CUSTOMERS")]
        coverage = compute_edge_coverage(validator, approved)

        assert coverage.approved_edges == 1
        assert coverage.covered_edges == 1
        assert coverage.coverage_rate == 1.0

    def test_missing_edge(self, sample_ontology_file):
        """Coverage when edge is missing from ontology."""
        from rigor_v1.sparql_validation import (
            SPARQLValidator,
            compute_edge_coverage,
        )

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_file)

        # This edge doesn't exist in sample ontology
        approved = [("PRODUCTS", "CATEGORIES")]
        coverage = compute_edge_coverage(validator, approved)

        assert coverage.approved_edges == 1
        assert coverage.covered_edges == 0
        assert coverage.coverage_rate == 0.0
        assert len(coverage.missing_edges) == 1


class TestCheckRelationNames:
    """Tests for check_relation_names function."""

    def test_empty_overrides(self, sample_ontology_file):
        """No relation name overrides."""
        from rigor_v1.sparql_validation import (
            SPARQLValidator,
            check_relation_names,
        )

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_file)

        result = check_relation_names(validator, {})
        assert result.expected == 0
        assert result.matched == 0

    def test_matching_name(self, sample_ontology_file):
        """Relation name matches override."""
        from rigor_v1.sparql_validation import (
            SPARQLValidator,
            check_relation_names,
        )

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_file)

        overrides = {("ORDERS", "CUSTOMERS"): "hasCustomer"}
        result = check_relation_names(validator, overrides)

        assert result.expected == 1
        assert result.matched == 1
        assert len(result.mismatches) == 0


class TestCheckClassifications:
    """Tests for check_classifications function."""

    def test_check_classifications(self, sample_ontology_file):
        """Check classification coverage."""
        from rigor_v1.sparql_validation import (
            SPARQLValidator,
            check_classifications,
        )

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_file)

        result = check_classifications(validator)

        # Sample ontology has 2 classes without classification annotations
        assert result.total_classes == 2
        assert result.classified == 0  # No classification annotations in sample


class TestBuildValidationReport:
    """Tests for build_validation_report function."""

    def test_successful_report(self, sample_ontology_file):
        """Build a successful validation report."""
        from rigor_v1.sparql_validation import build_validation_report

        report = build_validation_report(
            ontology_path=sample_ontology_file,
            base_iri="http://example.org/rigor#",
            approved_edges=[("ORDERS", "CUSTOMERS")],
        )

        assert report.owl_parse.success is True
        assert report.owl_parse.triple_count > 0
        assert report.duplicate_iris.count == 0
        assert report.gates.parse == "pass"
        assert "T" in report.timestamp  # ISO format

    def test_invalid_ontology_path(self):
        """Report for non-existent ontology file."""
        from rigor_v1.sparql_validation import build_validation_report

        report = build_validation_report(
            ontology_path="/nonexistent/file.owl",
            base_iri="http://example.org/rigor#",
        )

        assert report.owl_parse.success is False
        assert report.owl_parse.error is not None
        assert report.gates.parse == "fail"
        assert report.gates.overall == "fail"

    def test_with_config_thresholds(self, sample_ontology_file):
        """Report respects config thresholds."""
        from rigor_v1.sparql_validation import build_validation_report

        config = ValidationConfig(
            coverage_warn_threshold=0.50,
            coverage_pass_threshold=0.90,
        )

        # Approve many edges that don't exist
        approved_edges = [
            ("A", "B"),
            ("C", "D"),
            ("E", "F"),
            ("G", "H"),
            ("I", "J"),
        ]

        report = build_validation_report(
            ontology_path=sample_ontology_file,
            base_iri="http://example.org/rigor#",
            config=config,
            approved_edges=approved_edges,
        )

        # Coverage rate is 0% (none of these edges exist)
        assert report.coverage.coverage_rate == 0.0
        assert report.gates.coverage == "fail"
        assert report.gates.overall == "fail"

    def test_json_serializable(self, sample_ontology_file):
        """Report can be serialized to JSON."""
        import json
        from dataclasses import asdict
        from rigor_v1.sparql_validation import build_validation_report

        report = build_validation_report(
            ontology_path=sample_ontology_file,
            base_iri="http://example.org/rigor#",
        )

        # Should not raise
        json_str = json.dumps(asdict(report), indent=2)
        assert "timestamp" in json_str
        assert "owl_parse" in json_str
        assert "gates" in json_str


class TestSPARQLValidatorNewMethods:
    """Tests for new SPARQLValidator methods."""

    def test_get_triple_count(self, sample_ontology_file):
        """Get triple count from loaded ontology."""
        from rigor_v1.sparql_validation import SPARQLValidator

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_file)

        count = validator.get_triple_count()
        assert count > 0

    def test_get_triple_count_no_graph(self):
        """Triple count with no graph loaded."""
        from rigor_v1.sparql_validation import SPARQLValidator

        validator = SPARQLValidator("http://example.org/rigor#")
        count = validator.get_triple_count()
        assert count == 0

    def test_get_duplicate_iris(self, sample_ontology_file):
        """Get duplicate IRIs (should be none in sample)."""
        from rigor_v1.sparql_validation import SPARQLValidator

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_file)

        dupes = validator.get_duplicate_iris()
        assert dupes.count == 0
        assert len(dupes.duplicates) == 0

    def test_get_object_properties(self, sample_ontology_file):
        """Get object properties with domain/range."""
        from rigor_v1.sparql_validation import SPARQLValidator

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_file)

        props = validator.get_object_properties()
        # Sample has hasCustomer: Order -> Customer
        assert len(props) >= 1
        prop_names = [p[0] for p in props]
        assert "hasCustomer" in prop_names

    def test_get_all_classes(self, sample_ontology_file):
        """Get all class names."""
        from rigor_v1.sparql_validation import SPARQLValidator

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_file)

        classes = validator.get_all_classes()
        assert "Customer" in classes
        assert "Order" in classes

    def test_get_classified_classes(self, sample_ontology_file):
        """Get classified classes (empty in sample)."""
        from rigor_v1.sparql_validation import SPARQLValidator

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_file)

        classified = validator.get_classified_classes()
        # Sample ontology has no classification annotations
        assert len(classified) == 0


class TestBridgeTableValidation:
    """Tests for bridge table validation."""

    def test_bridge_table_dataclasses(self):
        """Test BridgeTableIssue and BridgeTableValidation dataclasses."""
        from rigor_v1.sparql_validation import BridgeTableIssue, BridgeTableValidation

        issue = BridgeTableIssue(
            class_name="OrderItem",
            expected_properties=2,
            actual_properties=1,
            reason="Too few ObjectProperties",
        )
        assert issue.class_name == "OrderItem"
        assert issue.expected_properties == 2
        assert issue.actual_properties == 1

        validation = BridgeTableValidation(
            total_bridge_classes=3,
            valid_bridge_classes=2,
            issues=[issue],
        )
        assert validation.total_bridge_classes == 3
        assert validation.valid_bridge_classes == 2
        assert validation.all_valid is False

    def test_bridge_table_all_valid(self):
        """Test all_valid property when no issues."""
        from rigor_v1.sparql_validation import BridgeTableValidation

        validation = BridgeTableValidation(
            total_bridge_classes=2,
            valid_bridge_classes=2,
            issues=[],
        )
        assert validation.all_valid is True

    def test_valid_bridge_table(self, sample_ontology_with_bridges):
        """Valid bridge table with exactly 2 ObjectProperties."""
        from rigor_v1.sparql_validation import SPARQLValidator, validate_bridge_tables

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_with_bridges)

        result = validate_bridge_tables(validator)

        assert result.total_bridge_classes == 1
        assert result.valid_bridge_classes == 1
        assert result.all_valid is True
        assert len(result.issues) == 0

    def test_invalid_bridge_too_few_properties(self, sample_ontology_invalid_bridge):
        """Invalid bridge table with only 1 ObjectProperty."""
        from rigor_v1.sparql_validation import SPARQLValidator, validate_bridge_tables

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_invalid_bridge)

        result = validate_bridge_tables(validator)

        assert result.total_bridge_classes == 1
        assert result.valid_bridge_classes == 0
        assert result.all_valid is False
        assert len(result.issues) == 1

        issue = result.issues[0]
        assert issue.class_name == "OrderItem"
        assert issue.expected_properties == 2
        assert issue.actual_properties == 1
        assert "Too few" in issue.reason

    def test_invalid_bridge_too_many_properties(self, sample_ontology_bridge_too_many):
        """Invalid bridge table with 3 ObjectProperties."""
        from rigor_v1.sparql_validation import SPARQLValidator, validate_bridge_tables

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_bridge_too_many)

        result = validate_bridge_tables(validator)

        assert result.total_bridge_classes == 1
        assert result.valid_bridge_classes == 0
        assert result.all_valid is False
        assert len(result.issues) == 1

        issue = result.issues[0]
        assert issue.class_name == "OrderItem"
        assert issue.expected_properties == 2
        assert issue.actual_properties == 3
        assert "Too many" in issue.reason

    def test_no_bridge_tables(self, sample_ontology_file):
        """Ontology with no bridge tables."""
        from rigor_v1.sparql_validation import SPARQLValidator, validate_bridge_tables

        validator = SPARQLValidator("http://example.org/rigor#")
        validator.load_ontology(sample_ontology_file)

        result = validate_bridge_tables(validator)

        assert result.total_bridge_classes == 0
        assert result.valid_bridge_classes == 0
        assert result.all_valid is True  # No issues means valid

    def test_sparql_query_exists(self):
        """Verify SPARQL query for bridge tables exists."""
        from rigor_v1.sparql_validation import SPARQL_QUERIES

        assert "bridge_classes_with_property_count" in SPARQL_QUERIES
        query = SPARQL_QUERIES["bridge_classes_with_property_count"]
        assert "rigor:classification" in query
        assert '"bridge"' in query

    def test_validation_report_includes_bridge_tables(self, sample_ontology_with_bridges):
        """ValidationReport includes bridge_tables field."""
        from rigor_v1.sparql_validation import build_validation_report

        report = build_validation_report(
            ontology_path=sample_ontology_with_bridges,
            base_iri="http://example.org/rigor#",
        )

        assert hasattr(report, "bridge_tables")
        assert report.bridge_tables.total_bridge_classes == 1
        assert report.bridge_tables.valid_bridge_classes == 1

    def test_validation_report_parse_failure_has_empty_bridge_tables(self):
        """Validation report on parse failure has empty bridge_tables."""
        from rigor_v1.sparql_validation import build_validation_report

        # Create invalid ontology file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".owl", delete=False
        ) as f:
            f.write("not valid XML")
            f.flush()
            invalid_path = f.name

        try:
            report = build_validation_report(
                ontology_path=invalid_path,
                base_iri="http://example.org/rigor#",
            )

            assert report.owl_parse.success is False
            assert report.bridge_tables.total_bridge_classes == 0
            assert report.bridge_tables.valid_bridge_classes == 0
        finally:
            Path(invalid_path).unlink(missing_ok=True)
