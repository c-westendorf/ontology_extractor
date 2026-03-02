"""SPARQL-based validation for the RIGOR-SF pipeline.

Implements coverage queries and validation checks per SPEC_V2.md §17.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

try:
    from rdflib import Graph, Namespace, URIRef
    from rdflib.namespace import OWL, RDF, RDFS

    RDFLIB_AVAILABLE = True
except ImportError:
    RDFLIB_AVAILABLE = False
    Graph = None

if TYPE_CHECKING:
    from .config import ValidationConfig


# SPARQL query templates per SPEC_V2.md §17
SPARQL_QUERIES = {
    "table_coverage": """
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rigor: <{base_iri}>

        SELECT (COUNT(DISTINCT ?class) AS ?covered)
        WHERE {{
            ?class a owl:Class .
            ?class rdfs:label ?label .
            FILTER(STRSTARTS(STR(?class), STR(rigor:)))
        }}
    """,
    "column_coverage": """
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rigor: <{base_iri}>

        SELECT (COUNT(DISTINCT ?prop) AS ?covered)
        WHERE {{
            {{ ?prop a owl:DatatypeProperty }}
            UNION
            {{ ?prop a owl:ObjectProperty }}
            ?prop rdfs:label ?label .
            FILTER(STRSTARTS(STR(?prop), STR(rigor:)))
        }}
    """,
    "relationship_coverage": """
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rigor: <{base_iri}>

        SELECT (COUNT(DISTINCT ?prop) AS ?relationships)
        WHERE {{
            ?prop a owl:ObjectProperty .
            ?prop rdfs:domain ?domain .
            ?prop rdfs:range ?range .
            FILTER(STRSTARTS(STR(?prop), STR(rigor:)))
        }}
    """,
    "orphan_classes": """
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rigor: <{base_iri}>

        SELECT ?class
        WHERE {{
            ?class a owl:Class .
            FILTER(STRSTARTS(STR(?class), STR(rigor:)))
            FILTER NOT EXISTS {{
                {{ ?prop rdfs:domain ?class }}
                UNION
                {{ ?prop rdfs:range ?class }}
                UNION
                {{ ?class rdfs:subClassOf ?parent }}
            }}
        }}
    """,
    "duplicate_iris": """
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rigor: <{base_iri}>

        SELECT ?iri (COUNT(?iri) AS ?count)
        WHERE {{
            {{ ?iri a owl:Class }}
            UNION
            {{ ?iri a owl:DatatypeProperty }}
            UNION
            {{ ?iri a owl:ObjectProperty }}
            FILTER(STRSTARTS(STR(?iri), STR(rigor:)))
        }}
        GROUP BY ?iri
        HAVING (COUNT(?iri) > 1)
    """,
    "missing_labels": """
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rigor: <{base_iri}>

        SELECT ?entity
        WHERE {{
            {{ ?entity a owl:Class }}
            UNION
            {{ ?entity a owl:DatatypeProperty }}
            UNION
            {{ ?entity a owl:ObjectProperty }}
            FILTER(STRSTARTS(STR(?entity), STR(rigor:)))
            FILTER NOT EXISTS {{ ?entity rdfs:label ?label }}
        }}
    """,
    "invalid_domains": """
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rigor: <{base_iri}>

        SELECT ?prop ?domain
        WHERE {{
            ?prop rdfs:domain ?domain .
            FILTER(STRSTARTS(STR(?prop), STR(rigor:)))
            FILTER NOT EXISTS {{ ?domain a owl:Class }}
        }}
    """,
    # Edge-based coverage queries for SPEC §17.6 validation report
    "object_properties_with_domain_range": """
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rigor: <{base_iri}>

        SELECT ?prop ?domain ?range
        WHERE {{
            ?prop a owl:ObjectProperty .
            ?prop rdfs:domain ?domain .
            ?prop rdfs:range ?range .
            FILTER(STRSTARTS(STR(?prop), STR(rigor:)))
        }}
    """,
    "all_classes": """
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rigor: <{base_iri}>

        SELECT ?class
        WHERE {{
            ?class a owl:Class .
            FILTER(STRSTARTS(STR(?class), STR(rigor:)))
        }}
    """,
    "classes_with_classification": """
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rigor: <{base_iri}>

        SELECT ?class ?classification
        WHERE {{
            ?class a owl:Class .
            ?class rigor:classification ?classification .
            FILTER(STRSTARTS(STR(?class), STR(rigor:)))
        }}
    """,
    # Bridge table validation query per SPEC_V2.md
    # Bridge tables should have exactly 2 outgoing ObjectProperties
    "bridge_classes_with_property_count": """
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rigor: <{base_iri}>

        SELECT ?class (COUNT(DISTINCT ?prop) AS ?prop_count)
        WHERE {{
            ?class a owl:Class .
            ?class rigor:classification "bridge" .
            OPTIONAL {{
                ?prop a owl:ObjectProperty .
                ?prop rdfs:domain ?class .
            }}
            FILTER(STRSTARTS(STR(?class), STR(rigor:)))
        }}
        GROUP BY ?class
    """,
}


@dataclass
class CoverageMetrics:
    """Coverage metrics from SPARQL validation."""

    table_count: int = 0
    table_covered: int = 0
    column_count: int = 0
    column_covered: int = 0
    relationship_count: int = 0

    @property
    def table_coverage(self) -> float:
        """Calculate table coverage ratio."""
        if self.table_count == 0:
            return 0.0
        return self.table_covered / self.table_count

    @property
    def column_coverage(self) -> float:
        """Calculate column coverage ratio."""
        if self.column_count == 0:
            return 0.0
        return self.column_covered / self.column_count


@dataclass
class ValidationIssue:
    """A validation issue found during SPARQL checks."""

    severity: str  # "error" | "warning"
    category: str
    message: str
    entity: str | None = None


@dataclass
class ValidationResult:
    """Result of SPARQL validation."""

    coverage: CoverageMetrics
    issues: list[ValidationIssue] = field(default_factory=list)
    passed: bool = True

    @property
    def error_count(self) -> int:
        """Count of error-level issues."""
        return sum(1 for i in self.issues if i.severity == "error")

    @property
    def warning_count(self) -> int:
        """Count of warning-level issues."""
        return sum(1 for i in self.issues if i.severity == "warning")


# ── SPEC §17.6 Validation Report Schema ───────────────────────────────────────


@dataclass
class MissingEdge:
    """An approved edge not found in the ontology."""

    from_table: str
    to_table: str
    reason: str


@dataclass
class EdgeCoverage:
    """Edge-based coverage metrics per SPEC §17.6."""

    approved_edges: int
    covered_edges: int
    coverage_rate: float
    missing_edges: list[MissingEdge] = field(default_factory=list)


@dataclass
class RelationMismatch:
    """A relation name mismatch between override and ontology."""

    expected: str
    actual: str
    edge: str  # "FROM_TABLE→TO_TABLE"


@dataclass
class RelationNames:
    """Relation name validation results per SPEC §17.6."""

    expected: int
    matched: int
    mismatches: list[RelationMismatch] = field(default_factory=list)


@dataclass
class ClassificationResult:
    """Classification coverage results per SPEC §17.6."""

    total_classes: int
    classified: int
    unclassified: list[str] = field(default_factory=list)


@dataclass
class BridgeTableIssue:
    """A bridge table validation issue."""

    class_name: str
    expected_properties: int
    actual_properties: int
    reason: str


@dataclass
class BridgeTableValidation:
    """Bridge table validation results per SPEC_V2.md.

    Bridge tables (many-to-many junction tables) should have exactly
    2 outgoing ObjectProperties linking to the tables they join.
    """

    total_bridge_classes: int
    valid_bridge_classes: int
    issues: list[BridgeTableIssue] = field(default_factory=list)

    @property
    def all_valid(self) -> bool:
        """Return True if all bridge tables have correct structure."""
        return len(self.issues) == 0


@dataclass
class ValidationGates:
    """Validation gate results per SPEC §17.6."""

    parse: str  # "pass" | "fail"
    duplicates: str  # "pass" | "fail"
    coverage: str  # "pass" | "warn" | "fail"
    overall: str  # "pass" | "fail"


@dataclass
class OWLParseResult:
    """OWL parse result per SPEC §17.6."""

    success: bool
    triple_count: int = 0
    error: str | None = None


@dataclass
class DuplicateIRIs:
    """Duplicate IRI detection result per SPEC §17.6."""

    count: int
    duplicates: list[str] = field(default_factory=list)


@dataclass
class ValidationReport:
    """Full validation report per SPEC §17.6.

    This is the top-level structure for data/validation_report.json.
    """

    timestamp: str
    owl_parse: OWLParseResult
    duplicate_iris: DuplicateIRIs
    coverage: EdgeCoverage
    relation_names: RelationNames
    classifications: ClassificationResult
    bridge_tables: BridgeTableValidation
    gates: ValidationGates


class SPARQLValidator:
    """SPARQL-based ontology validator.

    Performs coverage checks and validation per SPEC_V2.md §17.
    """

    def __init__(self, base_iri: str, config: "ValidationConfig | None" = None):
        """Initialize the validator.

        Args:
            base_iri: Base IRI for the ontology
            config: Optional ValidationConfig for thresholds
        """
        if not RDFLIB_AVAILABLE:
            raise ImportError("rdflib is required for SPARQL validation")

        self.base_iri = base_iri
        self.config = config
        self.graph: Graph | None = None

    def load_ontology(self, source: str, format: str = "xml") -> None:
        """Load an ontology for validation.

        Args:
            source: Path to ontology file or ontology content
            format: RDF format (xml, turtle, n3)
        """
        self.graph = Graph()
        # Try as file path first, then as content
        try:
            self.graph.parse(source, format=format)
        except Exception:
            self.graph.parse(data=source, format=format)

    def _execute_query(self, query_name: str) -> list:
        """Execute a named SPARQL query.

        Args:
            query_name: Name of query from SPARQL_QUERIES

        Returns:
            Query results as list
        """
        if self.graph is None:
            raise ValueError("No ontology loaded")

        query_template = SPARQL_QUERIES.get(query_name)
        if not query_template:
            raise ValueError(f"Unknown query: {query_name}")

        query = query_template.format(base_iri=self.base_iri)
        return list(self.graph.query(query))

    def compute_coverage(
        self, table_count: int, column_count: int
    ) -> CoverageMetrics:
        """Compute coverage metrics.

        Args:
            table_count: Total number of tables in schema
            column_count: Total number of columns in schema

        Returns:
            CoverageMetrics with computed values
        """
        # Get covered counts from ontology
        table_results = self._execute_query("table_coverage")
        table_covered = int(table_results[0][0]) if table_results else 0

        column_results = self._execute_query("column_coverage")
        column_covered = int(column_results[0][0]) if column_results else 0

        rel_results = self._execute_query("relationship_coverage")
        relationship_count = int(rel_results[0][0]) if rel_results else 0

        return CoverageMetrics(
            table_count=table_count,
            table_covered=table_covered,
            column_count=column_count,
            column_covered=column_covered,
            relationship_count=relationship_count,
        )

    def validate(
        self,
        table_count: int,
        column_count: int,
        allow_duplicate_iris: bool = False,
    ) -> ValidationResult:
        """Run full validation suite.

        Args:
            table_count: Total number of tables in schema
            column_count: Total number of columns in schema
            allow_duplicate_iris: If True, duplicate IRIs are warnings not errors

        Returns:
            ValidationResult with coverage and issues
        """
        coverage = self.compute_coverage(table_count, column_count)
        issues: list[ValidationIssue] = []
        passed = True

        # Check coverage thresholds
        if self.config:
            if coverage.table_coverage < self.config.coverage_warn_threshold:
                issues.append(
                    ValidationIssue(
                        severity="warning",
                        category="coverage",
                        message=f"Table coverage {coverage.table_coverage:.1%} below warning threshold {self.config.coverage_warn_threshold:.1%}",
                    )
                )
            if coverage.table_coverage < self.config.coverage_pass_threshold:
                issues.append(
                    ValidationIssue(
                        severity="error",
                        category="coverage",
                        message=f"Table coverage {coverage.table_coverage:.1%} below pass threshold {self.config.coverage_pass_threshold:.1%}",
                    )
                )
                passed = False

        # Check for duplicate IRIs
        duplicates = self._execute_query("duplicate_iris")
        for row in duplicates:
            iri, count = row
            severity = "warning" if allow_duplicate_iris else "error"
            issues.append(
                ValidationIssue(
                    severity=severity,
                    category="duplicate_iri",
                    message=f"Duplicate IRI found {count} times",
                    entity=str(iri),
                )
            )
            if not allow_duplicate_iris:
                passed = False

        # Check for orphan classes
        orphans = self._execute_query("orphan_classes")
        for row in orphans:
            issues.append(
                ValidationIssue(
                    severity="warning",
                    category="orphan_class",
                    message="Class has no properties or parent",
                    entity=str(row[0]),
                )
            )

        # Check for missing labels
        missing_labels = self._execute_query("missing_labels")
        for row in missing_labels:
            issues.append(
                ValidationIssue(
                    severity="warning",
                    category="missing_label",
                    message="Entity missing rdfs:label",
                    entity=str(row[0]),
                )
            )

        # Check for invalid domains
        invalid_domains = self._execute_query("invalid_domains")
        for row in invalid_domains:
            prop, domain = row
            issues.append(
                ValidationIssue(
                    severity="error",
                    category="invalid_domain",
                    message=f"Property domain {domain} is not a declared class",
                    entity=str(prop),
                )
            )
            passed = False

        return ValidationResult(
            coverage=coverage,
            issues=issues,
            passed=passed,
        )

    def get_query(self, query_name: str) -> str:
        """Get a formatted SPARQL query by name.

        Args:
            query_name: Name of query from SPARQL_QUERIES

        Returns:
            Formatted query string
        """
        query_template = SPARQL_QUERIES.get(query_name)
        if not query_template:
            raise ValueError(f"Unknown query: {query_name}")
        return query_template.format(base_iri=self.base_iri)

    def get_triple_count(self) -> int:
        """Get total triple count in loaded ontology."""
        if self.graph is None:
            return 0
        return len(self.graph)

    def get_duplicate_iris(self) -> DuplicateIRIs:
        """Get duplicate IRIs in the ontology."""
        duplicates = self._execute_query("duplicate_iris")
        dup_list = [str(row[0]) for row in duplicates]
        return DuplicateIRIs(count=len(dup_list), duplicates=dup_list)

    def get_object_properties(self) -> list[tuple[str, str, str]]:
        """Get all ObjectProperties with their domain and range.

        Returns:
            List of (property_local_name, domain_local_name, range_local_name)
        """
        results = self._execute_query("object_properties_with_domain_range")
        props = []
        for row in results:
            prop_iri, domain_iri, range_iri = row
            # Extract local names from IRIs
            prop_name = str(prop_iri).replace(self.base_iri, "")
            domain_name = str(domain_iri).replace(self.base_iri, "")
            range_name = str(range_iri).replace(self.base_iri, "")
            props.append((prop_name, domain_name, range_name))
        return props

    def get_all_classes(self) -> list[str]:
        """Get all class local names in the ontology."""
        results = self._execute_query("all_classes")
        return [str(row[0]).replace(self.base_iri, "") for row in results]

    def get_classified_classes(self) -> dict[str, str]:
        """Get classes with their classification annotations.

        Returns:
            Dict mapping class local name to classification value
        """
        results = self._execute_query("classes_with_classification")
        return {
            str(row[0]).replace(self.base_iri, ""): str(row[1])
            for row in results
        }


def validate_ontology(
    ontology_path: str,
    base_iri: str,
    table_count: int,
    column_count: int,
    config: "ValidationConfig | None" = None,
) -> ValidationResult:
    """Convenience function to validate an ontology file.

    Args:
        ontology_path: Path to the ontology file
        base_iri: Base IRI for the ontology
        table_count: Total number of tables in schema
        column_count: Total number of columns in schema
        config: Optional ValidationConfig

    Returns:
        ValidationResult
    """
    validator = SPARQLValidator(base_iri, config)

    # Determine format from extension
    format_map = {".owl": "xml", ".ttl": "turtle", ".n3": "n3", ".xml": "xml"}
    ext = "".join(c for c in ontology_path.lower() if c != " ").split(".")[-1]
    fmt = format_map.get(f".{ext}", "xml")

    validator.load_ontology(ontology_path, format=fmt)

    allow_duplicates = config.allow_duplicate_iris if config else False
    return validator.validate(table_count, column_count, allow_duplicates)


def _table_to_class_name(table_name: str) -> str:
    """Convert table name to expected class name.

    CUSTOMERS -> Customer
    ORDER_ITEMS -> OrderItem
    ADDRESSES -> Address
    """
    # Handle each part of underscore-separated name
    parts = table_name.upper().split("_")
    result_parts = []

    for part in parts:
        # Naive singularization per part
        if part.endswith("IES"):
            part = part[:-3] + "Y"
        elif part.endswith("SSES"):
            # ADDRESSES -> ADDRESS (keep SS)
            part = part[:-2]
        elif part.endswith("XES") or part.endswith("CHES") or part.endswith("SHES"):
            # BOXES -> BOX, BATCHES -> BATCH, DISHES -> DISH
            part = part[:-2]
        elif part.endswith("S") and not part.endswith("SS"):
            part = part[:-1]
        result_parts.append(part.capitalize())

    return "".join(result_parts)


def compute_edge_coverage(
    validator: SPARQLValidator,
    approved_edges: list[tuple[str, str]],
) -> EdgeCoverage:
    """Compute edge-based coverage per SPEC §17.6.

    Args:
        validator: Loaded SPARQLValidator
        approved_edges: List of (from_table, to_table) tuples

    Returns:
        EdgeCoverage with coverage rate and missing edges
    """
    if not approved_edges:
        return EdgeCoverage(
            approved_edges=0,
            covered_edges=0,
            coverage_rate=1.0,
            missing_edges=[],
        )

    # Get all object properties from the ontology
    obj_props = validator.get_object_properties()

    # Build a set of (domain_class, range_class) pairs from ontology
    ontology_edges: set[tuple[str, str]] = set()
    for _prop_name, domain, range_ in obj_props:
        ontology_edges.add((domain.upper(), range_.upper()))

    covered = 0
    missing: list[MissingEdge] = []

    for from_table, to_table in approved_edges:
        # Convert table names to expected class names
        from_class = _table_to_class_name(from_table).upper()
        to_class = _table_to_class_name(to_table).upper()

        # Check if any ObjectProperty connects these classes
        found = False
        for domain, range_ in ontology_edges:
            if domain.upper() == from_class and range_.upper() == to_class:
                found = True
                break

        if found:
            covered += 1
        else:
            missing.append(
                MissingEdge(
                    from_table=from_table,
                    to_table=to_table,
                    reason="ObjectProperty not found",
                )
            )

    coverage_rate = covered / len(approved_edges) if approved_edges else 1.0

    return EdgeCoverage(
        approved_edges=len(approved_edges),
        covered_edges=covered,
        coverage_rate=round(coverage_rate, 3),
        missing_edges=missing,
    )


def check_relation_names(
    validator: SPARQLValidator,
    overrides_relation_names: dict[tuple[str, str], str],
) -> RelationNames:
    """Check if generated relation names match override specifications.

    Args:
        validator: Loaded SPARQLValidator
        overrides_relation_names: Dict mapping (from_table, to_table) to expected relation name

    Returns:
        RelationNames with match counts and mismatches
    """
    if not overrides_relation_names:
        return RelationNames(expected=0, matched=0, mismatches=[])

    # Get object properties from ontology
    obj_props = validator.get_object_properties()

    # Build lookup: (domain_class, range_class) -> property_name
    ontology_relations: dict[tuple[str, str], str] = {}
    for prop_name, domain, range_ in obj_props:
        key = (_table_to_class_name(domain).upper(), _table_to_class_name(range_).upper())
        ontology_relations[key] = prop_name

    matched = 0
    mismatches: list[RelationMismatch] = []

    for (from_table, to_table), expected_name in overrides_relation_names.items():
        from_class = _table_to_class_name(from_table).upper()
        to_class = _table_to_class_name(to_table).upper()

        actual_name = ontology_relations.get((from_class, to_class))

        if actual_name is None:
            # Edge not found in ontology - skip (handled by coverage check)
            continue

        if actual_name.lower() == expected_name.lower():
            matched += 1
        else:
            mismatches.append(
                RelationMismatch(
                    expected=expected_name,
                    actual=actual_name,
                    edge=f"{from_table}→{to_table}",
                )
            )

    return RelationNames(
        expected=len(overrides_relation_names),
        matched=matched,
        mismatches=mismatches,
    )


def check_classifications(
    validator: SPARQLValidator,
    table_classifications: dict[str, str] | None = None,
) -> ClassificationResult:
    """Check classification coverage of ontology classes.

    Args:
        validator: Loaded SPARQLValidator
        table_classifications: Optional dict of table -> classification from overrides

    Returns:
        ClassificationResult with counts and unclassified list
    """
    all_classes = validator.get_all_classes()
    classified_classes = validator.get_classified_classes()

    # Classes without classification annotation
    unclassified = [c for c in all_classes if c not in classified_classes]

    return ClassificationResult(
        total_classes=len(all_classes),
        classified=len(classified_classes),
        unclassified=unclassified,
    )


def validate_bridge_tables(
    validator: SPARQLValidator,
) -> BridgeTableValidation:
    """Validate that bridge tables have exactly 2 outgoing ObjectProperties.

    Per SPEC_V2.md, bridge tables (many-to-many junction tables) should have
    exactly 2 ObjectProperties with the bridge class as domain, linking to
    the two tables they join.

    Args:
        validator: Loaded SPARQLValidator

    Returns:
        BridgeTableValidation with counts and issues
    """
    results = validator._execute_query("bridge_classes_with_property_count")

    total_bridge_classes = 0
    valid_bridge_classes = 0
    issues: list[BridgeTableIssue] = []

    for row in results:
        class_iri, prop_count = row
        class_name = str(class_iri).replace(validator.base_iri, "")
        actual_count = int(prop_count)
        total_bridge_classes += 1

        if actual_count == 2:
            valid_bridge_classes += 1
        else:
            reason = (
                "Too few ObjectProperties" if actual_count < 2
                else "Too many ObjectProperties"
            )
            issues.append(
                BridgeTableIssue(
                    class_name=class_name,
                    expected_properties=2,
                    actual_properties=actual_count,
                    reason=reason,
                )
            )

    return BridgeTableValidation(
        total_bridge_classes=total_bridge_classes,
        valid_bridge_classes=valid_bridge_classes,
        issues=issues,
    )


def build_validation_report(
    ontology_path: str,
    base_iri: str,
    config: "ValidationConfig | None" = None,
    approved_edges: list[tuple[str, str]] | None = None,
    overrides_relation_names: dict[tuple[str, str], str] | None = None,
    table_classifications: dict[str, str] | None = None,
) -> ValidationReport:
    """Build a full SPEC §17.6 validation report.

    Args:
        ontology_path: Path to the ontology file
        base_iri: Base IRI for the ontology
        config: Optional ValidationConfig for thresholds
        approved_edges: List of (from_table, to_table) approved edges
        overrides_relation_names: Dict of (from_table, to_table) -> expected relation name
        table_classifications: Dict of table_name -> classification

    Returns:
        ValidationReport per SPEC §17.6
    """
    from datetime import datetime, timezone

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Determine format from extension
    format_map = {".owl": "xml", ".ttl": "turtle", ".n3": "n3", ".xml": "xml"}
    ext = "".join(c for c in ontology_path.lower() if c != " ").split(".")[-1]
    fmt = format_map.get(f".{ext}", "xml")

    # Try to load and parse the ontology
    validator = SPARQLValidator(base_iri, config)
    try:
        validator.load_ontology(ontology_path, format=fmt)
        owl_parse = OWLParseResult(
            success=True,
            triple_count=validator.get_triple_count(),
        )
    except Exception as e:
        # Parse failed - return early with failure report
        return ValidationReport(
            timestamp=timestamp,
            owl_parse=OWLParseResult(success=False, error=str(e)),
            duplicate_iris=DuplicateIRIs(count=0, duplicates=[]),
            coverage=EdgeCoverage(
                approved_edges=len(approved_edges or []),
                covered_edges=0,
                coverage_rate=0.0,
                missing_edges=[],
            ),
            relation_names=RelationNames(expected=0, matched=0, mismatches=[]),
            classifications=ClassificationResult(
                total_classes=0, classified=0, unclassified=[]
            ),
            bridge_tables=BridgeTableValidation(
                total_bridge_classes=0, valid_bridge_classes=0, issues=[]
            ),
            gates=ValidationGates(
                parse="fail",
                duplicates="pass",
                coverage="fail",
                overall="fail",
            ),
        )

    # Get duplicate IRIs
    duplicate_iris = validator.get_duplicate_iris()

    # Compute edge coverage
    coverage = compute_edge_coverage(validator, approved_edges or [])

    # Check relation names
    relation_names = check_relation_names(validator, overrides_relation_names or {})

    # Check classifications
    classifications = check_classifications(validator, table_classifications)

    # Validate bridge tables
    bridge_tables = validate_bridge_tables(validator)

    # Compute gates
    allow_duplicates = config.allow_duplicate_iris if config else False
    coverage_warn = config.coverage_warn_threshold if config else 0.50
    coverage_pass = config.coverage_pass_threshold if config else 0.90

    gates = ValidationGates(
        parse="pass" if owl_parse.success else "fail",
        duplicates="pass" if (duplicate_iris.count == 0 or allow_duplicates) else "fail",
        coverage=(
            "pass"
            if coverage.coverage_rate >= coverage_pass
            else ("warn" if coverage.coverage_rate >= coverage_warn else "fail")
        ),
        overall="pass",  # computed below
    )

    # Overall gate: pass only if no failures
    if gates.parse == "fail" or gates.duplicates == "fail" or gates.coverage == "fail":
        gates = ValidationGates(
            parse=gates.parse,
            duplicates=gates.duplicates,
            coverage=gates.coverage,
            overall="fail",
        )

    return ValidationReport(
        timestamp=timestamp,
        owl_parse=owl_parse,
        duplicate_iris=duplicate_iris,
        coverage=coverage,
        relation_names=relation_names,
        classifications=classifications,
        bridge_tables=bridge_tables,
        gates=gates,
    )
