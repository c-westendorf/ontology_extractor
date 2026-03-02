"""Prompt templates for the RIGOR-SF pipeline.

Defines generation and judge prompts per SPEC_V2.md §7.
"""

from __future__ import annotations
from typing import Optional

# Default base IRI (can be overridden via config)
DEFAULT_BASE_IRI = "http://example.org/rigor#"

# ── Classification guidance injected into generation prompt ───────────────────
_CLASSIFICATION_GUIDANCE: dict[str, str] = {
    "fact": (
        "This table is a FACT table. It records events or transactions. "
        "Model it as a class representing the event type. "
        "Its numeric columns are likely measures (data properties). "
        "Its ID columns are likely foreign keys to dimension/entity tables (object properties)."
    ),
    "dimension": (
        "This table is a DIMENSION table. It describes attributes of entities. "
        "Model it as a descriptive class. Most columns are data properties. "
        "It is likely the REFERRED (parent) side of foreign key relationships."
    ),
    "entity": (
        "This table is an ENTITY table. It represents a core business concept. "
        "Model it as a first-class OWL class with a clear rdfs:label. "
        "It is typically the REFERRED side of foreign key relationships."
    ),
    "bridge": (
        "This table is a BRIDGE table representing a many-to-many relationship. "
        "Do NOT model this as a standalone class. Instead, generate an object property "
        "between the two endpoint entity/dimension classes it connects. "
        "Use the bridge table's non-ID columns as data properties on that object property "
        "or as an reified association class if they carry meaningful attributes."
    ),
    "staging": (
        "This table is a STAGING / ETL table. Treat it with lower confidence. "
        "Do not generate public OWL classes for it unless it clearly represents a "
        "business concept. Note this in assumptions."
    ),
}


def build_gen_prompt(
    table_name: str,
    schema_text: str,
    core_snips: list[str],
    external_snips: list[str],
    table_classification: Optional[str] = None,
    base_iri: Optional[str] = None,
) -> str:
    """Build the generation prompt for a table.

    Args:
        table_name: Name of the table
        schema_text: Schema documentation text
        core_snips: Snippets from core ontology
        external_snips: Snippets from external ontologies
        table_classification: Optional classification (fact/dimension/entity/bridge/staging)
        base_iri: Base IRI for the ontology (defaults to DEFAULT_BASE_IRI)

    Returns:
        Formatted prompt string
    """
    iri = base_iri or DEFAULT_BASE_IRI

    # Build classification block only when set
    if table_classification and table_classification.lower() in _CLASSIFICATION_GUIDANCE:
        cls_block = (
            f"\nTABLE_CLASSIFICATION: {table_classification.upper()}\n"
            f"{_CLASSIFICATION_GUIDANCE[table_classification.lower()]}\n"
        )
    else:
        cls_block = ""

    return f"""You are an ontology engineer. Convert a relational database table into an OWL 2 DL ontology fragment.

Return EXACTLY TWO PARTS:
1) A single-line JSON header with keys:
   - table
   - created_entities: {{classes:[], object_properties:[], data_properties:[]}}
   - assumptions: []
2) A Turtle (TTL) ontology fragment.

Rules:
- Use base IRI: {iri}
- Use PascalCase for class names (e.g., CustomerOrder)
- Use camelCase for property names (e.g., hasCustomer, orderDate)
- Create:
  - Classes for key entities
  - Object properties for foreign key relations (domain/range)
  - Data properties for columns (domain, xsd datatype when possible)
- Add rdfs:label and rdfs:comment where helpful.
- Keep fragment self-contained; do not redefine existing core terms unless extending.
- If uncertain, write assumptions in the JSON header.
{cls_block}
SCHEMA:
{schema_text}

CORE ONTOLOGY SNIPPETS:
{chr(10).join(core_snips) if core_snips else "(none)"}

EXTERNAL ONTOLOGY HINTS:
{chr(10).join(external_snips) if external_snips else "(none)"}

Now produce the JSON header + Turtle.
""".strip()


def build_judge_prompt(
    schema_text: str,
    candidate_ttl: str,
    core_snips: list[str] | None = None,
    base_iri: Optional[str] = None,
) -> str:
    """Build the judge/review prompt for validating generated TTL.

    Args:
        schema_text: Schema documentation text
        candidate_ttl: Generated Turtle to review
        core_snips: Snippets from core ontology
        base_iri: Base IRI for the ontology (defaults to DEFAULT_BASE_IRI)

    Returns:
        Formatted prompt string
    """
    iri = base_iri or DEFAULT_BASE_IRI

    core_block = (
        "\nCORE_ONTOLOGY_SNIPPETS (already committed — do not redefine these terms):\n"
        + (chr(10).join(core_snips) if core_snips else "(none)")
        + "\n"
    )

    return f"""You are a strict OWL/Turtle reviewer.

Given:
SCHEMA:
{schema_text}
{core_block}
CANDIDATE_TURTLE:
{candidate_ttl}

Tasks:
1) Fix any Turtle syntax errors.
2) Ensure foreign keys are modeled as object properties with correct domain/range when possible.
3) Ensure columns are data properties with plausible datatypes (xsd:string/int/decimal/date/dateTime/boolean).
4) Remove contradictions / duplicates.
5) Keep base IRI {iri}.
6) Ensure PascalCase for class names and camelCase for property names.
7) Do NOT redefine any term already present in CORE_ONTOLOGY_SNIPPETS — extend or reference instead.

Return ONLY corrected Turtle.
""".strip()
