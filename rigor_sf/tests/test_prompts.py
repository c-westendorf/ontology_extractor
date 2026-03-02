"""Tests for prompts.py module."""

import pytest

from rigor_sf.prompts import (
    DEFAULT_BASE_IRI,
    build_gen_prompt,
    build_judge_prompt,
    _CLASSIFICATION_GUIDANCE,
)


class TestDefaultBaseIRI:
    """Tests for DEFAULT_BASE_IRI constant."""

    def test_default_value(self):
        """Default IRI is set correctly."""
        assert DEFAULT_BASE_IRI == "http://example.org/rigor#"


class TestClassificationGuidance:
    """Tests for _CLASSIFICATION_GUIDANCE dictionary."""

    def test_all_classifications_defined(self):
        """All expected classifications have guidance."""
        expected = ["fact", "dimension", "entity", "bridge", "staging"]
        for cls in expected:
            assert cls in _CLASSIFICATION_GUIDANCE
            assert len(_CLASSIFICATION_GUIDANCE[cls]) > 0


class TestBuildGenPrompt:
    """Tests for build_gen_prompt function."""

    def test_basic_prompt(self):
        """Basic prompt generation."""
        prompt = build_gen_prompt(
            table_name="CUSTOMERS",
            schema_text="CREATE TABLE CUSTOMERS (ID INT, NAME VARCHAR)",
            core_snips=["rigor:Entity a owl:Class ."],
            external_snips=["foaf:Person a owl:Class ."],
        )

        assert "CUSTOMERS" in prompt
        assert "CREATE TABLE" in prompt
        assert "rigor:Entity" in prompt
        assert "foaf:Person" in prompt
        assert DEFAULT_BASE_IRI in prompt

    def test_custom_base_iri(self):
        """Uses custom base IRI."""
        custom_iri = "http://custom.org/onto#"
        prompt = build_gen_prompt(
            table_name="TEST",
            schema_text="schema",
            core_snips=[],
            external_snips=[],
            base_iri=custom_iri,
        )

        assert custom_iri in prompt
        assert DEFAULT_BASE_IRI not in prompt

    def test_with_classification(self):
        """Includes classification guidance."""
        prompt = build_gen_prompt(
            table_name="ORDERS",
            schema_text="schema",
            core_snips=[],
            external_snips=[],
            table_classification="fact",
        )

        assert "TABLE_CLASSIFICATION: FACT" in prompt
        assert "FACT table" in prompt
        assert "events or transactions" in prompt

    def test_classification_case_insensitive(self):
        """Classification lookup is case-insensitive."""
        prompt = build_gen_prompt(
            table_name="TEST",
            schema_text="schema",
            core_snips=[],
            external_snips=[],
            table_classification="DIMENSION",
        )

        assert "TABLE_CLASSIFICATION: DIMENSION" in prompt
        assert "DIMENSION table" in prompt

    def test_unknown_classification(self):
        """Unknown classification is ignored."""
        prompt = build_gen_prompt(
            table_name="TEST",
            schema_text="schema",
            core_snips=[],
            external_snips=[],
            table_classification="unknown_type",
        )

        assert "TABLE_CLASSIFICATION" not in prompt

    def test_empty_snips(self):
        """Handles empty snippets."""
        prompt = build_gen_prompt(
            table_name="TEST",
            schema_text="schema",
            core_snips=[],
            external_snips=[],
        )

        assert "(none)" in prompt

    def test_prompt_contains_required_sections(self):
        """Prompt contains all required sections."""
        prompt = build_gen_prompt(
            table_name="TEST",
            schema_text="schema",
            core_snips=["snip1"],
            external_snips=["snip2"],
        )

        assert "SCHEMA:" in prompt
        assert "CORE ONTOLOGY SNIPPETS:" in prompt
        assert "EXTERNAL ONTOLOGY HINTS:" in prompt
        assert "JSON header" in prompt
        assert "Turtle" in prompt

    def test_naming_conventions_mentioned(self):
        """Prompt mentions naming conventions per SPEC."""
        prompt = build_gen_prompt(
            table_name="TEST",
            schema_text="schema",
            core_snips=[],
            external_snips=[],
        )

        assert "PascalCase" in prompt
        assert "camelCase" in prompt


class TestBuildJudgePrompt:
    """Tests for build_judge_prompt function."""

    def test_basic_prompt(self):
        """Basic judge prompt generation."""
        prompt = build_judge_prompt(
            schema_text="CREATE TABLE TEST",
            candidate_ttl="rigor:Test a owl:Class .",
        )

        assert "CREATE TABLE TEST" in prompt
        assert "rigor:Test a owl:Class" in prompt
        assert DEFAULT_BASE_IRI in prompt

    def test_custom_base_iri(self):
        """Uses custom base IRI."""
        custom_iri = "http://custom.org/onto#"
        prompt = build_judge_prompt(
            schema_text="schema",
            candidate_ttl="ttl",
            base_iri=custom_iri,
        )

        assert custom_iri in prompt

    def test_with_core_snips(self):
        """Includes core snippets."""
        prompt = build_judge_prompt(
            schema_text="schema",
            candidate_ttl="ttl",
            core_snips=["rigor:Entity a owl:Class ."],
        )

        assert "CORE_ONTOLOGY_SNIPPETS" in prompt
        assert "rigor:Entity" in prompt
        assert "do not redefine" in prompt.lower()

    def test_empty_core_snips(self):
        """Handles empty core snippets."""
        prompt = build_judge_prompt(
            schema_text="schema",
            candidate_ttl="ttl",
            core_snips=[],
        )

        assert "(none)" in prompt

    def test_none_core_snips(self):
        """Handles None core snippets."""
        prompt = build_judge_prompt(
            schema_text="schema",
            candidate_ttl="ttl",
            core_snips=None,
        )

        assert "(none)" in prompt

    def test_tasks_listed(self):
        """Prompt lists review tasks."""
        prompt = build_judge_prompt(
            schema_text="schema",
            candidate_ttl="ttl",
        )

        assert "Turtle syntax" in prompt
        assert "object properties" in prompt
        assert "data properties" in prompt
        assert "datatypes" in prompt

    def test_naming_conventions_mentioned(self):
        """Prompt mentions naming conventions per SPEC."""
        prompt = build_judge_prompt(
            schema_text="schema",
            candidate_ttl="ttl",
        )

        assert "PascalCase" in prompt
        assert "camelCase" in prompt

    def test_returns_only_turtle(self):
        """Prompt asks for only corrected Turtle."""
        prompt = build_judge_prompt(
            schema_text="schema",
            candidate_ttl="ttl",
        )

        assert "Return ONLY corrected Turtle" in prompt


class TestPromptIntegration:
    """Integration tests for prompts."""

    def test_gen_prompt_not_empty(self):
        """Gen prompt is non-empty."""
        prompt = build_gen_prompt(
            table_name="T",
            schema_text="s",
            core_snips=[],
            external_snips=[],
        )
        assert len(prompt) > 100

    def test_judge_prompt_not_empty(self):
        """Judge prompt is non-empty."""
        prompt = build_judge_prompt(
            schema_text="s",
            candidate_ttl="t",
        )
        assert len(prompt) > 100

    def test_prompts_are_strings(self):
        """Both functions return strings."""
        gen = build_gen_prompt("T", "s", [], [])
        judge = build_judge_prompt("s", "t")

        assert isinstance(gen, str)
        assert isinstance(judge, str)
