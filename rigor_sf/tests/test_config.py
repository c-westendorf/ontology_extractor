"""Tests for config.py module."""

import pytest
import tempfile
import os
from pathlib import Path

from rigor_sf.config import (
    DBConfig,
    LLMConfig,
    ReviewConfig,
    OntologyConfig,
    ProfilingConfig,
    ValidationConfig,
    PathsConfig,
    LuminaConfig,
    MetadataConfig,
    AppConfig,
    load_config,
)
from rigor_sf.exit_codes import ConfigError


class TestDBConfig:
    """Tests for DBConfig."""

    def test_minimal_config(self):
        """DBConfig with only required url."""
        cfg = DBConfig(url="snowflake://user:pass@account/db/schema")
        assert cfg.url == "snowflake://user:pass@account/db/schema"
        assert cfg.schema_name is None
        assert cfg.include_tables == []
        assert cfg.exclude_tables == []

    def test_with_filters(self):
        """DBConfig with table filters."""
        cfg = DBConfig(
            url="snowflake://test",
            include_tables=["TABLE_A", "TABLE_B"],
            exclude_tables=["STAGING_*"],
        )
        assert cfg.include_tables == ["TABLE_A", "TABLE_B"]
        assert cfg.exclude_tables == ["STAGING_*"]


class TestLLMConfig:
    """Tests for LLMConfig."""

    def test_defaults(self):
        """LLMConfig defaults match SPEC_V2.md."""
        cfg = LLMConfig()
        assert cfg.provider == "cursor"
        assert cfg.model == "claude-3.5-sonnet"
        assert cfg.command == "agent"
        assert cfg.output_format == "json"
        assert cfg.debug is False
        assert cfg.max_retries == 3
        assert cfg.interactive_on_failure is True

    def test_invalid_provider(self):
        """Only 'cursor' provider allowed in v2."""
        with pytest.raises(ValueError, match="Unknown LLM provider"):
            LLMConfig(provider="openai")

    def test_negative_retries(self):
        """max_retries must be non-negative."""
        with pytest.raises(ValueError, match="non-negative"):
            LLMConfig(max_retries=-1)

    def test_valid_provider(self):
        """Valid cursor provider."""
        cfg = LLMConfig(provider="cursor")
        assert cfg.provider == "cursor"


class TestReviewConfig:
    """Tests for ReviewConfig."""

    def test_defaults(self):
        """ReviewConfig defaults."""
        cfg = ReviewConfig()
        assert cfg.auto_approve_threshold == 0.95
        assert cfg.auto_approve_confidence == 0.80
        assert cfg.require_human_review is True

    def test_threshold_validation(self):
        """Thresholds must be 0.0-1.0."""
        with pytest.raises(ValueError, match="between 0.0 and 1.0"):
            ReviewConfig(auto_approve_threshold=1.5)

        with pytest.raises(ValueError, match="between 0.0 and 1.0"):
            ReviewConfig(auto_approve_confidence=-0.1)

    def test_valid_thresholds(self):
        """Valid threshold values."""
        cfg = ReviewConfig(
            auto_approve_threshold=0.90,
            auto_approve_confidence=0.75,
        )
        assert cfg.auto_approve_threshold == 0.90


class TestOntologyConfig:
    """Tests for OntologyConfig."""

    def test_defaults(self):
        """OntologyConfig defaults."""
        cfg = OntologyConfig()
        assert cfg.base_iri == "http://example.org/rigor#"
        assert cfg.format == "xml"
        assert cfg.naming == "standard"

    def test_invalid_format(self):
        """Only xml, turtle, n3 formats allowed."""
        with pytest.raises(ValueError, match="Invalid ontology format"):
            OntologyConfig(format="json-ld")

    def test_valid_formats(self):
        """Valid ontology formats."""
        for fmt in ["xml", "turtle", "n3"]:
            cfg = OntologyConfig(format=fmt)
            assert cfg.format == fmt


class TestProfilingConfig:
    """Tests for ProfilingConfig."""

    def test_defaults(self):
        """ProfilingConfig defaults."""
        cfg = ProfilingConfig()
        assert cfg.sample_limit == 200_000
        assert cfg.match_rate_threshold == 0.90
        assert cfg.null_rate_warning == 0.20

    def test_rate_validation(self):
        """Rates must be 0.0-1.0."""
        with pytest.raises(ValueError, match="between 0.0 and 1.0"):
            ProfilingConfig(match_rate_threshold=2.0)


class TestValidationConfig:
    """Tests for ValidationConfig."""

    def test_defaults(self):
        """ValidationConfig defaults."""
        cfg = ValidationConfig()
        assert cfg.coverage_warn_threshold == 0.50
        assert cfg.coverage_pass_threshold == 0.90
        assert cfg.allow_duplicate_iris is False

    def test_coverage_validation(self):
        """Coverage thresholds must be 0.0-1.0."""
        with pytest.raises(ValueError, match="between 0.0 and 1.0"):
            ValidationConfig(coverage_warn_threshold=1.5)


class TestPathsConfig:
    """Tests for PathsConfig."""

    def test_defaults(self):
        """PathsConfig defaults."""
        cfg = PathsConfig()
        assert cfg.core_in == "data/core.owl"
        assert cfg.core_out == "data/core.owl"
        assert cfg.runs_dir == "runs"
        assert cfg.validation_report == "data/validation_report.json"


class TestLuminaConfig:
    """Tests for LuminaConfig."""

    def test_defaults(self):
        """LuminaConfig defaults."""
        cfg = LuminaConfig()
        assert cfg.enabled is False
        assert cfg.timeout_seconds == 30
        assert cfg.retry_count == 2


class TestAppConfig:
    """Tests for AppConfig."""

    def test_minimal_config(self):
        """AppConfig with minimal required fields."""
        cfg = AppConfig(
            db=DBConfig(url="snowflake://test"),
        )
        assert cfg.db.url == "snowflake://test"
        # Check defaults are applied
        assert cfg.llm.provider == "cursor"
        assert cfg.review.auto_approve_threshold == 0.95
        assert cfg.ontology.base_iri == "http://example.org/rigor#"

    def test_legacy_cursor_agent_migration(self):
        """cursor_agent field migrates to llm."""
        cfg = AppConfig(
            db=DBConfig(url="snowflake://test"),
            cursor_agent=LLMConfig(debug=True),
        )
        assert cfg.llm.debug is True


class TestLoadConfig:
    """Tests for load_config function."""

    def test_valid_config(self):
        """Load a valid config file."""
        config_content = """
db:
  url: "snowflake://user:pass@account/db/schema"
llm:
  provider: cursor
  max_retries: 5
ontology:
  base_iri: "http://test.org/onto#"
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(config_content)
            f.flush()
            try:
                cfg = load_config(f.name)
                assert cfg.db.url == "snowflake://user:pass@account/db/schema"
                assert cfg.llm.max_retries == 5
                assert cfg.ontology.base_iri == "http://test.org/onto#"
            finally:
                os.unlink(f.name)

    def test_missing_file(self):
        """ConfigError on missing file."""
        with pytest.raises(ConfigError, match="not found"):
            load_config("/nonexistent/config.yaml")

    def test_invalid_yaml(self):
        """ConfigError on invalid YAML."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write("invalid: yaml: content: [")
            f.flush()
            try:
                with pytest.raises(ConfigError, match="Invalid YAML"):
                    load_config(f.name)
            finally:
                os.unlink(f.name)

    def test_validation_error(self):
        """ConfigError on validation failure."""
        config_content = """
db:
  url: "snowflake://test"
llm:
  provider: invalid_provider
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(config_content)
            f.flush()
            try:
                with pytest.raises(ConfigError, match="validation failed"):
                    load_config(f.name)
            finally:
                os.unlink(f.name)
