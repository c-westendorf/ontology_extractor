"""Configuration schema for the RIGOR-SF pipeline.

Defines all configuration classes per SPEC_V2.md §9.
"""

from __future__ import annotations

from typing import List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator


class DBConfig(BaseModel):
    """Database connection configuration."""

    url: str
    schema_name: Optional[str] = Field(default=None, alias="schema")
    include_tables: List[str] = Field(default_factory=list)
    exclude_tables: List[str] = Field(default_factory=list)


class LLMConfig(BaseModel):
    """LLM provider configuration (SPEC §9 - v2).

    In v2, only 'cursor' provider is supported. Interface prepared for future providers.
    """

    provider: str = "cursor"
    model: str = "claude-3.5-sonnet"
    command: str = "agent"
    output_format: str = "json"
    debug: bool = False
    max_retries: int = 3
    interactive_on_failure: bool = True

    @field_validator("provider")
    @classmethod
    def validate_provider(cls, v: str) -> str:
        if v not in ("cursor",):
            raise ValueError(f"Unknown LLM provider: {v}. Only 'cursor' is supported in v2.")
        return v

    @field_validator("max_retries")
    @classmethod
    def validate_max_retries(cls, v: int) -> int:
        if v < 0:
            raise ValueError("max_retries must be non-negative")
        return v


class ReviewConfig(BaseModel):
    """Review phase configuration with auto-approve thresholds (SPEC §9 - v2)."""

    auto_approve_threshold: float = 0.95  # match_rate threshold
    auto_approve_confidence: float = 0.80  # SQL confidence threshold
    require_human_review: bool = True  # if False, skip UI for auto-approved edges

    @field_validator("auto_approve_threshold", "auto_approve_confidence")
    @classmethod
    def validate_threshold(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError("Threshold must be between 0.0 and 1.0")
        return v


class OntologyConfig(BaseModel):
    """Ontology generation configuration (SPEC §9 - v2)."""

    base_iri: str = "http://example.org/rigor#"
    format: str = "xml"  # xml | turtle | n3
    naming: str = "standard"  # standard = PascalCase classes, camelCase properties

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: str) -> str:
        if v not in ("xml", "turtle", "n3"):
            raise ValueError(f"Invalid ontology format: {v}. Must be xml, turtle, or n3.")
        return v


class ProfilingConfig(BaseModel):
    """Profiling thresholds configuration (SPEC §9 - v2)."""

    sample_limit: int = 200_000
    match_rate_threshold: float = 0.90
    null_rate_warning: float = 0.20
    frequency_boost_5: float = 0.05
    frequency_boost_10: float = 0.10

    @field_validator("match_rate_threshold", "null_rate_warning")
    @classmethod
    def validate_rate(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError("Rate must be between 0.0 and 1.0")
        return v


class ValidationConfig(BaseModel):
    """Validation phase configuration (SPEC §9 - v2)."""

    coverage_warn_threshold: float = 0.50
    coverage_pass_threshold: float = 0.90
    allow_duplicate_iris: bool = False

    @field_validator("coverage_warn_threshold", "coverage_pass_threshold")
    @classmethod
    def validate_coverage(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError("Coverage threshold must be between 0.0 and 1.0")
        return v


class PathsConfig(BaseModel):
    """File path configuration."""

    core_in: str = "data/core.owl"
    core_out: str = "data/core.owl"
    provenance_jsonl: str = "data/provenance.jsonl"
    fragments_dir: str = "data/fragments"
    inferred_relationships_csv: str = "data/inferred_relationships.csv"
    overrides_yaml: str = "golden/overrides.yaml"
    runs_dir: str = "runs"  # v2: base directory for query-gen runs
    data_quality_report: str = "data/data_quality_report.json"
    validation_report: str = "data/validation_report.json"


class LuminaConfig(BaseModel):
    """Lumina MCP client configuration."""

    enabled: bool = False
    base_url: str = ""
    bearer_token: str = ""
    chat_path: str = "/chat"
    extra_headers: dict[str, str] = Field(default_factory=dict)
    strict_json: bool = True
    timeout_seconds: int = 30  # v2: configurable timeout
    retry_count: int = 2  # v2: retry count


class MetadataConfig(BaseModel):
    """Metadata enrichment configuration."""

    tables_csv: str = "metadata/tables.csv"
    columns_csv: str = "metadata/columns.csv"
    lumina: LuminaConfig = Field(default_factory=LuminaConfig)


# Legacy alias for backward compatibility
CursorAgentConfig = LLMConfig


class AppConfig(BaseModel):
    """Root application configuration (SPEC §9).

    Migration note: 'cursor_agent' is renamed to 'llm' in v2.
    Both are accepted for backward compatibility.
    """

    db: DBConfig
    llm: LLMConfig = Field(default_factory=LLMConfig)
    review: ReviewConfig = Field(default_factory=ReviewConfig)
    paths: PathsConfig = Field(default_factory=PathsConfig)
    metadata: MetadataConfig = Field(default_factory=MetadataConfig)
    profiling: ProfilingConfig = Field(default_factory=ProfilingConfig)
    ontology: OntologyConfig = Field(default_factory=OntologyConfig)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)

    # Legacy field alias
    cursor_agent: Optional[LLMConfig] = Field(default=None, exclude=True)

    def model_post_init(self, __context) -> None:
        """Handle legacy cursor_agent field migration."""
        if self.cursor_agent is not None:
            # Merge legacy config into llm
            self.llm = self.cursor_agent


def load_config(path: str) -> AppConfig:
    """Load configuration from YAML file.

    Args:
        path: Path to config.yaml file

    Returns:
        Validated AppConfig instance

    Raises:
        ConfigError: If config file is invalid or missing required fields
    """
    from .exit_codes import ConfigError

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except FileNotFoundError:
        raise ConfigError(f"Config file not found: {path}")
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in config file: {path}", details=str(e))

    try:
        return AppConfig.model_validate(data)
    except Exception as e:
        raise ConfigError(f"Config validation failed: {path}", details=str(e))
