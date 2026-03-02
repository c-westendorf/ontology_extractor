"""Tests for exit_codes.py module."""

import pytest

from rigor_v1.exit_codes import (
    ExitCode,
    RigorError,
    ConfigError,
    PrerequisiteError,
    ValidationError,
    LLMError,
)


class TestExitCode:
    """Tests for ExitCode enum."""

    def test_exit_code_values(self):
        """Exit codes match SPEC_V2.md §16.5."""
        assert ExitCode.SUCCESS == 0
        assert ExitCode.CONFIG_ERROR == 1
        assert ExitCode.PREREQUISITE_NOT_MET == 2
        assert ExitCode.VALIDATION_FAILED == 3
        assert ExitCode.LLM_GENERATION_FAILED == 4

    def test_exit_code_is_int(self):
        """Exit codes can be used as integers."""
        assert int(ExitCode.SUCCESS) == 0
        assert int(ExitCode.CONFIG_ERROR) == 1


class TestRigorError:
    """Tests for RigorError base exception."""

    def test_basic_error(self):
        """Basic error with message only."""
        err = RigorError("Something went wrong")
        assert err.message == "Something went wrong"
        assert err.details is None
        assert str(err) == "Something went wrong"

    def test_error_with_details(self):
        """Error with additional details."""
        err = RigorError("Failed", details="More info here")
        assert err.message == "Failed"
        assert err.details == "More info here"
        assert "More info here" in str(err)

    def test_default_exit_code(self):
        """Default exit code is CONFIG_ERROR."""
        err = RigorError("Test")
        assert err.exit_code == ExitCode.CONFIG_ERROR


class TestConfigError:
    """Tests for ConfigError exception."""

    def test_exit_code(self):
        """ConfigError has correct exit code."""
        err = ConfigError("Invalid config")
        assert err.exit_code == ExitCode.CONFIG_ERROR

    def test_inheritance(self):
        """ConfigError inherits from RigorError."""
        err = ConfigError("Test")
        assert isinstance(err, RigorError)
        assert isinstance(err, Exception)


class TestPrerequisiteError:
    """Tests for PrerequisiteError exception."""

    def test_exit_code(self):
        """PrerequisiteError has correct exit code."""
        err = PrerequisiteError("Missing file")
        assert err.exit_code == ExitCode.PREREQUISITE_NOT_MET

    def test_with_details(self):
        """PrerequisiteError with details."""
        err = PrerequisiteError(
            "Inferred relationships CSV not found",
            details="Expected: data/inferred_relationships.csv",
        )
        assert "Expected:" in str(err)


class TestValidationError:
    """Tests for ValidationError exception."""

    def test_exit_code(self):
        """ValidationError has correct exit code."""
        err = ValidationError("Coverage below threshold")
        assert err.exit_code == ExitCode.VALIDATION_FAILED


class TestLLMError:
    """Tests for LLMError exception."""

    def test_exit_code(self):
        """LLMError has correct exit code."""
        err = LLMError("Generation failed")
        assert err.exit_code == ExitCode.LLM_GENERATION_FAILED

    def test_with_table_info(self):
        """LLMError with table and attempt info."""
        err = LLMError(
            "LLM generation failed",
            table="CUSTOMERS",
            attempt=3,
            details="Timeout after 30s",
        )
        assert err.table == "CUSTOMERS"
        assert err.attempt == 3
        assert err.details == "Timeout after 30s"

    def test_inheritance(self):
        """LLMError inherits from RigorError."""
        err = LLMError("Test")
        assert isinstance(err, RigorError)
