"""Exit codes and custom exceptions for the RIGOR-SF pipeline.

Exit codes follow SPEC_V2.md §16.5:
- 0: Success
- 1: Configuration error
- 2: Phase prerequisite not met
- 3: Validation failed
- 4: LLM generation failed
"""

from __future__ import annotations

from enum import IntEnum
from typing import Optional


class ExitCode(IntEnum):
    """Pipeline exit codes per SPEC_V2 §16.5."""

    SUCCESS = 0
    CONFIG_ERROR = 1
    PREREQUISITE_NOT_MET = 2
    VALIDATION_FAILED = 3
    LLM_GENERATION_FAILED = 4


class RigorError(Exception):
    """Base exception for RIGOR pipeline errors."""

    exit_code: ExitCode = ExitCode.CONFIG_ERROR

    def __init__(self, message: str, details: Optional[str] = None):
        self.message = message
        self.details = details
        super().__init__(message)

    def __str__(self) -> str:
        if self.details:
            return f"{self.message}\n\nDetails:\n{self.details}"
        return self.message


class ConfigError(RigorError):
    """Configuration loading or validation error."""

    exit_code = ExitCode.CONFIG_ERROR


class PrerequisiteError(RigorError):
    """Phase prerequisite not met (e.g., missing profiling data)."""

    exit_code = ExitCode.PREREQUISITE_NOT_MET


class ValidationError(RigorError):
    """Validation phase failed (coverage below threshold, duplicate IRIs, etc.)."""

    exit_code = ExitCode.VALIDATION_FAILED


class LLMError(RigorError):
    """LLM generation failed after all retries."""

    exit_code = ExitCode.LLM_GENERATION_FAILED

    def __init__(
        self,
        message: str,
        table: Optional[str] = None,
        attempt: Optional[int] = None,
        details: Optional[str] = None,
    ):
        self.table = table
        self.attempt = attempt
        super().__init__(message, details)
