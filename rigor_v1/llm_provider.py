"""LLM provider abstraction for the RIGOR-SF pipeline.

Defines the abstract LLMProvider interface and concrete implementations.
Per SPEC_V2.md §9, only 'cursor' provider is supported in v2.
"""

from __future__ import annotations

import json
import subprocess
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import wraps
from typing import TYPE_CHECKING, Callable, TypeVar

if TYPE_CHECKING:
    from .config import LLMConfig


T = TypeVar("T")


@dataclass
class LLMResponse:
    """Response from an LLM provider."""

    content: str
    raw_output: str
    success: bool
    error: str | None = None
    attempt: int = 1


def with_retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
) -> Callable[[Callable[..., LLMResponse]], Callable[..., LLMResponse]]:
    """Decorator for LLM calls with exponential backoff retry.

    Per SPEC_V2.md §9, implements retry logic with configurable backoff.

    Args:
        max_retries: Maximum number of retry attempts (total attempts = max_retries + 1)
        base_delay: Initial delay in seconds before first retry
        max_delay: Maximum delay between retries (caps exponential growth)
        exponential_base: Base for exponential backoff calculation

    Returns:
        Decorated function with retry logic
    """
    def decorator(func: Callable[..., LLMResponse]) -> Callable[..., LLMResponse]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> LLMResponse:
            last_response: LLMResponse | None = None

            for attempt in range(1, max_retries + 2):  # +2 because range is exclusive
                response = func(*args, **kwargs)
                response.attempt = attempt

                if response.success:
                    return response

                last_response = response

                # Don't sleep after the last attempt
                if attempt <= max_retries:
                    delay = min(base_delay * (exponential_base ** (attempt - 1)), max_delay)
                    time.sleep(delay)

            # Return the last failed response
            return last_response or LLMResponse(
                content="",
                raw_output="",
                success=False,
                error="All retry attempts exhausted",
                attempt=max_retries + 1,
            )
        return wrapper
    return decorator


def prompt_user_recovery(error_msg: str, table_name: str | None = None) -> str:
    """Prompt user for recovery action on LLM failure.

    Per SPEC_V2.md §9, provides interactive S/R/H prompt.

    Args:
        error_msg: Error message to display
        table_name: Optional table name for context

    Returns:
        User choice: 's' (skip), 'r' (retry), 'h' (halt), or custom content
    """
    context = f" for {table_name}" if table_name else ""
    print(f"\n[LLM FAILURE{context}] {error_msg}")
    print("Options:")
    print("  (S)kip  - Skip this item and continue")
    print("  (R)etry - Retry the LLM call")
    print("  (H)alt  - Stop the pipeline with exit code 4")
    print("  Or paste TTL content directly")

    while True:
        choice = input("\nChoice [S/R/H or TTL content]: ").strip()
        if not choice:
            continue

        lower = choice.lower()
        if lower in ("s", "skip"):
            return "skip"
        if lower in ("r", "retry"):
            return "retry"
        if lower in ("h", "halt"):
            return "halt"

        # Assume anything else is TTL content
        return choice


class LLMProvider(ABC):
    """Abstract base class for LLM providers.

    Interface prepared for future providers (e.g., direct API calls).
    In v2, only CursorProvider is implemented.
    """

    @abstractmethod
    def generate(self, prompt: str) -> LLMResponse:
        """Generate a response from the LLM.

        Args:
            prompt: The prompt to send to the LLM

        Returns:
            LLMResponse with the generated content
        """
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """Check if the provider is available and configured.

        Returns:
            True if the provider can be used
        """
        pass


class CursorProvider(LLMProvider):
    """LLM provider using Cursor CLI agent command.

    Wraps the Cursor CLI to invoke LLM generation via subprocess.
    """

    def __init__(self, config: "LLMConfig"):
        """Initialize the Cursor provider.

        Args:
            config: LLMConfig instance with provider settings
        """
        self.config = config
        self._command = config.command
        self._output_format = config.output_format
        self._debug = config.debug

    def generate(self, prompt: str) -> LLMResponse:
        """Generate a response using Cursor CLI.

        Args:
            prompt: The prompt to send to Cursor agent

        Returns:
            LLMResponse with the generated content
        """
        cmd = [self._command, "-p"]
        if self._output_format:
            cmd += ["--output-format", self._output_format]

        if self._debug:
            print("=== Cursor Agent prompt ===")
            print(prompt)
            print("=== end prompt ===")

        try:
            proc = subprocess.run(
                cmd + [prompt],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            return LLMResponse(
                content="",
                raw_output=e.stderr or e.stdout or "",
                success=False,
                error=f"Cursor CLI failed with exit code {e.returncode}: {e.stderr}",
            )
        except FileNotFoundError:
            return LLMResponse(
                content="",
                raw_output="",
                success=False,
                error=f"Cursor CLI command not found: {self._command}",
            )

        if self._debug:
            print("=== Cursor Agent raw stdout ===")
            print(proc.stdout)
            print("=== end stdout ===")
            if proc.stderr:
                print("=== Cursor Agent stderr ===")
                print(proc.stderr)
                print("=== end stderr ===")

        content = self._extract_content(proc.stdout)
        return LLMResponse(
            content=content,
            raw_output=proc.stdout,
            success=True,
        )

    def _extract_content(self, stdout: str) -> str:
        """Extract content from Cursor CLI output.

        Args:
            stdout: Raw stdout from Cursor CLI

        Returns:
            Extracted content string
        """
        if self._output_format != "json":
            return stdout

        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError:
            return stdout

        # Try common keys; fallback to raw stdout
        for key in ("result", "output", "message", "text", "content"):
            v = payload.get(key)
            if isinstance(v, str) and v.strip():
                return v

        # Some CLIs nest output
        if isinstance(payload.get("data"), dict):
            for key in ("result", "output", "text", "content"):
                v = payload["data"].get(key)
                if isinstance(v, str) and v.strip():
                    return v

        return stdout

    def is_available(self) -> bool:
        """Check if Cursor CLI is available.

        Returns:
            True if the Cursor CLI command exists
        """
        try:
            subprocess.run(
                [self._command, "--version"],
                capture_output=True,
                check=False,
            )
            return True
        except FileNotFoundError:
            return False


def create_provider(config: "LLMConfig") -> LLMProvider:
    """Factory function to create an LLM provider.

    Args:
        config: LLMConfig instance

    Returns:
        Appropriate LLMProvider implementation

    Raises:
        ValueError: If provider is not supported
    """
    if config.provider == "cursor":
        return CursorProvider(config)
    raise ValueError(f"Unknown LLM provider: {config.provider}. Only 'cursor' is supported in v2.")
