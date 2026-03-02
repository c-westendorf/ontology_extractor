"""Backward-compatible deprecation shim for legacy cursor_cli imports."""

from __future__ import annotations

import warnings
from dataclasses import dataclass

from .config import LLMConfig
from .llm_provider import create_provider
from .logging_config import get_logger

logger = get_logger(__name__)

_DEPRECATION_MESSAGE = (
    "cursor_cli module is deprecated. Use rigor_sf.llm_provider/create_provider instead."
)
_WARNED = False


def _warn_deprecated_once() -> None:
    global _WARNED
    if _WARNED:
        return
    _WARNED = True
    warnings.warn(_DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
    logger.warning(_DEPRECATION_MESSAGE)


@dataclass
class CursorAgentSettings:
    """Legacy compatibility settings."""

    command: str = "agent"
    output_format: str = "json"
    debug: bool = False


def call_cursor_agent(prompt: str, settings: CursorAgentSettings) -> str:
    """Legacy compatibility wrapper around the v2 LLM provider interface."""
    _warn_deprecated_once()

    provider_cfg = LLMConfig(
        provider="cursor",
        command=settings.command,
        output_format=settings.output_format,
        debug=settings.debug,
    )
    provider = create_provider(provider_cfg)
    response = provider.generate(prompt)

    if response.success:
        return response.content

    raise RuntimeError(f"cursor_cli shim call failed: {response.error or 'unknown error'}")


_warn_deprecated_once()
