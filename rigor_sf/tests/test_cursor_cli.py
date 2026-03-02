"""Tests for legacy cursor_cli shim compatibility."""

from __future__ import annotations

import importlib
import warnings
from unittest.mock import MagicMock

import pytest

import rigor_sf.cursor_cli as cursor_cli
from rigor_sf.llm_provider import LLMResponse


class TestCursorCliShim:
    """Compatibility tests for deprecated cursor_cli module."""

    def test_legacy_import_works(self):
        """Legacy symbols remain importable."""
        assert hasattr(cursor_cli, "CursorAgentSettings")
        assert hasattr(cursor_cli, "call_cursor_agent")

    def test_deprecation_warning_emitted_once(self):
        """Warning is emitted once per import lifecycle."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", DeprecationWarning)
            mod = importlib.reload(cursor_cli)
            mod._warn_deprecated_once()
            mod._warn_deprecated_once()
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert len(dep_warnings) == 1
        assert "deprecated" in str(dep_warnings[0].message).lower()

    def test_delegates_to_provider(self, monkeypatch):
        """Legacy wrapper delegates to create_provider(generate)."""
        fake_provider = MagicMock()
        fake_provider.generate.return_value = LLMResponse(
            content="delegated output",
            raw_output='{"result":"delegated output"}',
            success=True,
        )

        created = {}

        def _fake_create_provider(cfg):
            created["cfg"] = cfg
            return fake_provider

        monkeypatch.setattr(cursor_cli, "create_provider", _fake_create_provider)

        settings = cursor_cli.CursorAgentSettings(
            command="agentx",
            output_format="text",
            debug=True,
        )
        out = cursor_cli.call_cursor_agent("hello", settings)

        assert out == "delegated output"
        assert created["cfg"].provider == "cursor"
        assert created["cfg"].command == "agentx"
        assert created["cfg"].output_format == "text"
        assert created["cfg"].debug is True
        fake_provider.generate.assert_called_once_with("hello")

    def test_failure_raises_runtime_error(self, monkeypatch):
        """Failed provider response raises deterministic RuntimeError."""
        fake_provider = MagicMock()
        fake_provider.generate.return_value = LLMResponse(
            content="",
            raw_output="",
            success=False,
            error="bad call",
        )
        monkeypatch.setattr(cursor_cli, "create_provider", lambda _cfg: fake_provider)

        with pytest.raises(RuntimeError, match="cursor_cli shim call failed: bad call"):
            cursor_cli.call_cursor_agent("prompt", cursor_cli.CursorAgentSettings())
