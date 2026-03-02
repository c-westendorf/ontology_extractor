"""DEPRECATED: This module is superseded by llm_provider.py.

This module is kept for reference during migration. All new code should use
the LLMProvider interface from llm_provider.py instead.

The new llm_provider.py provides:
- Abstract LLMProvider interface for provider independence
- CursorProvider implementation with retry logic
- LLMResponse dataclass for structured responses
- Error recovery with interactive S/R/H prompts

To migrate from cursor_cli.py to llm_provider.py:

    # Old code:
    from rigor_v1.cursor_cli import call_cursor_agent, CursorAgentSettings
    settings = CursorAgentSettings(command="agent", output_format="json")
    result = call_cursor_agent(prompt, settings)

    # New code:
    from rigor_v1.llm_provider import create_provider, LLMConfig
    config = LLMConfig(provider="cursor", command="agent", output_format="json")
    provider = create_provider(config)
    response = provider.generate(prompt)
    result = response.content if response.success else None

This module will be removed in a future version.
"""

import warnings

warnings.warn(
    "cursor_cli module is deprecated. Use llm_provider module instead.",
    DeprecationWarning,
    stacklevel=2,
)

from __future__ import annotations
import json
import subprocess
from dataclasses import dataclass

@dataclass
class CursorAgentSettings:
    command: str = "agent"
    output_format: str = "json"
    debug: bool = False

def call_cursor_agent(prompt: str, settings: CursorAgentSettings) -> str:
    cmd = [settings.command, "-p"]
    if settings.output_format:
        cmd += ["--output-format", settings.output_format]

    if settings.debug:
        print("=== Cursor Agent prompt ===")
        print(prompt)
        print("=== end prompt ===")

    proc = subprocess.run(
        cmd + [prompt],
        check=True,
        capture_output=True,
        text=True,
    )

    if settings.debug:
        print("=== Cursor Agent raw stdout ===")
        print(proc.stdout)
        print("=== end stdout ===")
        if proc.stderr:
            print("=== Cursor Agent stderr ===")
            print(proc.stderr)
            print("=== end stderr ===")

    # Cursor docs say JSON is available for scripting; schema can vary by version.
    if settings.output_format == "json":
        payload = json.loads(proc.stdout)
        # Try common keys; fallback to raw stdout.
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
        return proc.stdout

    return proc.stdout
