"""Tests for llm_provider.py module."""

import pytest
import time
from unittest.mock import patch, MagicMock

import subprocess

from rigor_sf.llm_provider import (
    LLMResponse,
    LLMProvider,
    CursorProvider,
    create_provider,
    with_retry,
    prompt_user_recovery,
)
from rigor_sf.config import LLMConfig


class TestLLMResponse:
    """Tests for LLMResponse dataclass."""

    def test_successful_response(self):
        """Successful LLM response."""
        resp = LLMResponse(
            content="Generated text",
            raw_output='{"result": "Generated text"}',
            success=True,
        )
        assert resp.content == "Generated text"
        assert resp.success is True
        assert resp.error is None

    def test_failed_response(self):
        """Failed LLM response."""
        resp = LLMResponse(
            content="",
            raw_output="",
            success=False,
            error="Connection timeout",
        )
        assert resp.success is False
        assert resp.error == "Connection timeout"


class TestCursorProvider:
    """Tests for CursorProvider."""

    def test_init(self):
        """CursorProvider initializes from LLMConfig."""
        config = LLMConfig(
            command="agent",
            output_format="json",
            debug=True,
        )
        provider = CursorProvider(config)
        assert provider._command == "agent"
        assert provider._output_format == "json"
        assert provider._debug is True

    @patch("subprocess.run")
    def test_generate_success_json(self, mock_run):
        """Successful generation with JSON output."""
        mock_run.return_value = MagicMock(
            stdout='{"result": "Hello world"}',
            stderr="",
            returncode=0,
        )

        config = LLMConfig(output_format="json")
        provider = CursorProvider(config)
        response = provider.generate("Test prompt")

        assert response.success is True
        assert response.content == "Hello world"
        mock_run.assert_called_once()

    @patch("subprocess.run")
    def test_generate_success_text(self, mock_run):
        """Successful generation with text output."""
        mock_run.return_value = MagicMock(
            stdout="Plain text response",
            stderr="",
            returncode=0,
        )

        config = LLMConfig(output_format="text")
        provider = CursorProvider(config)
        response = provider.generate("Test prompt")

        assert response.success is True
        assert response.content == "Plain text response"

    @patch("subprocess.run")
    def test_generate_json_nested_data(self, mock_run):
        """JSON output with nested data structure."""
        mock_run.return_value = MagicMock(
            stdout='{"data": {"content": "Nested response"}}',
            stderr="",
            returncode=0,
        )

        config = LLMConfig(output_format="json")
        provider = CursorProvider(config)
        response = provider.generate("Test prompt")

        assert response.success is True
        assert response.content == "Nested response"

    @patch("subprocess.run")
    def test_generate_subprocess_error(self, mock_run):
        """Subprocess CalledProcessError handling."""
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=["agent", "-p"],
            stderr="Command failed",
        )

        config = LLMConfig()
        provider = CursorProvider(config)
        response = provider.generate("Test prompt")

        assert response.success is False
        assert "exit code 1" in response.error

    @patch("subprocess.run")
    def test_generate_command_not_found(self, mock_run):
        """FileNotFoundError when command not found."""
        mock_run.side_effect = FileNotFoundError()

        config = LLMConfig(command="nonexistent")
        provider = CursorProvider(config)
        response = provider.generate("Test prompt")

        assert response.success is False
        assert "not found" in response.error

    @patch("subprocess.run")
    def test_is_available_true(self, mock_run):
        """is_available returns True when command exists."""
        mock_run.return_value = MagicMock(returncode=0)

        config = LLMConfig()
        provider = CursorProvider(config)

        assert provider.is_available() is True

    @patch("subprocess.run")
    def test_is_available_false(self, mock_run):
        """is_available returns False when command missing."""
        mock_run.side_effect = FileNotFoundError()

        config = LLMConfig()
        provider = CursorProvider(config)

        assert provider.is_available() is False

    @patch("subprocess.run")
    def test_extract_content_fallback(self, mock_run):
        """Falls back to raw output when no known keys found."""
        mock_run.return_value = MagicMock(
            stdout='{"unknown_key": "value"}',
            stderr="",
            returncode=0,
        )

        config = LLMConfig(output_format="json")
        provider = CursorProvider(config)
        response = provider.generate("Test prompt")

        assert response.success is True
        # Falls back to raw stdout
        assert response.content == '{"unknown_key": "value"}'


class TestCreateProvider:
    """Tests for create_provider factory function."""

    def test_create_cursor_provider(self):
        """Creates CursorProvider for 'cursor' provider."""
        config = LLMConfig(provider="cursor")
        provider = create_provider(config)

        assert isinstance(provider, CursorProvider)
        assert isinstance(provider, LLMProvider)

    def test_unknown_provider(self):
        """Raises ValueError for unknown provider."""
        # We need to bypass validation to test the factory
        config = LLMConfig()
        config.provider = "unknown"  # type: ignore

        with pytest.raises(ValueError, match="Unknown LLM provider"):
            create_provider(config)


class TestWithRetry:
    """Tests for with_retry decorator."""

    def test_success_first_attempt(self):
        """Returns successful response on first attempt."""
        @with_retry(max_retries=3)
        def always_succeeds(prompt: str) -> LLMResponse:
            return LLMResponse(content="success", raw_output="", success=True)

        response = always_succeeds("test")
        assert response.success is True
        assert response.attempt == 1

    def test_success_after_retries(self):
        """Succeeds after a few failed attempts."""
        call_count = 0

        @with_retry(max_retries=3, base_delay=0.01)  # Small delay for fast tests
        def succeeds_on_third(prompt: str) -> LLMResponse:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return LLMResponse(content="", raw_output="", success=False, error="fail")
            return LLMResponse(content="success", raw_output="", success=True)

        response = succeeds_on_third("test")
        assert response.success is True
        assert response.attempt == 3
        assert call_count == 3

    def test_all_retries_exhausted(self):
        """Returns failed response after all retries exhausted."""
        call_count = 0

        @with_retry(max_retries=2, base_delay=0.01)
        def always_fails(prompt: str) -> LLMResponse:
            nonlocal call_count
            call_count += 1
            return LLMResponse(content="", raw_output="", success=False, error="fail")

        response = always_fails("test")
        assert response.success is False
        assert response.attempt == 3  # 1 initial + 2 retries
        assert call_count == 3

    def test_exponential_backoff_timing(self):
        """Verifies exponential backoff delays."""
        call_times = []

        @with_retry(max_retries=2, base_delay=0.1, exponential_base=2.0)
        def record_time(prompt: str) -> LLMResponse:
            call_times.append(time.time())
            return LLMResponse(content="", raw_output="", success=False, error="fail")

        record_time("test")

        # Should have 3 calls: initial + 2 retries
        assert len(call_times) == 3

        # Check delays (approximately)
        delay1 = call_times[1] - call_times[0]
        delay2 = call_times[2] - call_times[1]

        # First delay should be ~0.1s (base_delay * 2^0)
        assert 0.05 <= delay1 <= 0.2, f"First delay {delay1} not in expected range"
        # Second delay should be ~0.2s (base_delay * 2^1)
        assert 0.1 <= delay2 <= 0.4, f"Second delay {delay2} not in expected range"

    def test_max_delay_cap(self):
        """Verifies max_delay caps exponential growth."""
        @with_retry(max_retries=1, base_delay=100.0, max_delay=0.01)
        def quick_fail(prompt: str) -> LLMResponse:
            return LLMResponse(content="", raw_output="", success=False, error="fail")

        start = time.time()
        quick_fail("test")
        elapsed = time.time() - start

        # Should be capped to ~0.01s, not 100s
        assert elapsed < 1.0

    def test_response_attempt_tracking(self):
        """Tracks attempt number in response."""
        attempts = []

        @with_retry(max_retries=3, base_delay=0.01)
        def track_attempts(prompt: str) -> LLMResponse:
            return LLMResponse(content="", raw_output="", success=False, error="fail")

        response = track_attempts("test")
        assert response.attempt == 4  # 1 initial + 3 retries


class TestPromptUserRecovery:
    """Tests for prompt_user_recovery function."""

    @patch("builtins.input", return_value="s")
    @patch("builtins.print")
    def test_skip_choice(self, mock_print, mock_input):
        """Returns 'skip' when user enters 's'."""
        result = prompt_user_recovery("Error message", "TABLE_NAME")
        assert result == "skip"

    @patch("builtins.input", return_value="skip")
    @patch("builtins.print")
    def test_skip_full_word(self, mock_print, mock_input):
        """Returns 'skip' when user enters 'skip'."""
        result = prompt_user_recovery("Error message")
        assert result == "skip"

    @patch("builtins.input", return_value="r")
    @patch("builtins.print")
    def test_retry_choice(self, mock_print, mock_input):
        """Returns 'retry' when user enters 'r'."""
        result = prompt_user_recovery("Error message")
        assert result == "retry"

    @patch("builtins.input", return_value="h")
    @patch("builtins.print")
    def test_halt_choice(self, mock_print, mock_input):
        """Returns 'halt' when user enters 'h'."""
        result = prompt_user_recovery("Error message")
        assert result == "halt"

    @patch("builtins.input", return_value="@prefix rigor: <http://example.org/rigor#> .")
    @patch("builtins.print")
    def test_custom_ttl_content(self, mock_print, mock_input):
        """Returns custom TTL content when user pastes it."""
        result = prompt_user_recovery("Error message")
        assert result == "@prefix rigor: <http://example.org/rigor#> ."

    @patch("builtins.input", side_effect=["", "", "s"])
    @patch("builtins.print")
    def test_empty_input_reprompts(self, mock_print, mock_input):
        """Reprompts on empty input."""
        result = prompt_user_recovery("Error message")
        assert result == "skip"
        assert mock_input.call_count == 3

    @patch("builtins.print")
    def test_displays_table_name_context(self, mock_print):
        """Displays table name in error context."""
        with patch("builtins.input", return_value="s"):
            prompt_user_recovery("Error message", "CUSTOMERS")

        # Check that CUSTOMERS appears in printed output
        printed_text = " ".join(str(call) for call in mock_print.call_args_list)
        assert "CUSTOMERS" in printed_text
