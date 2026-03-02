"""Unit tests for rigor_v1.metadata.lumina_mcp module.

Tests the Lumina MCP client with CircuitBreaker and retry logic per SPEC_V2.md §2.4.
"""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import RequestException, Timeout

from rigor_v1.metadata.lumina_mcp import (
    LuminaMCPConfig,
    LuminaMCPClient,
    CircuitBreaker,
    LuminaResponse,
    _extract_first_json_object,
)


class TestLuminaMCPConfig:
    """Tests for LuminaMCPConfig dataclass."""

    def test_default_values(self):
        """Config should have sensible defaults."""
        cfg = LuminaMCPConfig(base_url="http://test.com", bearer_token="token")
        assert cfg.chat_path == "/chat"
        assert cfg.strict_json is True
        assert cfg.timeout_seconds == 30
        assert cfg.retry_count == 2

    def test_custom_values(self):
        """Config should accept custom values."""
        cfg = LuminaMCPConfig(
            base_url="http://test.com",
            bearer_token="token",
            chat_path="/api/chat",
            timeout_seconds=60,
            retry_count=5,
        )
        assert cfg.chat_path == "/api/chat"
        assert cfg.timeout_seconds == 60
        assert cfg.retry_count == 5

    def test_extra_headers(self):
        """Config should accept extra headers."""
        cfg = LuminaMCPConfig(
            base_url="http://test.com",
            bearer_token="token",
            extra_headers={"X-Custom": "value"},
        )
        assert cfg.extra_headers == {"X-Custom": "value"}


class TestCircuitBreaker:
    """Tests for CircuitBreaker class."""

    def test_starts_closed(self):
        """Circuit breaker should start in closed state."""
        cb = CircuitBreaker()
        assert cb.is_open is False
        assert cb.failure_count == 0

    def test_opens_after_threshold(self):
        """Circuit breaker should open after failure threshold reached."""
        cb = CircuitBreaker(failure_threshold=3)

        cb.record_failure()
        assert cb.is_open is False
        assert cb.failure_count == 1

        cb.record_failure()
        assert cb.is_open is False
        assert cb.failure_count == 2

        cb.record_failure()
        assert cb.is_open is True
        assert cb.failure_count == 3

    def test_success_resets_failure_count(self):
        """Recording success should reset failure count."""
        cb = CircuitBreaker(failure_threshold=3)

        cb.record_failure()
        cb.record_failure()
        assert cb.failure_count == 2

        cb.record_success()
        assert cb.failure_count == 0
        assert cb.is_open is False

    def test_success_closes_open_circuit(self):
        """Recording success should close an open circuit."""
        cb = CircuitBreaker(failure_threshold=2)

        cb.record_failure()
        cb.record_failure()
        assert cb.is_open is True

        cb.record_success()
        assert cb.is_open is False

    def test_manual_reset(self):
        """reset() should manually close the circuit."""
        cb = CircuitBreaker(failure_threshold=2)

        cb.record_failure()
        cb.record_failure()
        assert cb.is_open is True

        cb.reset()
        assert cb.is_open is False
        assert cb.failure_count == 0

    def test_half_open_after_timeout(self):
        """Circuit should allow test request after reset timeout."""
        cb = CircuitBreaker(failure_threshold=1, reset_timeout_seconds=0.1)

        cb.record_failure()
        assert cb.is_open is True

        # Wait for reset timeout
        time.sleep(0.15)

        # Should now be half-open (allow test request)
        assert cb.is_open is False

    def test_remains_open_before_timeout(self):
        """Circuit should remain open before reset timeout."""
        cb = CircuitBreaker(failure_threshold=1, reset_timeout_seconds=10)

        cb.record_failure()
        assert cb.is_open is True

        # Should still be open (no timeout elapsed)
        assert cb.is_open is True


class TestExtractFirstJsonObject:
    """Tests for _extract_first_json_object function."""

    def test_parses_valid_json(self):
        """Should parse valid JSON directly."""
        result = _extract_first_json_object('{"key": "value"}')
        assert result == {"key": "value"}

    def test_parses_json_array(self):
        """Should parse JSON arrays."""
        result = _extract_first_json_object('[1, 2, 3]')
        assert result == [1, 2, 3]

    def test_extracts_json_from_text(self):
        """Should extract JSON from surrounding text."""
        result = _extract_first_json_object('Some text {"key": "value"} more text')
        assert result == {"key": "value"}

    def test_raises_for_no_json(self):
        """Should raise ValueError when no JSON found."""
        with pytest.raises(ValueError, match="No JSON found"):
            _extract_first_json_object("no json here")

    def test_raises_for_invalid_json(self):
        """Should raise ValueError for invalid JSON blocks."""
        with pytest.raises(ValueError, match="failed to parse"):
            _extract_first_json_object('{invalid json}')

    def test_handles_whitespace(self):
        """Should handle whitespace around JSON."""
        result = _extract_first_json_object('  \n  {"key": "value"}  \n  ')
        assert result == {"key": "value"}


class TestLuminaMCPClient:
    """Tests for LuminaMCPClient class."""

    def make_client(self, **kwargs):
        """Create a test client with default config."""
        defaults = {
            "base_url": "http://test.com",
            "bearer_token": "test-token",
            "timeout_seconds": 30,
            "retry_count": 2,
        }
        defaults.update(kwargs)
        cfg = LuminaMCPConfig(**defaults)
        return LuminaMCPClient(cfg)

    def test_builds_correct_url(self):
        """Client should build correct URL from config."""
        client = self.make_client(base_url="http://test.com", chat_path="/api/chat")
        url = client._build_url()
        assert url == "http://test.com/api/chat"

    def test_builds_url_handles_trailing_slash(self):
        """Client should handle trailing slash in base_url."""
        client = self.make_client(base_url="http://test.com/", chat_path="/chat")
        url = client._build_url()
        assert url == "http://test.com/chat"

    def test_builds_headers_with_auth(self):
        """Client should include Authorization header."""
        client = self.make_client(bearer_token="my-token")
        headers = client._build_headers()
        assert headers["Authorization"] == "Bearer my-token"
        assert headers["Content-Type"] == "application/json"

    def test_builds_headers_without_token(self):
        """Client should work without bearer token."""
        client = self.make_client(bearer_token="")
        headers = client._build_headers()
        assert "Authorization" not in headers

    def test_builds_headers_with_extras(self):
        """Client should include extra headers."""
        client = self.make_client(extra_headers={"X-Custom": "value"})
        headers = client._build_headers()
        assert headers["X-Custom"] == "value"

    def test_builds_prompt_for_tables(self):
        """Client should build proper metadata prompt."""
        client = self.make_client()
        prompt = client._build_prompt(["TABLE1", "TABLE2"])
        assert "TABLE1" in prompt
        assert "TABLE2" in prompt
        assert "table_comments" in prompt
        assert "column_comments" in prompt

    def test_limits_tables_in_prompt(self):
        """Client should limit tables to 200 in prompt."""
        client = self.make_client()
        tables = [f"TABLE_{i}" for i in range(300)]
        prompt = client._build_prompt(tables)
        # First 200 should be included
        assert "TABLE_0" in prompt
        assert "TABLE_199" in prompt
        # Beyond 200 should not
        assert "TABLE_200" not in prompt

    def test_parses_valid_response(self):
        """Client should parse valid Lumina response."""
        client = self.make_client()
        raw = json.dumps({
            "table_comments": {"CUSTOMERS": "Customer data"},
            "column_comments": [
                {"table": "CUSTOMERS", "column": "ID", "comment": "Primary key"}
            ],
        })
        table_comments, column_comments = client._parse_response(raw)
        assert table_comments == {"CUSTOMERS": "Customer data"}
        assert column_comments == {("CUSTOMERS", "ID"): "Primary key"}

    def test_handles_empty_response(self):
        """Client should handle empty response gracefully."""
        client = self.make_client()
        raw = json.dumps({"table_comments": {}, "column_comments": []})
        table_comments, column_comments = client._parse_response(raw)
        assert table_comments == {}
        assert column_comments == {}

    def test_filters_invalid_comments(self):
        """Client should filter invalid comment entries."""
        client = self.make_client()
        raw = json.dumps({
            "table_comments": {
                "VALID": "comment",
                "": "empty key",
                "EMPTY": "",
            },
            "column_comments": [
                {"table": "T", "column": "C", "comment": "valid"},
                {"table": "", "column": "C", "comment": "empty table"},
                {"table": "T", "column": "", "comment": "empty column"},
                {"table": "T", "column": "C", "comment": ""},
                {"invalid": "entry"},
            ],
        })
        table_comments, column_comments = client._parse_response(raw)
        assert "VALID" in table_comments
        assert "" not in table_comments
        assert "EMPTY" not in table_comments
        assert ("T", "C") in column_comments
        assert len(column_comments) == 1

    @patch("rigor_v1.metadata.lumina_mcp.requests.post")
    def test_fetch_metadata_success(self, mock_post):
        """Client should return metadata on successful request."""
        mock_response = MagicMock()
        mock_response.text = json.dumps({
            "table_comments": {"TABLE1": "comment1"},
            "column_comments": [],
        })
        mock_post.return_value = mock_response

        client = self.make_client()
        table_comments, column_comments = client.fetch_metadata(["TABLE1"])

        assert table_comments == {"TABLE1": "comment1"}
        mock_post.assert_called_once()

    @patch("rigor_v1.metadata.lumina_mcp.requests.post")
    def test_fetch_metadata_retries_on_failure(self, mock_post):
        """Client should retry on transient failures."""
        # First call fails, second succeeds
        mock_post.side_effect = [
            RequestException("Network error"),
            MagicMock(text=json.dumps({"table_comments": {}, "column_comments": []})),
        ]

        client = self.make_client(retry_count=2)
        # Need to mock time.sleep to avoid delays
        with patch("rigor_v1.metadata.lumina_mcp.time.sleep"):
            table_comments, column_comments = client.fetch_metadata(["TABLE1"])

        assert mock_post.call_count == 2

    @patch("rigor_v1.metadata.lumina_mcp.requests.post")
    def test_fetch_metadata_returns_empty_after_retries(self, mock_post):
        """Client should return empty dicts after all retries exhausted."""
        mock_post.side_effect = RequestException("Network error")

        client = self.make_client(retry_count=2)
        with patch("rigor_v1.metadata.lumina_mcp.time.sleep"):
            table_comments, column_comments = client.fetch_metadata(["TABLE1"])

        assert table_comments == {}
        assert column_comments == {}
        # Should have tried 3 times (initial + 2 retries)
        assert mock_post.call_count == 3

    @patch("rigor_v1.metadata.lumina_mcp.requests.post")
    def test_fetch_metadata_trips_circuit_breaker(self, mock_post):
        """Client should trip circuit breaker after failures.

        Circuit opens when failure_count >= failure_threshold.
        For retry_count=2, threshold is 3 (retry_count + 1).
        Each fetch_metadata call that exhausts retries records ONE failure.
        """
        mock_post.side_effect = RequestException("Network error")

        client = self.make_client(retry_count=2)
        with patch("rigor_v1.metadata.lumina_mcp.time.sleep"):
            # Need multiple calls to trip the circuit
            # Each call exhausts retries and records one failure
            client.fetch_metadata(["TABLE1"])  # failure_count = 1
            client.fetch_metadata(["TABLE1"])  # failure_count = 2
            client.fetch_metadata(["TABLE1"])  # failure_count = 3, threshold met

        # Circuit should now be open
        assert client.circuit_breaker.is_open is True

    @patch("rigor_v1.metadata.lumina_mcp.requests.post")
    def test_fetch_metadata_skips_when_circuit_open(self, mock_post):
        """Client should return empty immediately when circuit is open."""
        client = self.make_client()
        client.circuit_breaker._is_open = True
        client.circuit_breaker._failure_count = 10

        table_comments, column_comments = client.fetch_metadata(["TABLE1"])

        assert table_comments == {}
        assert column_comments == {}
        mock_post.assert_not_called()

    @patch("rigor_v1.metadata.lumina_mcp.requests.post")
    def test_fetch_metadata_resets_circuit_on_success(self, mock_post):
        """Client should reset circuit breaker on success."""
        mock_response = MagicMock()
        mock_response.text = json.dumps({"table_comments": {}, "column_comments": []})
        mock_post.return_value = mock_response

        client = self.make_client()
        # Simulate partial failures
        client.circuit_breaker._failure_count = 2

        client.fetch_metadata(["TABLE1"])

        # Should be reset
        assert client.circuit_breaker.failure_count == 0

    @patch("rigor_v1.metadata.lumina_mcp.requests.post")
    def test_fetch_metadata_uses_configured_timeout(self, mock_post):
        """Client should use configured timeout."""
        mock_response = MagicMock()
        mock_response.text = json.dumps({"table_comments": {}, "column_comments": []})
        mock_post.return_value = mock_response

        client = self.make_client(timeout_seconds=45)
        client.fetch_metadata(["TABLE1"])

        # Check timeout was passed to requests.post
        call_kwargs = mock_post.call_args.kwargs
        assert call_kwargs["timeout"] == 45


class TestLuminaMCPClientWithResponse:
    """Tests for fetch_metadata_with_response method."""

    def make_client(self, **kwargs):
        """Create a test client with default config."""
        defaults = {
            "base_url": "http://test.com",
            "bearer_token": "test-token",
            "retry_count": 2,
        }
        defaults.update(kwargs)
        cfg = LuminaMCPConfig(**defaults)
        return LuminaMCPClient(cfg)

    @patch("rigor_v1.metadata.lumina_mcp.requests.post")
    def test_returns_success_response(self, mock_post):
        """Method should return successful LuminaResponse."""
        mock_response = MagicMock()
        mock_response.text = json.dumps({
            "table_comments": {"T": "comment"},
            "column_comments": [],
        })
        mock_post.return_value = mock_response

        client = self.make_client()
        response = client.fetch_metadata_with_response(["T"])

        assert response.success is True
        assert response.table_comments == {"T": "comment"}
        assert response.attempts == 1
        assert response.error is None

    @patch("rigor_v1.metadata.lumina_mcp.requests.post")
    def test_returns_failure_response(self, mock_post):
        """Method should return failed LuminaResponse."""
        mock_post.side_effect = RequestException("Network error")

        client = self.make_client(retry_count=0)
        response = client.fetch_metadata_with_response(["T"])

        assert response.success is False
        assert response.error is not None
        assert "Network error" in response.error
        assert response.attempts == 1

    def test_returns_circuit_open_response(self):
        """Method should return failure when circuit is open."""
        client = self.make_client()
        client.circuit_breaker._is_open = True

        response = client.fetch_metadata_with_response(["T"])

        assert response.success is False
        assert response.error == "Circuit breaker open"
        assert response.attempts == 0


class TestLuminaMCPClientHealthCheck:
    """Tests for health_check method."""

    def make_client(self, **kwargs):
        """Create a test client with default config."""
        defaults = {
            "base_url": "http://test.com",
            "bearer_token": "test-token",
        }
        defaults.update(kwargs)
        cfg = LuminaMCPConfig(**defaults)
        return LuminaMCPClient(cfg)

    @patch("rigor_v1.metadata.lumina_mcp.requests.post")
    def test_health_check_success(self, mock_post):
        """health_check should return True on success."""
        mock_post.return_value = MagicMock()

        client = self.make_client()
        assert client.health_check() is True

    @patch("rigor_v1.metadata.lumina_mcp.requests.post")
    def test_health_check_failure(self, mock_post):
        """health_check should return False on failure."""
        mock_post.side_effect = RequestException("Failed")

        client = self.make_client()
        assert client.health_check() is False

    def test_health_check_circuit_open(self):
        """health_check should return False when circuit is open."""
        client = self.make_client()
        client.circuit_breaker._is_open = True

        assert client.health_check() is False


class TestExponentialBackoff:
    """Tests for exponential backoff behavior."""

    @patch("rigor_v1.metadata.lumina_mcp.requests.post")
    @patch("rigor_v1.metadata.lumina_mcp.time.sleep")
    def test_backoff_delays_increase(self, mock_sleep, mock_post):
        """Backoff delays should increase exponentially."""
        mock_post.side_effect = RequestException("Error")

        cfg = LuminaMCPConfig(
            base_url="http://test.com",
            bearer_token="token",
            retry_count=3,
        )
        client = LuminaMCPClient(cfg)
        client.fetch_metadata(["TABLE"])

        # Check sleep was called with increasing delays
        sleep_calls = [call.args[0] for call in mock_sleep.call_args_list]
        # Should be approximately [1.0, 2.0, 4.0] (capped at 30)
        assert len(sleep_calls) == 3
        assert sleep_calls[0] == 1.0
        assert sleep_calls[1] == 2.0
        assert sleep_calls[2] == 4.0

    @patch("rigor_v1.metadata.lumina_mcp.requests.post")
    @patch("rigor_v1.metadata.lumina_mcp.time.sleep")
    def test_backoff_capped_at_30_seconds(self, mock_sleep, mock_post):
        """Backoff delays should be capped at 30 seconds."""
        mock_post.side_effect = RequestException("Error")

        cfg = LuminaMCPConfig(
            base_url="http://test.com",
            bearer_token="token",
            retry_count=10,  # High retry count to test cap
        )
        client = LuminaMCPClient(cfg)
        client.fetch_metadata(["TABLE"])

        sleep_calls = [call.args[0] for call in mock_sleep.call_args_list]
        # All delays should be <= 30
        assert all(d <= 30.0 for d in sleep_calls)


class TestLuminaResponse:
    """Tests for LuminaResponse dataclass."""

    def test_success_response(self):
        """Should create successful response."""
        response = LuminaResponse(
            success=True,
            table_comments={"T": "comment"},
            column_comments={("T", "C"): "comment"},
            attempts=2,
        )
        assert response.success is True
        assert response.error is None

    def test_failure_response(self):
        """Should create failed response."""
        response = LuminaResponse(
            success=False,
            error="Network error",
            attempts=3,
        )
        assert response.success is False
        assert response.error == "Network error"
        assert response.table_comments == {}
        assert response.column_comments == {}
