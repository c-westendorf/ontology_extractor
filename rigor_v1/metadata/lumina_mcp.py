"""Lumina MCP client for metadata enrichment.

Provides HTTP client for Lumina LLM endpoint with:
- Circuit breaker for fault tolerance
- Configurable retry logic with exponential backoff
- Structured logging
- Graceful degradation on failures

Per SPEC_V2.md §2.4, this module handles Lumina errors gracefully,
returning empty metadata on failure rather than crashing the pipeline.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

import requests
from requests.exceptions import RequestException

from ..logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class LuminaMCPConfig:
    """Configuration for Lumina MCP client.

    Attributes:
        base_url: Base URL of Lumina endpoint
        bearer_token: Authentication token
        chat_path: API path for chat endpoint
        extra_headers: Additional HTTP headers
        strict_json: If True, enforce JSON parsing
        timeout_seconds: Request timeout (default 30s per config.py)
        retry_count: Number of retries before circuit breaker trips (default 2)
    """

    base_url: str
    bearer_token: str
    chat_path: str = "/chat"
    extra_headers: Dict[str, str] | None = None
    strict_json: bool = True
    timeout_seconds: int = 30
    retry_count: int = 2


@dataclass
class CircuitBreaker:
    """Circuit breaker for fault tolerance.

    Prevents cascading failures by tracking consecutive failures
    and opening the circuit after a threshold is reached.

    States:
    - CLOSED: Normal operation, requests allowed
    - OPEN: Failures exceeded threshold, requests blocked
    - HALF_OPEN: After reset timeout, allow one test request

    Attributes:
        failure_threshold: Number of failures before opening circuit
        reset_timeout_seconds: Time before attempting recovery
    """

    failure_threshold: int = 3
    reset_timeout_seconds: float = 60.0

    # State tracking (private)
    _failure_count: int = field(default=0, repr=False)
    _is_open: bool = field(default=False, repr=False)
    _last_failure_time: Optional[float] = field(default=None, repr=False)

    @property
    def is_open(self) -> bool:
        """Check if circuit is open (blocking requests).

        Also handles half-open state: if reset_timeout has passed,
        returns False to allow a test request.
        """
        if not self._is_open:
            return False

        # Check if we should try half-open
        if self._last_failure_time is not None:
            elapsed = time.time() - self._last_failure_time
            if elapsed >= self.reset_timeout_seconds:
                return False  # Allow test request (half-open)

        return True

    @property
    def failure_count(self) -> int:
        """Current consecutive failure count."""
        return self._failure_count

    def record_failure(self) -> None:
        """Record a failure and potentially open the circuit."""
        self._failure_count += 1
        self._last_failure_time = time.time()

        if self._failure_count >= self.failure_threshold:
            if not self._is_open:
                logger.warning(
                    "Lumina circuit breaker OPEN after %d consecutive failures",
                    self._failure_count,
                )
            self._is_open = True

    def record_success(self) -> None:
        """Record a success and reset the circuit."""
        if self._is_open:
            logger.info("Lumina circuit breaker CLOSED after successful request")
        self._failure_count = 0
        self._is_open = False
        self._last_failure_time = None

    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        self._failure_count = 0
        self._is_open = False
        self._last_failure_time = None


@dataclass
class LuminaResponse:
    """Response from Lumina metadata fetch.

    Attributes:
        success: Whether the request succeeded
        table_comments: Mapping of table name to comment
        column_comments: Mapping of (table, column) to comment
        error: Error message if failed
        attempts: Number of attempts made
    """

    success: bool
    table_comments: Dict[str, str] = field(default_factory=dict)
    column_comments: Dict[Tuple[str, str], str] = field(default_factory=dict)
    error: Optional[str] = None
    attempts: int = 1


def _extract_first_json_object(text: str) -> Any:
    """Best-effort extraction of the first JSON object or array from a text response.

    Args:
        text: Raw text response that may contain JSON

    Returns:
        Parsed JSON object

    Raises:
        ValueError: If no valid JSON found
    """
    text = text.strip()

    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Look for JSON block pattern
    m = re.search(r"(\{.*\}|\[.*\])", text, flags=re.S)
    if not m:
        raise ValueError("No JSON found in Lumina response.")

    candidate = m.group(1)
    try:
        return json.loads(candidate)
    except json.JSONDecodeError as e:
        raise ValueError(f"Found JSON-like block but failed to parse: {e}") from e


class LuminaMCPClient:
    """HTTP client for a Lumina endpoint that behaves like an LLM.

    Features:
    - Circuit breaker for fault tolerance
    - Retry logic with exponential backoff
    - Configurable timeout
    - Graceful degradation (returns empty metadata on failure)

    Contract we enforce via prompting:
    Returns JSON with:
      {
        "table_comments": {"TABLE": "comment", ...},
        "column_comments": [{"table":"T","column":"C","comment":"..."}, ...]
      }

    We then normalize to:
      - Dict[table, comment]
      - Dict[(table, column), comment]
    """

    def __init__(self, cfg: LuminaMCPConfig):
        """Initialize client with configuration.

        Args:
            cfg: Lumina MCP configuration
        """
        self.cfg = cfg
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=cfg.retry_count + 1,  # Open after retry_count failures
            reset_timeout_seconds=60.0,
        )

    def _build_url(self) -> str:
        """Build full URL for Lumina endpoint."""
        return self.cfg.base_url.rstrip("/") + "/" + self.cfg.chat_path.lstrip("/")

    def _build_headers(self) -> Dict[str, str]:
        """Build HTTP headers for request."""
        headers = {"Content-Type": "application/json"}
        if self.cfg.bearer_token:
            headers["Authorization"] = f"Bearer {self.cfg.bearer_token}"
        if self.cfg.extra_headers:
            headers.update(self.cfg.extra_headers)
        return headers

    def _post(self, prompt: str) -> str:
        """Make HTTP POST request to Lumina.

        Args:
            prompt: Prompt text to send

        Returns:
            Raw response text

        Raises:
            RequestException: On HTTP errors
        """
        url = self._build_url()
        headers = self._build_headers()
        payload = {"prompt": prompt}

        response = requests.post(
            url,
            headers=headers,
            data=json.dumps(payload),
            timeout=self.cfg.timeout_seconds,
        )
        response.raise_for_status()
        return response.text

    def _parse_response(self, raw: str) -> Tuple[Dict[str, str], Dict[Tuple[str, str], str]]:
        """Parse Lumina response into metadata dictionaries.

        Args:
            raw: Raw response text

        Returns:
            Tuple of (table_comments, column_comments)

        Raises:
            ValueError: If response cannot be parsed
        """
        data = _extract_first_json_object(raw) if self.cfg.strict_json else raw

        if isinstance(data, str):
            data = _extract_first_json_object(data)

        if not isinstance(data, dict):
            raise ValueError("Lumina JSON must be an object at top-level.")

        table_comments: Dict[str, str] = {}
        column_comments: Dict[Tuple[str, str], str] = {}

        # Parse table comments
        tc = data.get("table_comments") or {}
        if isinstance(tc, dict):
            for k, v in tc.items():
                if isinstance(k, str) and isinstance(v, str) and k.strip() and v.strip():
                    table_comments[k.strip()] = v.strip()

        # Parse column comments
        cc = data.get("column_comments") or []
        if isinstance(cc, list):
            for row in cc:
                if not isinstance(row, dict):
                    continue
                t = (row.get("table") or "").strip()
                c = (row.get("column") or "").strip()
                com = (row.get("comment") or "").strip()
                if t and c and com:
                    column_comments[(t, c)] = com

        return table_comments, column_comments

    def fetch_metadata(
        self, table_names: list[str]
    ) -> Tuple[Dict[str, str], Dict[Tuple[str, str], str]]:
        """Fetch metadata for tables from Lumina.

        Implements retry logic with exponential backoff and circuit breaker.
        On failure, returns empty dictionaries to allow pipeline to continue.

        Args:
            table_names: List of table names to fetch metadata for

        Returns:
            Tuple of (table_comments, column_comments)
            Returns empty dicts on failure (graceful degradation)
        """
        # Check circuit breaker
        if self.circuit_breaker.is_open:
            logger.warning(
                "Lumina circuit breaker open, returning empty metadata for %d tables",
                len(table_names),
            )
            return {}, {}

        prompt = self._build_prompt(table_names)

        # Retry loop with exponential backoff
        last_error: Optional[str] = None
        for attempt in range(self.cfg.retry_count + 1):
            try:
                logger.debug(
                    "Lumina request attempt %d/%d for %d tables",
                    attempt + 1,
                    self.cfg.retry_count + 1,
                    len(table_names),
                )

                raw = self._post(prompt)
                table_comments, column_comments = self._parse_response(raw)

                # Success - record and return
                self.circuit_breaker.record_success()
                logger.info(
                    "Lumina metadata fetched: %d table comments, %d column comments",
                    len(table_comments),
                    len(column_comments),
                )
                return table_comments, column_comments

            except RequestException as e:
                last_error = f"HTTP error: {e}"
                logger.warning(
                    "Lumina request failed (attempt %d/%d): %s",
                    attempt + 1,
                    self.cfg.retry_count + 1,
                    last_error,
                )

            except ValueError as e:
                last_error = f"Parse error: {e}"
                logger.warning(
                    "Lumina response parse failed (attempt %d/%d): %s",
                    attempt + 1,
                    self.cfg.retry_count + 1,
                    last_error,
                )

            except Exception as e:
                last_error = f"Unexpected error: {e}"
                logger.warning(
                    "Lumina request failed unexpectedly (attempt %d/%d): %s",
                    attempt + 1,
                    self.cfg.retry_count + 1,
                    last_error,
                )

            # Calculate backoff delay (exponential: 1s, 2s, 4s, ...)
            if attempt < self.cfg.retry_count:
                delay = min(1.0 * (2.0 ** attempt), 30.0)
                logger.debug("Retrying Lumina request in %.1fs...", delay)
                time.sleep(delay)

        # All retries exhausted
        self.circuit_breaker.record_failure()
        logger.error(
            "Lumina metadata fetch failed after %d attempts: %s. Returning empty metadata.",
            self.cfg.retry_count + 1,
            last_error,
        )
        return {}, {}

    def fetch_metadata_with_response(self, table_names: list[str]) -> LuminaResponse:
        """Fetch metadata with detailed response information.

        Same as fetch_metadata but returns LuminaResponse with status info.

        Args:
            table_names: List of table names to fetch metadata for

        Returns:
            LuminaResponse with success status, metadata, and error info
        """
        # Check circuit breaker
        if self.circuit_breaker.is_open:
            return LuminaResponse(
                success=False,
                error="Circuit breaker open",
                attempts=0,
            )

        prompt = self._build_prompt(table_names)
        attempts = 0

        for attempt in range(self.cfg.retry_count + 1):
            attempts = attempt + 1
            try:
                raw = self._post(prompt)
                table_comments, column_comments = self._parse_response(raw)
                self.circuit_breaker.record_success()

                return LuminaResponse(
                    success=True,
                    table_comments=table_comments,
                    column_comments=column_comments,
                    attempts=attempts,
                )

            except Exception as e:
                if attempt < self.cfg.retry_count:
                    delay = min(1.0 * (2.0 ** attempt), 30.0)
                    time.sleep(delay)

                if attempt == self.cfg.retry_count:
                    self.circuit_breaker.record_failure()
                    return LuminaResponse(
                        success=False,
                        error=str(e),
                        attempts=attempts,
                    )

        # Should not reach here, but just in case
        return LuminaResponse(success=False, error="Unknown error", attempts=attempts)

    def _build_prompt(self, table_names: list[str]) -> str:
        """Build prompt for Lumina metadata request.

        Args:
            table_names: List of table names

        Returns:
            Formatted prompt string
        """
        # Limit to 200 tables to avoid token limits
        tn = ", ".join(table_names[:200])
        return f"""You are a metadata assistant. I have a Snowflake database with tables:
{tn}

Return ONLY valid JSON (no markdown) with this schema:
{{
  "table_comments": {{"TABLE_NAME": "short description", "...": "..."}},
  "column_comments": [
    {{"table": "TABLE_NAME", "column": "COLUMN_NAME", "comment": "short description"}}
  ]
}}

Rules:
- Use exact table/column names as provided.
- If you don't know a comment, omit it (do not guess).
- Keep comments concise (<= 20 words).
""".strip()

    def health_check(self) -> bool:
        """Check if Lumina endpoint is healthy.

        Returns:
            True if endpoint responds successfully, False otherwise
        """
        if self.circuit_breaker.is_open:
            return False

        try:
            # Simple health check with minimal prompt
            self._post("ping")
            return True
        except Exception:
            return False
