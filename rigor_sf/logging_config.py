"""Structured logging configuration for the RIGOR-SF pipeline.

Provides centralized logging setup per SPEC_V2.md §2.3.
Supports:
- Console output with configurable level
- File logging to run directory
- Structured log format with timestamps
- Log level configuration via config.yaml

Usage:
    from rigor_sf.logging_config import setup_logging, get_logger

    # At pipeline startup
    logger = setup_logging(run_dir="runs/2026-01-01T12-00-00Z", debug=True)

    # In modules
    logger = get_logger(__name__)
    logger.info("Processing table: %s", table_name)
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# Module-level logger registry
_loggers: dict[str, logging.Logger] = {}
_root_logger: Optional[logging.Logger] = None
_log_file_path: Optional[Path] = None


class RigorLogFormatter(logging.Formatter):
    """Custom formatter with consistent timestamp and level formatting.

    Format: YYYY-MM-DD HH:MM:SS [LEVEL] module: message
    """

    def __init__(self, include_timestamp: bool = True):
        self.include_timestamp = include_timestamp
        if include_timestamp:
            fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
            datefmt = "%Y-%m-%d %H:%M:%S"
        else:
            fmt = "[%(levelname)s] %(name)s: %(message)s"
            datefmt = None
        super().__init__(fmt=fmt, datefmt=datefmt)

    def format(self, record: logging.LogRecord) -> str:
        # Shorten module names for cleaner output
        # rigor_sf.pipeline -> pipeline
        if record.name.startswith("rigor_sf."):
            record.name = record.name[9:]
        elif record.name.startswith("rigor."):
            record.name = record.name[6:]
        return super().format(record)


class PhaseLogFilter(logging.Filter):
    """Filter that adds phase context to log records."""

    def __init__(self, phase: Optional[str] = None):
        super().__init__()
        self.phase = phase

    def filter(self, record: logging.LogRecord) -> bool:
        record.phase = self.phase or "main"
        return True


def setup_logging(
    run_dir: Optional[str | Path] = None,
    debug: bool = False,
    log_to_console: bool = True,
    log_to_file: bool = True,
    console_level: Optional[int] = None,
    file_level: Optional[int] = None,
) -> logging.Logger:
    """Configure structured logging for the pipeline.

    Args:
        run_dir: Directory for log file (creates pipeline_<timestamp>.log)
        debug: If True, set log level to DEBUG
        log_to_console: Enable console output
        log_to_file: Enable file logging (requires run_dir)
        console_level: Override console log level
        file_level: Override file log level

    Returns:
        Root logger for the rigor package
    """
    global _root_logger, _log_file_path

    # Determine log levels
    base_level = logging.DEBUG if debug else logging.INFO
    console_lvl = console_level if console_level is not None else base_level
    file_lvl = file_level if file_level is not None else logging.DEBUG

    # Get or create root rigor logger
    root = logging.getLogger("rigor")
    root.setLevel(logging.DEBUG)  # Allow all messages, filter at handlers

    # Clear existing handlers
    root.handlers.clear()

    # Console handler
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(console_lvl)
        console_handler.setFormatter(RigorLogFormatter(include_timestamp=False))
        root.addHandler(console_handler)

    # File handler
    if log_to_file and run_dir:
        run_path = Path(run_dir)
        run_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        log_path = run_path / f"pipeline_{timestamp}.log"

        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(file_lvl)
        file_handler.setFormatter(RigorLogFormatter(include_timestamp=True))
        root.addHandler(file_handler)
        _log_file_path = log_path

    # Prevent propagation to root logger
    root.propagate = False

    _root_logger = root
    return root


def get_logger(name: str) -> logging.Logger:
    """Get a logger for the given module name.

    If setup_logging() hasn't been called, returns a logger with
    a basic console handler to ensure messages aren't lost.

    Args:
        name: Module name (typically __name__)

    Returns:
        Logger instance

    Example:
        logger = get_logger(__name__)
        logger.info("Starting process")
    """
    global _loggers

    # Normalize name to rigor namespace
    if not name.startswith("rigor"):
        if name.startswith("rigor_sf"):
            name = "rigor." + name[9:].lstrip(".")
        else:
            name = f"rigor.{name}"

    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)

    # If root logger not configured, add a basic handler
    root = logging.getLogger("rigor")
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        handler.setFormatter(RigorLogFormatter(include_timestamp=False))
        root.addHandler(handler)
        root.setLevel(logging.INFO)

    _loggers[name] = logger
    return logger


def get_log_file_path() -> Optional[Path]:
    """Get the current log file path, if file logging is enabled.

    Returns:
        Path to log file, or None if file logging not configured
    """
    return _log_file_path


def set_phase(phase: str) -> None:
    """Set the current pipeline phase for log context.

    Args:
        phase: Phase name (e.g., "query-gen", "infer", "generate")
    """
    root = logging.getLogger("rigor")
    # Remove existing phase filters
    root.filters = [f for f in root.filters if not isinstance(f, PhaseLogFilter)]
    root.addFilter(PhaseLogFilter(phase))


def log_exception(
    logger: logging.Logger,
    message: str,
    exc: Exception,
    *args,
    level: int = logging.ERROR,
) -> None:
    """Log an exception with full context.

    Args:
        logger: Logger instance
        message: Log message (with optional format args)
        exc: Exception to log
        args: Format arguments for message
        level: Log level (default ERROR)
    """
    # Format the message if args provided
    if args:
        formatted_msg = message % args
    else:
        formatted_msg = message

    # Log with exception info
    logger.log(level, "%s: %s", formatted_msg, exc, exc_info=True)


def configure_from_config(cfg: "AppConfig", run_dir: Optional[str | Path] = None) -> logging.Logger:
    """Configure logging from AppConfig settings.

    This is a convenience function that extracts debug settings from config.

    Args:
        cfg: Application configuration
        run_dir: Optional run directory for file logging

    Returns:
        Configured root logger
    """
    debug = getattr(cfg.llm, "debug", False)
    return setup_logging(run_dir=run_dir, debug=debug)


# Convenience aliases for common log levels
def debug(msg: str, *args, **kwargs) -> None:
    """Log a debug message to the root rigor logger."""
    get_logger("rigor").debug(msg, *args, **kwargs)


def info(msg: str, *args, **kwargs) -> None:
    """Log an info message to the root rigor logger."""
    get_logger("rigor").info(msg, *args, **kwargs)


def warning(msg: str, *args, **kwargs) -> None:
    """Log a warning message to the root rigor logger."""
    get_logger("rigor").warning(msg, *args, **kwargs)


def error(msg: str, *args, **kwargs) -> None:
    """Log an error message to the root rigor logger."""
    get_logger("rigor").error(msg, *args, **kwargs)


# Phase-prefixed logging helpers
class PhaseLogger:
    """Logger wrapper that prefixes messages with phase context.

    Usage:
        log = PhaseLogger("generate")
        log.info("Processing table: %s", table_name)
        # Output: [INFO] rigor.pipeline: [generate] Processing table: CUSTOMERS
    """

    def __init__(self, phase: str, logger: Optional[logging.Logger] = None):
        self.phase = phase
        self._logger = logger or get_logger("rigor.pipeline")

    def _format(self, msg: str) -> str:
        return f"[{self.phase}] {msg}"

    def debug(self, msg: str, *args, **kwargs) -> None:
        self._logger.debug(self._format(msg), *args, **kwargs)

    def info(self, msg: str, *args, **kwargs) -> None:
        self._logger.info(self._format(msg), *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> None:
        self._logger.warning(self._format(msg), *args, **kwargs)

    def error(self, msg: str, *args, **kwargs) -> None:
        self._logger.error(self._format(msg), *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs) -> None:
        self._logger.exception(self._format(msg), *args, **kwargs)
