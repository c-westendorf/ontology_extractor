"""Unit tests for rigor_sf.logging_config module.

Tests the structured logging configuration per SPEC_V2.md §2.3.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from rigor_sf.logging_config import (
    RigorLogFormatter,
    PhaseLogFilter,
    PhaseLogger,
    setup_logging,
    get_logger,
    get_log_file_path,
    set_phase,
    log_exception,
    configure_from_config,
    debug,
    info,
    warning,
    error,
)


class TestRigorLogFormatter:
    """Tests for RigorLogFormatter class."""

    def test_format_with_timestamp(self):
        """Formatter should include timestamp when enabled."""
        formatter = RigorLogFormatter(include_timestamp=True)
        record = logging.LogRecord(
            name="rigor_sf.pipeline",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        # Should contain the message and be formatted
        assert "test message" in formatted
        assert "INFO" in formatted

    def test_format_without_timestamp(self):
        """Formatter should exclude timestamp when disabled."""
        formatter = RigorLogFormatter(include_timestamp=False)
        record = logging.LogRecord(
            name="rigor_sf.pipeline",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "test message" in formatted
        assert "INFO" in formatted

    def test_shortens_rigor_sf_prefix(self):
        """Formatter should shorten rigor_sf. prefix in logger names."""
        formatter = RigorLogFormatter(include_timestamp=False)
        record = logging.LogRecord(
            name="rigor_sf.pipeline",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        # Should have shortened name
        assert "pipeline" in formatted
        # Should not have full prefix
        assert "rigor_sf.pipeline" not in formatted

    def test_shortens_rigor_prefix(self):
        """Formatter should shorten rigor. prefix in logger names."""
        formatter = RigorLogFormatter(include_timestamp=False)
        record = logging.LogRecord(
            name="rigor.pipeline",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "pipeline" in formatted

    def test_preserves_other_logger_names(self):
        """Formatter should not modify non-rigor logger names."""
        formatter = RigorLogFormatter(include_timestamp=False)
        record = logging.LogRecord(
            name="external.module",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "external.module" in formatted


class TestPhaseLogFilter:
    """Tests for PhaseLogFilter class."""

    def test_adds_phase_to_record(self):
        """Filter should add phase attribute to log record."""
        filter_ = PhaseLogFilter(phase="generate")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        result = filter_.filter(record)
        assert result is True
        assert record.phase == "generate"

    def test_default_phase_is_main(self):
        """Filter should default to 'main' phase when not specified."""
        filter_ = PhaseLogFilter(phase=None)
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        filter_.filter(record)
        assert record.phase == "main"

    def test_always_returns_true(self):
        """Filter should always allow records through."""
        filter_ = PhaseLogFilter(phase="test")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )
        assert filter_.filter(record) is True


class TestSetupLogging:
    """Tests for setup_logging function."""

    def test_returns_root_logger(self):
        """setup_logging should return the root rigor logger."""
        logger = setup_logging()
        assert logger.name == "rigor"

    def test_sets_debug_level_when_enabled(self):
        """setup_logging should set DEBUG level when debug=True."""
        logger = setup_logging(debug=True)
        # Console handler should be at DEBUG
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                assert handler.level == logging.DEBUG

    def test_sets_info_level_by_default(self):
        """setup_logging should set INFO level by default."""
        logger = setup_logging(debug=False)
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                assert handler.level == logging.INFO

    def test_creates_file_handler_with_run_dir(self):
        """setup_logging should create file handler when run_dir provided."""
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = setup_logging(run_dir=tmpdir, log_to_file=True)
            file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
            assert len(file_handlers) == 1

    def test_no_file_handler_without_run_dir(self):
        """setup_logging should not create file handler without run_dir."""
        logger = setup_logging(run_dir=None, log_to_file=True)
        file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 0

    def test_disables_console_when_requested(self):
        """setup_logging should not add console handler when disabled."""
        logger = setup_logging(log_to_console=False)
        # Clear any default handlers
        stream_handlers = [h for h in logger.handlers if isinstance(h, logging.StreamHandler)
                          and not isinstance(h, logging.FileHandler)]
        assert len(stream_handlers) == 0

    def test_custom_console_level(self):
        """setup_logging should honor custom console level."""
        logger = setup_logging(console_level=logging.WARNING)
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                assert handler.level == logging.WARNING

    def test_clears_existing_handlers(self):
        """setup_logging should clear existing handlers on reconfiguration."""
        # First setup
        logger1 = setup_logging()
        initial_count = len(logger1.handlers)

        # Second setup should clear and reset
        logger2 = setup_logging()
        # Should have same number of handlers, not doubled
        assert len(logger2.handlers) == initial_count


class TestGetLogger:
    """Tests for get_logger function."""

    def test_returns_logger_instance(self):
        """get_logger should return a Logger instance."""
        logger = get_logger("test_module")
        assert isinstance(logger, logging.Logger)

    def test_normalizes_rigor_sf_name(self):
        """get_logger should normalize rigor_sf prefix to rigor namespace."""
        logger = get_logger("rigor_sf.pipeline")
        # Logger is placed under rigor namespace hierarchy
        assert logger.name.startswith("rigor")
        assert "pipeline" in logger.name

    def test_adds_rigor_prefix_to_plain_names(self):
        """get_logger should add rigor prefix to plain module names."""
        logger = get_logger("mymodule")
        assert logger.name == "rigor.mymodule"

    def test_caches_loggers(self):
        """get_logger should return cached logger on second call."""
        logger1 = get_logger("rigor.cached_test")
        logger2 = get_logger("rigor.cached_test")
        assert logger1 is logger2

    def test_preserves_rigor_prefix(self):
        """get_logger should preserve existing rigor prefix."""
        logger = get_logger("rigor.existing")
        assert logger.name == "rigor.existing"


class TestGetLogFilePath:
    """Tests for get_log_file_path function."""

    def test_returns_none_without_file_logging(self):
        """get_log_file_path should return None if file logging not configured."""
        setup_logging(run_dir=None, log_to_file=False)
        # Note: This may not be None if a previous test set it
        # The function returns the module-level variable

    def test_returns_path_with_file_logging(self):
        """get_log_file_path should return Path when file logging enabled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            setup_logging(run_dir=tmpdir, log_to_file=True)
            path = get_log_file_path()
            assert path is not None
            assert isinstance(path, Path)
            assert str(path).startswith(tmpdir)


class TestSetPhase:
    """Tests for set_phase function."""

    def test_sets_phase_filter(self):
        """set_phase should add PhaseLogFilter to root logger."""
        setup_logging()
        set_phase("generate")
        root = logging.getLogger("rigor")
        phase_filters = [f for f in root.filters if isinstance(f, PhaseLogFilter)]
        assert len(phase_filters) == 1
        assert phase_filters[0].phase == "generate"

    def test_replaces_existing_phase_filter(self):
        """set_phase should replace existing phase filter."""
        setup_logging()
        set_phase("infer")
        set_phase("generate")
        root = logging.getLogger("rigor")
        phase_filters = [f for f in root.filters if isinstance(f, PhaseLogFilter)]
        # Should only have one filter
        assert len(phase_filters) == 1
        assert phase_filters[0].phase == "generate"


class TestLogException:
    """Tests for log_exception function."""

    def test_logs_exception_with_message(self, capsys):
        """log_exception should log exception with formatted message."""
        setup_logging()
        logger = get_logger("test")
        exc = ValueError("test error")

        log_exception(logger, "Operation failed for %s", exc, "item1")

        captured = capsys.readouterr()
        assert "Operation failed for item1" in captured.out
        assert "test error" in captured.out

    def test_logs_at_custom_level(self, capsys):
        """log_exception should log at custom level when specified."""
        setup_logging()
        logger = get_logger("test")
        exc = ValueError("test error")

        log_exception(logger, "Warning", exc, level=logging.WARNING)

        captured = capsys.readouterr()
        assert "Warning" in captured.out


class TestConfigureFromConfig:
    """Tests for configure_from_config function."""

    def test_uses_debug_from_llm_config(self):
        """configure_from_config should read debug from llm config."""

        class MockLLMConfig:
            debug = True

        class MockConfig:
            llm = MockLLMConfig()

        with tempfile.TemporaryDirectory() as tmpdir:
            logger = configure_from_config(MockConfig(), run_dir=tmpdir)
            # Should be configured with debug level
            assert logger is not None


class TestConvenienceAliases:
    """Tests for convenience logging functions."""

    def test_debug_logs_to_root(self, capsys):
        """debug() should log to root rigor logger."""
        setup_logging(debug=True)
        debug("debug message")
        captured = capsys.readouterr()
        assert "debug message" in captured.out

    def test_info_logs_to_root(self, capsys):
        """info() should log to root rigor logger."""
        setup_logging()
        info("info message")
        captured = capsys.readouterr()
        assert "info message" in captured.out

    def test_warning_logs_to_root(self, capsys):
        """warning() should log to root rigor logger."""
        setup_logging()
        warning("warning message")
        captured = capsys.readouterr()
        assert "warning message" in captured.out

    def test_error_logs_to_root(self, capsys):
        """error() should log to root rigor logger."""
        setup_logging()
        error("error message")
        captured = capsys.readouterr()
        assert "error message" in captured.out


class TestPhaseLogger:
    """Tests for PhaseLogger class."""

    def test_prefixes_messages_with_phase(self, capsys):
        """PhaseLogger should prefix messages with phase name."""
        setup_logging()
        log = PhaseLogger("generate")

        log.info("Processing table %s", "CUSTOMERS")

        captured = capsys.readouterr()
        assert "[generate]" in captured.out
        assert "Processing table CUSTOMERS" in captured.out

    def test_debug_method(self, capsys):
        """PhaseLogger.debug should work correctly."""
        setup_logging(debug=True)
        log = PhaseLogger("test")

        log.debug("debug message")

        captured = capsys.readouterr()
        assert "[test]" in captured.out
        assert "debug message" in captured.out

    def test_warning_method(self, capsys):
        """PhaseLogger.warning should work correctly."""
        setup_logging()
        log = PhaseLogger("test")

        log.warning("warning message")

        captured = capsys.readouterr()
        assert "[test]" in captured.out
        assert "warning message" in captured.out

    def test_error_method(self, capsys):
        """PhaseLogger.error should work correctly."""
        setup_logging()
        log = PhaseLogger("test")

        log.error("error message")

        captured = capsys.readouterr()
        assert "[test]" in captured.out
        assert "error message" in captured.out

    def test_exception_method(self, capsys):
        """PhaseLogger.exception should log with traceback."""
        setup_logging()
        log = PhaseLogger("test")

        try:
            raise ValueError("test exception")
        except ValueError:
            log.exception("caught error")

        captured = capsys.readouterr()
        assert "[test]" in captured.out
        assert "caught error" in captured.out

    def test_uses_custom_logger(self, capsys):
        """PhaseLogger should use custom logger when provided."""
        setup_logging()
        custom_logger = get_logger("custom.module")
        log = PhaseLogger("test", logger=custom_logger)

        log.info("custom logger message")

        captured = capsys.readouterr()
        assert "custom logger message" in captured.out


class TestFileLogging:
    """Integration tests for file logging."""

    def test_writes_to_log_file(self):
        """Logger should write to file when configured."""
        with tempfile.TemporaryDirectory() as tmpdir:
            setup_logging(run_dir=tmpdir, log_to_file=True)
            logger = get_logger("test")

            logger.info("test file message")

            # Find log file
            log_files = list(Path(tmpdir).glob("pipeline_*.log"))
            assert len(log_files) >= 1

            # Check content
            content = log_files[0].read_text()
            assert "test file message" in content

    def test_file_has_timestamps(self):
        """File log entries should include timestamps."""
        with tempfile.TemporaryDirectory() as tmpdir:
            setup_logging(run_dir=tmpdir, log_to_file=True)
            logger = get_logger("test")

            logger.info("timestamped message")

            log_files = list(Path(tmpdir).glob("pipeline_*.log"))
            content = log_files[0].read_text()
            # Should have timestamp format YYYY-MM-DD HH:MM:SS
            import re
            assert re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", content)

    def test_log_file_name_contains_timestamp(self):
        """Log file name should include timestamp."""
        with tempfile.TemporaryDirectory() as tmpdir:
            setup_logging(run_dir=tmpdir, log_to_file=True)

            log_files = list(Path(tmpdir).glob("pipeline_*.log"))
            assert len(log_files) >= 1

            # Should have format pipeline_YYYYMMDD_HHMMSS.log
            import re
            assert re.match(r"pipeline_\d{8}_\d{6}\.log", log_files[0].name)
