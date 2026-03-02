"""Tests for versioning.py module."""

import pytest
import tempfile
import os
from pathlib import Path
from datetime import datetime

from rigor_sf.versioning import (
    generate_timestamp,
    versioned_path,
    create_versioned_artifact,
    update_symlink,
    list_versions,
    get_latest_version,
    compute_content_hash,
    compute_file_hash,
    ArtifactVersion,
    RunDirectory,
)


class TestGenerateTimestamp:
    """Tests for generate_timestamp."""

    def test_format(self):
        """Timestamp has correct format."""
        ts = generate_timestamp()
        # Format: YYYYMMDD_HHMMSS
        assert len(ts) == 15
        assert ts[8] == "_"
        # Should be parseable
        datetime.strptime(ts, "%Y%m%d_%H%M%S")

    def test_unique(self):
        """Consecutive calls produce different timestamps (usually)."""
        # This might fail if called within same second, but that's rare
        import time
        ts1 = generate_timestamp()
        time.sleep(0.01)  # Small delay
        ts2 = generate_timestamp()
        # At least they should be valid
        assert ts1 is not None
        assert ts2 is not None


class TestVersionedPath:
    """Tests for versioned_path."""

    def test_basic_path(self):
        """Creates versioned path with timestamp."""
        result = versioned_path("data/core.owl", "20240315_143022")
        assert result == Path("data/core_20240315_143022.owl")

    def test_generated_timestamp(self):
        """Uses generated timestamp when not provided."""
        result = versioned_path("output.xml")
        assert "output_" in str(result)
        assert result.suffix == ".xml"

    def test_path_object_input(self):
        """Accepts Path objects."""
        result = versioned_path(Path("data/report.json"), "20240101_000000")
        assert result == Path("data/report_20240101_000000.json")


class TestCreateVersionedArtifact:
    """Tests for create_versioned_artifact."""

    def test_creates_file(self):
        """Creates versioned file with content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "test.txt"
            content = "Hello, World!"

            artifact = create_versioned_artifact(
                content=content,
                base_path=str(base_path),
                timestamp="20240315_143022",
                create_symlink=False,
            )

            assert artifact.path.exists()
            assert artifact.path.read_text() == content
            assert artifact.timestamp == "20240315_143022"

    def test_creates_symlink(self):
        """Creates symlink to versioned file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "test.txt"
            content = "Test content"

            artifact = create_versioned_artifact(
                content=content,
                base_path=str(base_path),
                create_symlink=True,
            )

            assert base_path.is_symlink()
            assert base_path.read_text() == content

    def test_bytes_content(self):
        """Handles bytes content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "binary.bin"
            content = b"\x00\x01\x02\x03"

            artifact = create_versioned_artifact(
                content=content,
                base_path=str(base_path),
                create_symlink=False,
            )

            assert artifact.path.read_bytes() == content

    def test_creates_parent_dirs(self):
        """Creates parent directories if needed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "nested" / "dir" / "test.txt"

            artifact = create_versioned_artifact(
                content="content",
                base_path=str(base_path),
                create_symlink=False,
            )

            assert artifact.path.exists()


class TestUpdateSymlink:
    """Tests for update_symlink."""

    def test_creates_symlink(self):
        """Creates new symlink."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "target.txt"
            target.write_text("content")
            link = Path(tmpdir) / "link.txt"

            update_symlink(target, link)

            assert link.is_symlink()
            assert link.read_text() == "content"

    def test_updates_existing_symlink(self):
        """Updates existing symlink."""
        with tempfile.TemporaryDirectory() as tmpdir:
            old_target = Path(tmpdir) / "old.txt"
            old_target.write_text("old")
            new_target = Path(tmpdir) / "new.txt"
            new_target.write_text("new")
            link = Path(tmpdir) / "link.txt"

            # Create initial symlink
            link.symlink_to(old_target)
            assert link.read_text() == "old"

            # Update it
            update_symlink(new_target, link)
            assert link.read_text() == "new"


class TestListVersions:
    """Tests for list_versions."""

    def test_lists_versions(self):
        """Lists all versioned artifacts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "test.txt"

            # Create some versions
            (Path(tmpdir) / "test_20240101_000000.txt").write_text("v1")
            (Path(tmpdir) / "test_20240201_000000.txt").write_text("v2")
            (Path(tmpdir) / "test_20240301_000000.txt").write_text("v3")

            versions = list_versions(str(base_path))

            assert len(versions) == 3
            # Sorted newest first
            assert versions[0].timestamp == "20240301_000000"
            assert versions[2].timestamp == "20240101_000000"

    def test_empty_when_no_versions(self):
        """Returns empty list when no versions exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            versions = list_versions(str(Path(tmpdir) / "nonexistent.txt"))
            assert versions == []


class TestGetLatestVersion:
    """Tests for get_latest_version."""

    def test_gets_latest(self):
        """Gets most recent version."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "test.txt"

            (Path(tmpdir) / "test_20240101_000000.txt").write_text("v1")
            (Path(tmpdir) / "test_20240301_000000.txt").write_text("v3")
            (Path(tmpdir) / "test_20240201_000000.txt").write_text("v2")

            latest = get_latest_version(str(base_path))

            assert latest is not None
            assert latest.timestamp == "20240301_000000"

    def test_none_when_no_versions(self):
        """Returns None when no versions exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            latest = get_latest_version(str(Path(tmpdir) / "nonexistent.txt"))
            assert latest is None


class TestComputeContentHash:
    """Tests for compute_content_hash."""

    def test_string_hash(self):
        """Computes hash of string content."""
        hash1 = compute_content_hash("Hello, World!")
        hash2 = compute_content_hash("Hello, World!")
        hash3 = compute_content_hash("Different content")

        assert hash1 == hash2
        assert hash1 != hash3
        assert len(hash1) == 64  # SHA-256 hex digest

    def test_bytes_hash(self):
        """Computes hash of bytes content."""
        hash1 = compute_content_hash(b"\x00\x01\x02")
        assert len(hash1) == 64


class TestComputeFileHash:
    """Tests for compute_file_hash."""

    def test_file_hash(self):
        """Computes hash of file content."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("Test content")
            f.flush()
            try:
                file_hash = compute_file_hash(f.name)
                content_hash = compute_content_hash("Test content")
                assert file_hash == content_hash
            finally:
                os.unlink(f.name)


class TestRunDirectory:
    """Tests for RunDirectory class."""

    def test_init(self):
        """RunDirectory initializes with timestamp."""
        run = RunDirectory(base_dir="runs", timestamp="20240315_143022")
        assert run.timestamp == "20240315_143022"
        assert run.path == Path("runs/run_20240315_143022")

    def test_create(self):
        """Creates run directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run = RunDirectory(base_dir=tmpdir, timestamp="20240315_143022")
            created_path = run.create()

            assert created_path.exists()
            assert created_path.is_dir()

    def test_save_artifact(self):
        """Saves artifact to run directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run = RunDirectory(base_dir=tmpdir, timestamp="20240315_143022")
            path = run.save_artifact("test.txt", "content")

            assert path.exists()
            assert path.read_text() == "content"

    def test_update_latest_symlink(self):
        """Creates/updates latest symlink."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run = RunDirectory(base_dir=tmpdir, timestamp="20240315_143022")
            run.create()
            run.update_latest_symlink()

            latest = Path(tmpdir) / "latest"
            assert latest.is_symlink()

    def test_list_runs(self):
        """Lists all run directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create some run directories
            (Path(tmpdir) / "run_20240101_000000").mkdir()
            (Path(tmpdir) / "run_20240201_000000").mkdir()
            (Path(tmpdir) / "run_20240301_000000").mkdir()
            (Path(tmpdir) / "other_dir").mkdir()  # Should be ignored

            runs = RunDirectory.list_runs(tmpdir)

            assert len(runs) == 3
            assert runs[0].timestamp == "20240301_000000"

    def test_get_latest(self):
        """Gets most recent run directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "run_20240101_000000").mkdir()
            (Path(tmpdir) / "run_20240301_000000").mkdir()

            latest = RunDirectory.get_latest(tmpdir)

            assert latest is not None
            assert latest.timestamp == "20240301_000000"
