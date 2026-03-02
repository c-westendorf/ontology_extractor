"""Artifact versioning for the RIGOR-SF pipeline.

Implements timestamp-based versioning with symlinks per SPEC_V2.md §6.
"""

from __future__ import annotations

import hashlib
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import NamedTuple


class ArtifactVersion(NamedTuple):
    """Represents a versioned artifact."""

    path: Path
    timestamp: str
    base_name: str


def generate_timestamp() -> str:
    """Generate an ISO 8601 timestamp suffix.

    Returns:
        Timestamp string in format YYYYMMDD_HHMMSS
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def versioned_path(base_path: str | Path, timestamp: str | None = None) -> Path:
    """Create a versioned path with timestamp suffix.

    Args:
        base_path: Original file path (e.g., 'data/core.owl')
        timestamp: Optional timestamp; generated if not provided

    Returns:
        Versioned path (e.g., 'data/core_20240315_143022.owl')
    """
    path = Path(base_path)
    ts = timestamp or generate_timestamp()
    stem = path.stem
    suffix = path.suffix
    return path.parent / f"{stem}_{ts}{suffix}"


def create_versioned_artifact(
    content: str | bytes,
    base_path: str | Path,
    timestamp: str | None = None,
    create_symlink: bool = True,
) -> ArtifactVersion:
    """Create a versioned artifact file with optional symlink.

    Args:
        content: File content (string or bytes)
        base_path: Base path for the artifact
        timestamp: Optional timestamp; generated if not provided
        create_symlink: If True, create/update symlink at base_path

    Returns:
        ArtifactVersion with path and metadata
    """
    ts = timestamp or generate_timestamp()
    vpath = versioned_path(base_path, ts)

    # Ensure parent directory exists
    vpath.parent.mkdir(parents=True, exist_ok=True)

    # Write content
    mode = "wb" if isinstance(content, bytes) else "w"
    encoding = None if isinstance(content, bytes) else "utf-8"
    with open(vpath, mode, encoding=encoding) as f:
        f.write(content)

    # Create/update symlink
    if create_symlink:
        update_symlink(vpath, Path(base_path))

    return ArtifactVersion(
        path=vpath,
        timestamp=ts,
        base_name=Path(base_path).name,
    )


def update_symlink(target: Path, link_path: Path) -> None:
    """Create or update a symlink to point to target.

    Args:
        target: The file the symlink should point to
        link_path: The symlink path to create/update
    """
    # Remove existing symlink or file
    if link_path.is_symlink() or link_path.exists():
        link_path.unlink()

    # Create relative symlink
    rel_target = os.path.relpath(target, link_path.parent)
    link_path.symlink_to(rel_target)


def list_versions(base_path: str | Path) -> list[ArtifactVersion]:
    """List all versioned artifacts for a base path.

    Args:
        base_path: Base path (e.g., 'data/core.owl')

    Returns:
        List of ArtifactVersion sorted by timestamp (newest first)
    """
    path = Path(base_path)
    parent = path.parent
    stem = path.stem
    suffix = path.suffix

    if not parent.exists():
        return []

    versions = []
    pattern = f"{stem}_*{suffix}"

    for file_path in parent.glob(pattern):
        # Extract timestamp from filename
        name = file_path.stem
        if name.startswith(f"{stem}_"):
            ts = name[len(stem) + 1 :]
            versions.append(
                ArtifactVersion(
                    path=file_path,
                    timestamp=ts,
                    base_name=path.name,
                )
            )

    # Sort by timestamp descending (newest first)
    return sorted(versions, key=lambda v: v.timestamp, reverse=True)


def get_latest_version(base_path: str | Path) -> ArtifactVersion | None:
    """Get the most recent versioned artifact.

    Args:
        base_path: Base path (e.g., 'data/core.owl')

    Returns:
        Latest ArtifactVersion or None if no versions exist
    """
    versions = list_versions(base_path)
    return versions[0] if versions else None


def compute_content_hash(content: str | bytes) -> str:
    """Compute SHA-256 hash of content.

    Args:
        content: Content to hash

    Returns:
        Hex digest of SHA-256 hash
    """
    if isinstance(content, str):
        content = content.encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def compute_file_hash(path: str | Path) -> str:
    """Compute SHA-256 hash of a file.

    Args:
        path: Path to file

    Returns:
        Hex digest of SHA-256 hash
    """
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


class RunDirectory:
    """Manages a timestamped run directory for query-gen artifacts.

    Per SPEC_V2.md §6, each run gets a directory like:
    runs/run_20240315_143022/
    """

    def __init__(self, base_dir: str | Path = "runs", timestamp: str | None = None):
        """Initialize a run directory.

        Args:
            base_dir: Base directory for runs
            timestamp: Optional timestamp; generated if not provided
        """
        self.base_dir = Path(base_dir)
        self.timestamp = timestamp or generate_timestamp()
        self.path = self.base_dir / f"run_{self.timestamp}"

    def create(self) -> Path:
        """Create the run directory.

        Returns:
            Path to the created directory
        """
        self.path.mkdir(parents=True, exist_ok=True)
        return self.path

    def save_artifact(self, name: str, content: str | bytes) -> Path:
        """Save an artifact to the run directory.

        Args:
            name: Artifact filename
            content: File content

        Returns:
            Path to saved file
        """
        self.create()
        file_path = self.path / name

        mode = "wb" if isinstance(content, bytes) else "w"
        encoding = None if isinstance(content, bytes) else "utf-8"
        with open(file_path, mode, encoding=encoding) as f:
            f.write(content)

        return file_path

    def update_latest_symlink(self) -> None:
        """Create/update 'latest' symlink pointing to this run."""
        latest_link = self.base_dir / "latest"
        update_symlink(self.path, latest_link)

    @classmethod
    def list_runs(cls, base_dir: str | Path = "runs") -> list["RunDirectory"]:
        """List all run directories.

        Args:
            base_dir: Base directory for runs

        Returns:
            List of RunDirectory instances sorted by timestamp (newest first)
        """
        base = Path(base_dir)
        if not base.exists():
            return []

        runs = []
        for entry in base.iterdir():
            if entry.is_dir() and entry.name.startswith("run_"):
                ts = entry.name[4:]  # Remove "run_" prefix
                run = cls(base_dir, ts)
                run.path = entry
                runs.append(run)

        return sorted(runs, key=lambda r: r.timestamp, reverse=True)

    @classmethod
    def get_latest(cls, base_dir: str | Path = "runs") -> "RunDirectory | None":
        """Get the most recent run directory.

        Args:
            base_dir: Base directory for runs

        Returns:
            Latest RunDirectory or None
        """
        runs = cls.list_runs(base_dir)
        return runs[0] if runs else None
