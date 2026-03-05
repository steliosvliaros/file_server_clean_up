from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class RenameOperation:
    source: Path
    target: Path
    status: str
    error: str = ""


@dataclass(slots=True)
class CleanupSummary:
    scanned_files: int = 0
    planned_renames: int = 0
    renamed_files: int = 0
    failed_files: int = 0
