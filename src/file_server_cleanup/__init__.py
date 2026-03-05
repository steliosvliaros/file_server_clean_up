"""Core package for file server cleanup logic.

This package is intentionally scaffolded as a clean starting point.
Implement cleanup workflows in dedicated modules and keep notebook cells thin.
"""

from .config import CleanupConfig
from .cleanup_service import CleanupService
from .inventory import (
	FileInventoryResult,
	MoveDuplicatesResult,
	export_file_inventory_csv,
	move_duplicated_files_from_csv,
)

__all__ = [
	"CleanupConfig",
	"CleanupService",
	"FileInventoryResult",
	"MoveDuplicatesResult",
	"export_file_inventory_csv",
	"move_duplicated_files_from_csv",
]
