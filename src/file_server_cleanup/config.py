from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class CleanupConfig:
    root_path: Path
    output_dir: Path = Path("outputs")
    dry_run: bool = True
    include_hidden: bool = False
    skip_dirs_starting_with: str = "_"
    rename_prefix: str = "HTL00p049-01_CN_DRWTEC_"
    log_level: str = "INFO"
    extra_exclude_dir_names: set[str] = field(default_factory=set)
