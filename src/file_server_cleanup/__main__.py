from __future__ import annotations

from pathlib import Path

from .cleanup_service import CleanupService
from .config import CleanupConfig
from .logging_config import setup_logging


def main() -> None:
    config = CleanupConfig(root_path=Path("."), dry_run=True)
    setup_logging(level=config.log_level)
    service = CleanupService(config)
    service.run()


if __name__ == "__main__":
    main()
