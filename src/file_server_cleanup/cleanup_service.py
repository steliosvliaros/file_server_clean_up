from __future__ import annotations

from .config import CleanupConfig
from .logging_config import get_logger
from .models import CleanupSummary


class CleanupService:
    def __init__(self, config: CleanupConfig) -> None:
        self.config = config
        self.logger = get_logger(self.__class__.__name__)

    def run(self) -> CleanupSummary:
        self.logger.info("Starting cleanup workflow", extra={"root": str(self.config.root_path)})

        summary = CleanupSummary()

        # TODO: implement scan and candidate collection.
        # TODO: implement rename planning rules.
        # TODO: implement CSV audit export.
        # TODO: execute or simulate operations based on dry_run.

        self.logger.info("Cleanup workflow finished")
        return summary
