import os
from pathlib import Path

import psycopg

from ..config.settings import DATABASE_URL, get_bridge_logger
from ..db.models import Station

logger = get_bridge_logger(__name__)


class LegacyReportLoader:
    def __init__(self, report_path: str = ".", prefix: str = "va_facility_data") -> None:
        self.database_url = DATABASE_URL
        if not self.database_url:
            raise ValueError("Database URL is required")

        # get the list of relevant directories
        self.dirs = [
            entry
            for entry in os.listdir(report_path)
            if os.path.isdir(os.path.join(report_path, entry)) and entry.startswith(prefix)
        ]

        # sort in ascending order
        self.dirs.sort()

        # process the reports in each directory
        for d in self.dirs:
            self.process_directory(d)

    def process_directory(self, relevant_dir: str):
        logger.info(f"Processing reports in directory: {relevant_dir}")
        with psycopg.connect(self.database_url) as conn:
            for report in Path(relevant_dir).glob("*.xlsx"):
                prefix = report.stem.split(" - ")[0].strip()
                station = Station.by_prefix(prefix, conn)
                if station is None:
                    logger.info(f"Skipping report {report.name} - station not found")
                    continue
                file_name = report.name.split('";')[0].strip()
                logger.info(f"Processing report in {file_name}...")
