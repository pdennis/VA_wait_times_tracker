import os
from datetime import datetime
from pathlib import Path

import psycopg

from ..config.settings import DATABASE_URL, get_bridge_logger
from ..db.download_reports import DownloadReports
from ..db.models import Station, StationReport

logger = get_bridge_logger(__name__)

RENAMED = {
    "Cadillac VA Clinic": "Duane E. Dewey VA Clinic",
}


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
        dir_date = relevant_dir.split("_")[-1]
        report_date = datetime.strptime(dir_date, "%Y-%m-%d")
        with psycopg.connect(self.database_url) as conn:
            file_count = 0
            unknown_prefix = 0
            germane = 0
            for report in sorted(Path(relevant_dir).glob("*.xlsx")):
                file_count += 1
                prefix = report.stem.split(" - ")[0].strip()
                station = Station.by_prefix(prefix, conn)
                if station is None and prefix in RENAMED:
                    prefix = RENAMED.get(prefix, prefix)
                    station = Station.by_prefix(prefix, conn)
                if station is None:
                    logger.info(f"Skipping report {dir_date} {report.name} - station not found")
                    unknown_prefix += 1
                    continue
                file_name = report.name.split('";')[0].strip()
                logger.info(f"Processing report in {file_name}...")
                with open(report.resolve(), "rb") as file:
                    byte_data = file.read()
                sha = DownloadReports.hash_excel_data(file_name, byte_data)
                if sha:
                    report = StationReport(
                        report_id=0,
                        station_id=station.station_id,
                        file_name=file_name,
                        size=len(byte_data),
                        report=byte_data,
                        report_hash=sha[0],
                        downloaded=report_date,
                    )
                    try:
                        report = report.insert(conn)
                        conn.commit()
                    except psycopg.errors.UniqueViolation:
                        conn.rollback()
                        logger.info(f"Report {report.file_name} already stored, continuing...")
                    np = DownloadReports.process_excel_sheets(report, sha[1], conn)
                    logger.info(f"Processed {np} sheet(s) for report {file_name}")
                    germane += 1
            logger.info(
                f"Processed {file_count} report(s) in {relevant_dir}; "
                f"germane: {germane}, unknown prefix: {unknown_prefix}"
            )
