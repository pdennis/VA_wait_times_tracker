from dataclasses import dataclass
from datetime import datetime
from threading import Thread
from time import sleep

import psycopg
import requests
from psycopg import Connection
from psycopg.rows import class_row
from requests import Response

from ..config.settings import DATABASE_URL, get_bridge_logger

VA_REPORT_URL = "https://www.accesstocare.va.gov/FacilityPerformanceData/FacilityDataExcel?stationNumber={}"
STATION_QUERY = "select * from station where coalesce(active, true) = True order by station_id;"


logger = get_bridge_logger(__name__)


@dataclass
class Station:
    station_id: str
    prefix: str
    active: bool
    awol: bool
    total_failures: int
    last_report: datetime
    last_failure: datetime
    created: datetime
    updated: datetime


class DownloadReports(Thread):
    def __init__(self, pause: float = 2) -> None:
        self.database_url = DATABASE_URL
        if not self.database_url:
            raise ValueError("Database URL is required")

        super().__init__(daemon=True, name="DownloadReports")
        self.pause = pause
        self.start()

    def run(self) -> None:
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor(row_factory=class_row(Station)) as cur:
                cur.execute(STATION_QUERY)
                for row in cur:
                    logger.info(f"Downloading report for station: {row.station_id}...")
                    try:
                        response = requests.get(VA_REPORT_URL.format(row.station_id))
                        content_type = response.headers.get("Content-Type", None)
                        if content_type and "spreadsheetml.sheet" in content_type:
                            self.process_report(row, response, conn)
                        else:
                            self.process_failure(row, response, conn)
                    except Exception as e:
                        print(f"Error downloading station {row.station_id}: {e}")
                    # don't overload VA servers
                    sleep(self.pause)

    def process_report(self, row: Station, response: Response, conn: Connection) -> None:
        logger.info(f"Processing report for station: {row.station_id}...")
        cd_dict = self.parse_content_disposition(response.headers.get("Content-Disposition", None))
        if not row.prefix and cd_dict.get("prefix", None):
            row.prefix = cd_dict["prefix"]
        row.last_report = datetime.now()
        row.awol = False
        row.active = True
        with conn.cursor() as cur:
            cur.execute(
                """
                    update station
                        set prefix = %s, awol = false, active = true, last_report = Now(), updated = Now()
                        where station_id = %s;
                    """,
                (row.prefix, row.station_id),
            )
            conn.commit()

    @staticmethod
    def process_failure(row: Station, response: Response, conn: Connection) -> None:
        content_type = response.headers.get("Content-Type", None)
        logger.info(f"Skipping station {row.station_id} - received {content_type}")
        row.total_failures += 1
        row.last_failure = datetime.now()
        if content_type and "text/html" in content_type:
            row.active = False
        with conn.cursor() as cur:
            cur.execute(
                "update station set active = %s, total_failures = %s, last_failure = %s where station_id = %s;",
                (row.active, row.total_failures, row.last_failure, row.station_id),
            )
            conn.commit()

    @staticmethod
    def parse_content_disposition(content_disposition: str) -> dict[str, str]:
        cd = {}
        if content_disposition:
            for part in content_disposition.split(";"):
                if "=" in part:
                    key, value = part.split("=", 1)
                    cd[key.strip()] = value.strip("\"'")
            filename = cd.get("filename", None)
            if filename is not None:
                # get the first part of the file name; it will be used to process legacy files
                # noinspection PyUnresolvedReferences
                cd["prefix"] = filename.split(" - ", 1)[0].strip()
        return cd
