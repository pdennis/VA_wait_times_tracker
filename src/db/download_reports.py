from __future__ import annotations

import hashlib
import io
import math
from datetime import datetime
from threading import Thread
from time import sleep

import pandas as pd
import psycopg
import requests
from pandas import DataFrame, ExcelFile
from psycopg import Connection
from psycopg.rows import class_row
from requests import Response

from ..config.settings import DATABASE_URL, get_bridge_logger
from .models import SatisfactionReport, Station, StationReport, WaitTimeReport

VA_REPORT_URL = "https://www.accesstocare.va.gov/FacilityPerformanceData/FacilityDataExcel?stationNumber={}"
ALL_STATIONS_QUERY = "select * from station where germane = true and coalesce(active, true) = True order by station_id;"
STATION_QUERY = "select * from station where station_id = %s;"


logger = get_bridge_logger(__name__)


class DownloadReports(Thread):
    def __init__(self, station_id: str = None, pause: float = 2) -> None:
        self.database_url = DATABASE_URL
        if not self.database_url:
            raise ValueError("Database URL is required")

        super().__init__(daemon=True, name="DownloadReports")
        self.pause = pause
        if station_id:
            with psycopg.connect(self.database_url) as conn:
                with conn.cursor(row_factory=class_row(Station)) as cur:
                    cur.execute(STATION_QUERY, (station_id,))
                    for row in cur:
                        self.get_station_report(row, conn)
        else:
            self.start()

    def run(self) -> None:
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor(row_factory=class_row(Station)) as cur:
                cur.execute(ALL_STATIONS_QUERY)
                for row in cur:
                    self.get_station_report(row, conn)
                    # don't overload VA servers
                    sleep(self.pause)

    def get_station_report(self, row: Station, conn: Connection):
        logger.info(f"Downloading report for station: {row.station_id}...")
        response = None
        try:
            response = requests.get(VA_REPORT_URL.format(row.station_id))
            content_type = response.headers.get("Content-Type", None)
            if content_type and "spreadsheetml.sheet" in content_type:
                self.process_report(row, response, conn)
            else:
                self.process_failure(row, response, conn)
        except Exception as e:
            sc = f"Status: {response.status_code}" if response else "N/A"
            logger.exception(f"Error downloading station {row.station_id}: {sc} {e}", exc_info=True)
            if response:
                self.process_failure(row, response, conn)

    def process_report(self, row: Station, response: Response, conn: Connection) -> None:
        logger.info(f"Processing report for station: {row.station_id}...")
        cd_dict = self.parse_content_disposition(response.headers.get("Content-Disposition", None))
        prefix_updated = False
        if not row.prefix and cd_dict.get("prefix", None):
            row.prefix = cd_dict["prefix"]
            prefix_updated = True
        with conn.cursor(row_factory=class_row(StationReport)) as cur:
            try:
                sha = self.hash_excel_data(cd_dict["filename"], response.content)
                if sha:
                    report = StationReport(
                        report_id=0,
                        station_id=row.station_id,
                        file_name=cd_dict["filename"],
                        size=int(response.headers["Content-Length"]),
                        report=response.content,
                        report_hash=sha[0],
                        downloaded=datetime.now(),
                    )
                    report = report.insert(conn)
                    row.awol = False
                    row.active = True
                    cur.execute(
                        """
                            update station
                                set prefix = %s, awol = false, active = true, germane=true,
                                    last_report = now(), updated = now()
                                where station_id = %s;
                            """,
                        (row.prefix, row.station_id),
                    )
                    conn.commit()

                    # Save Excel workbook sheet(s), as appropriate
                    np = self.process_excel_sheets(report, sha[1], conn)
                else:
                    np = 0
                    if prefix_updated:
                        cur.execute(
                            """
                                update station
                                    set prefix = %s , germane = false, last_report = Now(), updated = Now()
                                    where station_id = %s;
                            """,
                            (row.prefix, row.station_id),
                        )
                    else:
                        # we must enclose query parameters in parens to make it a tuple
                        cur.execute(
                            """
                                update station
                                    set germane = false, last_report = Now(), updated = Now()
                                    where station_id = %s;
                            """,
                            (row.station_id,),
                        )
                    conn.commit()
                logger.info(f"Processed {np} sheet(s) for station {row.station_id}")
            except psycopg.errors.UniqueViolation:
                conn.rollback()
                logger.info(f"Report for station {row.station_id} already processed, ignoring...")

    @staticmethod
    def process_failure(row: Station, response: Response, conn: Connection) -> None:
        try:
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
        except Exception as e:
            conn.rollback()  # Roll back the transaction if anything goes wrong
            logger.exception(f"Error in process_failure for station {row.station_id}: {e}")
            raise  # Re-raise the exception after rolling back

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

    @staticmethod
    def hash_excel_data(file_name: str, excel_bytes: bytes) -> tuple[bytes, ExcelFile] | None:
        """
        Calculates a hash of an Excel workbook, ignoring most properties
        by hashing the data in each sheet. This is necessary because the
        file creation date is included in the properties, causing files
        with the same data to have different hashes.

        Returns:
            str: A hexadecimal hash string.
        """
        of_interest_workbook = False
        all_sheet_data = [file_name.encode("utf-8")]
        try:
            xls = pd.ExcelFile(io.BytesIO(excel_bytes))
            for sheet_name in xls.sheet_names:
                if sheet_name.lower() in OF_INTEREST_SHEETS:
                    of_interest_workbook = True
                df = pd.read_excel(xls, sheet_name=sheet_name)
                all_sheet_data.append(df.to_string().encode("utf-8"))
            if of_interest_workbook is False:
                return None
        except Exception as e:
            print(f"Error reading excel file: {e}")
            return None

        # Combine the string representations of all sheets and compute hash
        return hashlib.sha512(b"".join(all_sheet_data), usedforsecurity=False).digest(), xls

    @staticmethod
    def process_excel_sheets(report: StationReport, workbook: ExcelFile, conn: Connection) -> int:
        num_processed = 0
        for sheet_name in workbook.sheet_names:
            handler = OF_INTEREST_SHEETS.get(sheet_name.lower(), None)
            if handler:
                logger.info(f"Processing Excel sheet: {sheet_name}...")
                df = pd.read_excel(workbook, sheet_name=sheet_name)
                handler(report, df, conn)
                num_processed += 1
        return num_processed

    @staticmethod
    def process_wait_times(report: StationReport, df: DataFrame, conn: Connection):
        for index, row in df.iterrows():
            wtr = WaitTimeReport(
                report.station_id,
                report.report_id,
                row.ReportDate.date(),
                row.AppointmentType,
                row.EstablishedPatients,
                row.NewPatients,
                row.DataSource
            )
            wtr.insert(conn)
        conn.commit()

    @staticmethod
    def process_satisfaction_with_care(report: StationReport, df: DataFrame, conn: Connection):
        for index, row in df.iterrows():
            # massage Percent value
            if isinstance(row.Percent, str):
                percent = float(row.Percent.strip("%"))
            elif row.Percent and math.isnan(row.Percent):
                percent = None
            else:
                percent = float(row.Percent)
            sr = SatisfactionReport(
                report.station_id,
                report.report_id,
                row.ReportDate.date(),
                row.AppointmentType,
                percent,
            )
            sr.insert(conn)
        conn.commit()


# Rather than use an if/elif/else loop, associate the handler method to persist
# an Excel sheet in the dict below. This dict doubles as a means of determining
# if a given workbook is of interest to us.
OF_INTEREST_SHEETS = {
    "wait times": DownloadReports.process_wait_times,
    "satisfaction with care": DownloadReports.process_satisfaction_with_care,
}