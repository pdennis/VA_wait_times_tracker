from dataclasses import dataclass
from datetime import date, datetime
from typing import Self

from psycopg import Connection
from psycopg.rows import class_row


@dataclass
class FacilityStaging:
    station_id: str
    facility: str
    website: str
    address: str
    state: str
    phone: str
    mailing_address: str
    normalized_address: str
    address_parts: tuple


@dataclass
class Station:
    station_id: str
    prefix: str
    legacy: bool
    active: bool
    awol: bool
    total_failures: int
    last_report: datetime | None
    last_failure: datetime | None
    created: datetime
    updated: datetime


@dataclass
class StationReport:
    def __init__(
        self,
        report_id: int,
        station_id: str,
        file_name: str,
        size: int,
        report: bytes,
        report_hash: bytes,
        downloaded: datetime,
    ):
        self.report_id: int = report_id
        self.station_id: str = station_id
        self.file_name: str = file_name
        self.size: int = size
        self.report: bytes = report
        self.report_hash: bytes = report_hash
        self.downloaded: datetime = downloaded

    def insert(self, conn: Connection) -> Self:
        with conn.cursor(row_factory=class_row(StationReport)) as cur:
            cur.execute(
                """
                    insert into station_report
                        (station_id, file_name, size, report, report_hash, downloaded)
                        values (%s, %s, %s, %s, %s, now())  returning *;
                    """,
                (
                    self.station_id,
                    self.file_name,
                    self.size,
                    self.report,
                    self.report_hash,
                ),
            )
            report = cur.fetchone()
            return report


@dataclass
class WaitTimeReport:
    station_id: str
    report_id: int
    report_date: date
    appointment_type: str
    established: float
    new: float
    source: str

    def insert(self, conn: Connection) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    insert into wait_time_report
                        (station_id, report_id, report_date, appointment_type, established, new, source)
                        values (%s, %s, %s, %s, %s, %s, %s);
                    """,
                (
                    self.station_id,
                    self.report_id,
                    self.report_date,
                    self.appointment_type,
                    self.established,
                    self.new,
                    self.source,
                ),
            )


@dataclass
class SatisfactionReport:
    station_id: str
    report_id: int
    report_date: date
    appointment_type: str
    percent: float

    def insert(self, conn: Connection) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    insert into satisfaction_report
                        (station_id, report_id, report_date, appointment_type, percent)
                        values (%s, %s, %s, %s, %s);
                    """,
                (self.station_id, self.report_id, self.report_date, self.appointment_type, self.percent),
            )
