from dataclasses import dataclass
from datetime import date, datetime


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
    last_report: datetime
    last_failure: datetime
    created: datetime
    updated: datetime


@dataclass
class StationReport:
    id: int
    station_id: str
    file_name: str
    size: int
    report: bytes
    hash: bytes
    downloaded: datetime


@dataclass
class WaitTimeReport:
    report_id: int
    station_id: str
    report_date: date
    appointment_type: str
    established: float
    new: float
    source: str


@dataclass
class SatisfactionReport:
    report_id: int
    station_id: str
    report_date: date
    appointment_type: str
    percent: float
