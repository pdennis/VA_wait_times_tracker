from dataclasses import dataclass
from pathlib import Path

import psycopg

from ..config.settings import DATABASE_URL, get_bridge_logger

logger = get_bridge_logger(__name__)

STAGING_COPY = """
    COPY facility_staging (station_id, facility, website, address, state, phone)
    FROM STDIN WITH (FORMAT CSV, HEADER)
"""

UPDATE_FAC = """
    update facility_staging
    set address = strip_mailing_address(address), mailing_address = extract_mailing_address(address)
    where position('Mailing Address: ' in address) > 0
"""

# noinspection SqlWithoutWhere
NORMALIZE_ADDRESSES = """
    update facility_staging
    set normalized_address = tiger.pprint_addy(tiger.normalize_address(replace(replace(address, '(', ' '), ')', ' '))),
        address_parts = tiger.normalize_address(replace(replace(address, '(', ' '), ')', ' ')),
        phones = ARRAY(SELECT TRIM(both ' ' from unnest(string_to_array(COALESCE(phone, ''), 'Or'))));
"""


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


class UpdateFacility:
    def __init__(self, facility_csv: str) -> None:
        self.database_url = DATABASE_URL
        if not self.database_url:
            raise ValueError("Database URL is required")

        # make sure we have a good facility csv file
        if not facility_csv:
            raise ValueError("facility CSV file-path is required")
        facility_csv = Path(facility_csv)
        if not facility_csv.exists() and facility_csv.suffix != ".csv":
            facility_csv = facility_csv.with_suffix(".csv")
        if not facility_csv.exists():
            raise FileNotFoundError(f"facility CSV file not found: {facility_csv}")
        self.facility_csv = Path(facility_csv)

        # load new data into staging table
        self.load_staging_table()

        # normalize addresses
        self.post_process_staging_table()

        # update facility table with new data
        self.update_facility_table()

        # update station table with new stations, if any
        self.update_station_table()

    def load_staging_table(self):
        """
        Load VA facility information from CSV into the staging table.
        """
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                # truncate the table to avoid duplicates
                logger.info("Truncating the facility_staging table...")
                cur.execute("truncate table facility_staging;")

                # load the data from the csv file
                logger.info(f"Loading facility_staging table from {self.facility_csv}...")
                with open(self.facility_csv, "r") as f:
                    with cur.copy(STAGING_COPY) as copy:
                        while data := f.read(1024):
                            copy.write(data)
            conn.commit()

    def post_process_staging_table(self):
        """
        Postprocess the data in the staging table, normalizing addresses, etc.
        """
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as update_cursor:
                logger.info("Splitting out mailing address from address...")
                update_cursor.execute(UPDATE_FAC)
                conn.commit()

            # Then normalize all addresses
            with conn.cursor() as norm_cursor:
                logger.info("Normalizing addresses in facility_staging table...")
                norm_cursor.execute(NORMALIZE_ADDRESSES)
                conn.commit()

    def update_facility_table(self):
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as update_cursor:
                logger.info("Updating facility table...")
                update_cursor.execute("call update_facilities();")
                conn.commit()

    def update_station_table(self):
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as update_cursor:
                logger.info("Updating station table...")
                update_cursor.execute("select count(*) from station;")
                old_count = update_cursor.fetchone()[0]
                update_cursor.execute("call update_stations();")
                update_cursor.execute("select count(*) from station;")
                new_count = update_cursor.fetchone()[0]
                logger.info(f"Updated station table: {old_count} -> {new_count}")
                conn.commit()
