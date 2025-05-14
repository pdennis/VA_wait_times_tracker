from pathlib import Path

import psycopg

from ..config.settings import DATABASE_URL, get_bridge_logger

logger = get_bridge_logger(__name__)


class UpdateFacilities:
    def __init__(self, facilities_csv: str) -> None:
        self.database_url = DATABASE_URL
        if not self.database_url:
            raise ValueError("Database URL is required")

        # make sure we have a good facilities csv file
        if not facilities_csv:
            raise ValueError("Facilities CSV file-path is required")
        facilities_csv = Path(facilities_csv)
        if not facilities_csv.exists() and facilities_csv.suffix != ".csv":
            facilities_csv = facilities_csv.with_suffix(".csv")
        if not facilities_csv.exists():
            raise FileNotFoundError(f"Facilities CSV file not found: {facilities_csv}")
        self.facilities_csv = Path(facilities_csv)

        # load new data into staging table
        self.load_staging_table()

    def load_staging_table(self):
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                # truncate the table to avoid duplicates
                logger.info("Truncating the facilities_staging table...")
                cur.execute("truncate table facilities_staging;")

                # load the data from the csv file
                logger.info(f"Loading facilities_staging table from {self.facilities_csv}...")
                with open(self.facilities_csv, "r") as f:
                    with cur.copy("COPY facilities_staging FROM STDIN WITH (FORMAT CSV, HEADER) ") as copy:
                        while data := f.read(1024):
                            copy.write(data)
            conn.commit()
