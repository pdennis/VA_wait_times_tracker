import psycopg

from ..config.settings import DATABASE_URL, get_bridge_logger
from ..util.congress_gov_api import CongressGovApi

logger = get_bridge_logger(__name__)


class DownloadCongress:
    def __init__(self) -> None:
        super().__init__()
        self.database_url = DATABASE_URL
        self.api = CongressGovApi()

    def download_congress_data(self):
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("select distinct state, geoid from facility where geoid is not null order by state, geoid;")
                for row in cur:
                    state, geoid = row
                    district = int(geoid[2:])
                    member = self.api.get_house_member(state, district)
                    if member:
                        try:
                            member.insert(conn)
                            conn.commit()
                            logger.info(f"{state} {district} Member {member.name} stored")
                        except psycopg.errors.UniqueViolation:
                            conn.rollback()
                            logger.info(f"Member {member.name} already stored, continuing...")
