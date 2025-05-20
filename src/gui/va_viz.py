from ..config.settings import DATABASE_URL


class VaViz:
    def __init__(self):
        self.database_url = DATABASE_URL
        if not self.database_url:
            raise ValueError("Database URL is required")


if __name__ == '__main__':
    VaViz()