import logging
import os
import sys
from logging import Logger

import dotenv


def parse_bool(env_value):
    if isinstance(env_value, bool):
        return env_value
    if isinstance(env_value, int):
        return env_value > 0
    return env_value is not None and len(env_value.strip()) > 0 and env_value.lower() not in ("0", "false")


def parse_list(env_value):
    if env_value:
        list_env = [x.strip().lower() for x in env_value.split(",") if x is not None and len(x.strip()) > 0]
        if list_env:
            return list_env
    return None


def parse_text(env_value):
    if env_value and env_value.strip():
        return env_value.strip()
    return None


# load environment variables from .env file
if "_test.py" in sys.argv[0] or "tests.py" in sys.argv[0] or "py.test" in sys.argv[0]:
    dotenv.load_dotenv("testing/test.env")
elif os.environ.get("ENV_FILE"):
    dotenv.load_dotenv(os.environ.get("ENV_FILE"))
else:
    dotenv_filename = dotenv.find_dotenv()
    if dotenv_filename:
        dotenv.load_dotenv(dotenv_filename)

# Operational mode and logging
DEBUG = parse_bool(os.environ.get("DEBUG")) or False
TESTING = parse_bool(os.environ.get("TESTING")) or False

# noinspection PyBroadException
try:
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
except Exception:
    print(f"Failed to set LOG_LEVEL to '{os.environ.get('LOG_LEVEL')}', setting to 'INFO' instead")
    LOG_LEVEL = logging.INFO

# set up logging
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger: Logger = logging.getLogger(__name__)


def get_bridge_logger(name: str = __name__) -> Logger:
    """
    Use in our code to get a configured logger instance
    """
    if name:
        name_parts = name.split(".")
        name = name_parts[-1] if len(name_parts) > 1 else name
    return logging.getLogger(name)


# SQL database URI (for authentication)
DATABASE_URL: str = os.environ.get("DATABASE_URL")
# allow alternate form
if DATABASE_URL and DATABASE_URL.startswith("postgres:"):
    DATABASE_URL = DATABASE_URL.replace("postgres:", "postgresql:")
logger.debug(f"Using database URL: {DATABASE_URL}")
