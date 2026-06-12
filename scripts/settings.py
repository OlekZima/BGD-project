"""Shared, env-driven connection settings for the ingestion scripts.

All values can be overridden in `.env` (or the process environment); the
defaults match the docker-compose services.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

RAW_DIR = PROJECT_ROOT / "data" / "raw"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_API_VERSION = (3, 9, 0)
KAFKA_CONTAINER = os.getenv("KAFKA_CONTAINER", "bgd_kafka")

TOPIC_STATUS = "citibike-status"
TOPIC_INFO = "citibike-info"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "citibike")
DB_USER = os.getenv("DB_USER", "citibike")


def db_connection_kwargs() -> dict:
    """Connection parameters for psycopg; fails fast when the password is missing."""
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("POSTGRES_PASSWORD environment variable is not set.")
    return {
        "host": DB_HOST,
        "port": DB_PORT,
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": password,
    }
