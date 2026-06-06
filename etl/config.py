"""ETL configuration paths and constants."""

import os
from pathlib import Path

import polars as pl

from dotenv import load_dotenv

# Load .env file from project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# Layer directories (under project root /data/)
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Dataset info
KAGGLE_DATASET = "rosenthal/citi-bike-stations"
NUM_EXPECTED_FILES = 50
CSV_PATTERN = "citi_bike_data_*.csv"

# Data sample size — how many CSV files to process.
# Read from .env; "0" or "all" means process everything.
_raw = os.getenv("ETL_DATA_SAMPLE_SIZE", "3")
if _raw.strip().lower() in ("0", "all", ""):
    DATA_SAMPLE_SIZE: int | None = None  # None = all files
else:
    DATA_SAMPLE_SIZE = max(1, int(_raw))

# Dataset download path (set after download)
DATASET_DIR: Path | None = None  # populated dynamically at runtime

# CSV parsing
NULL_VALUES = ["\\N", "", "null"]

# Schema overrides for raw ingestion (Bronze) — keep everything as string/utf8
BRONZE_SCHEMA_OVERRIDES = {
    "station_id": pl.Utf8,
    "num_bikes_available": pl.Utf8,
    "num_ebikes_available": pl.Utf8,
    "num_bikes_disabled": pl.Utf8,
    "num_docks_available": pl.Utf8,
    "num_docks_disabled": pl.Utf8,
    "is_installed": pl.Utf8,
    "is_renting": pl.Utf8,
    "is_returning": pl.Utf8,
    "station_status_last_reported": pl.Utf8,
    "station_name": pl.Utf8,
    "lat": pl.Utf8,
    "lon": pl.Utf8,
    "region_id": pl.Utf8,
    "capacity": pl.Utf8,
    "has_kiosk": pl.Utf8,
    "station_information_last_updated": pl.Utf8,
    "missing_station_information": pl.Utf8,
}

# Gold: default aggregation bins
DEFAULT_BIN_HOURS = 6  # 4 bins per day for daily patterns

# Ensure data directories exist
for d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR, DATA_DIR / "raw"]:
    d.mkdir(parents=True, exist_ok=True)
