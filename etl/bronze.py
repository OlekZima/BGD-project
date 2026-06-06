"""Bronze Layer — Raw Data Ingestion.

Downloads the Kaggle dataset and ingests all CSV files into
a single partitioned Parquet dataset (raw/unchanged).
"""

import logging
from pathlib import Path

import kagglehub
import polars as pl

from etl.config import (
    BRONZE_DIR,
    KAGGLE_DATASET,
    DATA_SAMPLE_SIZE,
    NULL_VALUES,
    CSV_PATTERN,
    BRONZE_SCHEMA_OVERRIDES,
)

logger = logging.getLogger(__name__)


def download_dataset() -> Path:
    """Download the dataset from Kaggle and return the path to the CSV directory."""
    logger.info("Downloading dataset '%s' from KaggleHub...", KAGGLE_DATASET)
    path = Path(kagglehub.dataset_download(KAGGLE_DATASET))
    logger.info("Dataset downloaded to %s", path)
    return path


def ingest_bronze(csv_dir: Path | None = None, force: bool = False) -> Path:
    """Ingest raw CSVs into Bronze layer (partitioned Parquet).

    Args:
        csv_dir: Directory containing the CSV files. If None, downloads.
        force: If True, re-ingest even if bronze data already exists.

    Returns:
        Path to the bronze Parquet directory.
    """
    if BRONZE_DIR.exists() and any(BRONZE_DIR.iterdir()) and not force:
        logger.info("Bronze data already exists at %s (use force=True to re-ingest)", BRONZE_DIR)
        return BRONZE_DIR

    if csv_dir is None:
        csv_dir = download_dataset()

    csv_files = sorted(csv_dir.glob(CSV_PATTERN))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files matching '{CSV_PATTERN}' found in {csv_dir}")

    # Respect sample size from .env
    if DATA_SAMPLE_SIZE is not None and len(csv_files) > DATA_SAMPLE_SIZE:
        csv_files = csv_files[:DATA_SAMPLE_SIZE]
        logger.info("Sample mode: using first %d CSV file(s) (set ETL_DATA_SAMPLE_SIZE in .env to change)", DATA_SAMPLE_SIZE)

    logger.info("Found %d CSV files, ingesting into Bronze layer...", len(csv_files))

    # Lazy scan all CSVs — keep everything as string to preserve raw data.
    # sink_parquet streams output and avoids materializing the full dataset in memory.
    bronze_lf = pl.scan_csv(
        csv_files,
        schema_overrides=BRONZE_SCHEMA_OVERRIDES,
        null_values=NULL_VALUES,
        infer_schema=False,
        low_memory=True,
    )

    # Write as Parquet (raw copy)
    bronze_lf.sink_parquet(BRONZE_DIR / "data.parquet")
    logger.info("Bronze layer written to %s", BRONZE_DIR / "data.parquet")

    return BRONZE_DIR
