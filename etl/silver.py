"""Silver Layer — Cleaned & Typed Data.

Transforms the raw Bronze data by:
- Casting all columns to proper types (timestamps, floats, ints, bools)
- Splitting into fact table (station_status_snapshots) and dimension table (stations)
- Handling `\\N` MySQL-style nulls properly
- Normalizing UUID-style station IDs alongside numeric IDs
- Validating data quality (null checks, range checks)
"""

import logging
from pathlib import Path

import polars as pl

from etl.config import SILVER_DIR, BRONZE_DIR

logger = logging.getLogger(__name__)

# Columns in the source data
STATUS_COLS = [
    "num_bikes_available",
    "num_ebikes_available",
    "num_bikes_disabled",
    "num_docks_available",
    "num_docks_disabled",
    "is_installed",
    "is_renting",
    "is_returning",
]


def parse_bronze(bronze_path: Path = BRONZE_DIR / "data.parquet") -> pl.LazyFrame:
    """Scan and parse the Bronze parquet into typed columns."""
    logger.info("Scanning Bronze data from %s", bronze_path)
    df = pl.scan_parquet(bronze_path)

    # --- Parse all columns from strings ---

    # station_id: keep as string (mix of numeric and UUID)
    df = df.with_columns(
        pl.col("station_id").str.strip_chars().alias("station_id")
    )

    # Numeric status columns
    for col in STATUS_COLS:
        df = df.with_columns(
            pl.col(col).cast(pl.Int64, strict=False).alias(col)
        )

    # Timestamp: epoch seconds -> datetime
    df = df.with_columns(
        pl.col("station_status_last_reported")
        .cast(pl.Int64, strict=False)
        .alias("ts_epoch"),
    ).with_columns(
        pl.when(pl.col("ts_epoch") > 1_000_000_000)
        .then(pl.from_epoch("ts_epoch", time_unit="s"))
        .otherwise(None)
        .alias("reported_at")
    ).drop("ts_epoch")

    # Station info columns
    df = df.with_columns(
        pl.col("station_name").str.strip_chars().alias("station_name"),
        pl.col("lat").cast(pl.Float64, strict=False).alias("lat"),
        pl.col("lon").cast(pl.Float64, strict=False).alias("lon"),
        pl.col("region_id").cast(pl.Int64, strict=False).alias("region_id"),
        pl.col("capacity").cast(pl.Int64, strict=False).alias("capacity"),
        pl.col("has_kiosk").alias("has_kiosk"),
        pl.col("station_information_last_updated")
        .cast(pl.Int64, strict=False)
        .alias("info_updated_epoch"),
    )

    df = df.with_columns(
        pl.when(pl.col("info_updated_epoch") > 1_000_000_000)
        .then(pl.from_epoch("info_updated_epoch", time_unit="s"))
        .otherwise(None)
        .alias("info_updated_at")
    ).drop("info_updated_epoch")

    # missing_station_information -> bool
    df = df.with_columns(
        pl.col("missing_station_information")
        .str.to_lowercase()
        .eq("true")
        .fill_null(False)
        .alias("missing_station_information"),
    )

    # Infer has_kiosk as bool
    df = df.with_columns(
        pl.col("has_kiosk")
        .str.to_lowercase()
        .eq("true")
        .fill_null(False)
        .alias("has_kiosk")
    )

    return df


def build_station_dimension(df: pl.LazyFrame) -> pl.LazyFrame:
    """Build the station dimension table from rows with station information.

    Extracts the most recent (non-null) station info per station_id.
    """
    stations = (
        df.filter(pl.col("missing_station_information") == False)
        .group_by("station_id")
        .agg([
            pl.col("station_name").drop_nulls().first(),
            pl.col("lat").drop_nulls().first(),
            pl.col("lon").drop_nulls().first(),
            pl.col("region_id").drop_nulls().first(),
            pl.col("capacity").drop_nulls().first(),
            pl.col("has_kiosk").drop_nulls().first(),
            pl.col("info_updated_at").drop_nulls().first(),
        ])
    )

    return stations


def build_status_fact(df: pl.LazyFrame) -> pl.LazyFrame:
    """Build the station status fact table (time-series snapshot data).

    Drops duplicate rows and reorders columns for analytics.
    """
    status = df.select([
        "station_id",
        "reported_at",
        "num_bikes_available",
        "num_ebikes_available",
        "num_bikes_disabled",
        "num_docks_available",
        "num_docks_disabled",
        "is_installed",
        "is_renting",
        "is_returning",
        "missing_station_information",
    ]).filter(
        pl.col("station_id").is_not_null()
        & pl.col("reported_at").is_not_null()
    )

    # Deduplicate — same station at same timestamp should only appear once
    return status.unique(subset=["station_id", "reported_at"], keep="first")


def process_silver(
    bronze_path: Path = BRONZE_DIR / "data.parquet",
    force: bool = False,
) -> tuple[Path, Path]:
    """Run the Bronze → Silver transformation.

    Args:
        bronze_path: Path to the Bronze parquet file.
        force: If True, re-process even if Silver data exists.

    Returns:
        (status_fact_path, station_dim_path)
    """
    status_fact_path = SILVER_DIR / "station_status.parquet"
    station_dim_path = SILVER_DIR / "station_dimension.parquet"

    if status_fact_path.exists() and station_dim_path.exists() and not force:
        logger.info("Silver data already exists (use force=True to re-process)")
        return status_fact_path, station_dim_path

    df = parse_bronze(bronze_path)

    status = build_status_fact(df)
    stations = build_station_dimension(df)

    status.sink_parquet(status_fact_path)
    stations.sink_parquet(station_dim_path)

    logger.info(
        "Silver layer written:\n"
        "  Status fact:  %s\n"
        "  Station dim:  %s",
        status_fact_path,
        station_dim_path,
    )

    return status_fact_path, station_dim_path
