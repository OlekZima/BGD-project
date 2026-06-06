"""Gold Layer — Analytics-Ready Views.

Builds aggregated views on top of Silver data:
  - Daily station status summary
  - Station dimension with latest metadata
  - Time-of-day patterns aggregated across stations
"""

import logging
from pathlib import Path

import polars as pl

from etl.config import GOLD_DIR, SILVER_DIR

logger = logging.getLogger(__name__)


def build_daily_station_summary(
    status_path: Path = SILVER_DIR / "station_status.parquet",
    dim_path: Path = SILVER_DIR / "station_dimension.parquet",
    force: bool = False,
) -> Path:
    """Build a daily aggregated view: per-station, per-day metrics.

    Output format: Parquet partitioned by date (yyyymmdd).
    """
    output = GOLD_DIR / "daily_station_summary.parquet"

    if output.exists() and not force:
        logger.info("Gold daily summary already exists (use force=True)")
        return output

    status = pl.scan_parquet(status_path)
    dim = pl.scan_parquet(dim_path)

    daily = (
        status
        .with_columns(
            pl.col("reported_at").dt.date().alias("date"),
        )
        .group_by(["station_id", "date"])
        .agg([
            pl.col("num_bikes_available").mean().round(2).alias("avg_bikes_available"),
            pl.col("num_bikes_available").min().alias("min_bikes_available"),
            pl.col("num_bikes_available").max().alias("max_bikes_available"),
            pl.col("num_ebikes_available").mean().round(2).alias("avg_ebikes_available"),
            pl.col("num_docks_available").mean().round(2).alias("avg_docks_available"),
            pl.col("num_docks_available").min().alias("min_docks_available"),
            pl.col("num_docks_available").max().alias("max_docks_available"),
            pl.col("reported_at").count().alias("num_snapshots"),
        ])
    )

    # Join station dimension info
    daily = daily.join(dim, on="station_id", how="left")

    daily.sink_parquet(output)
    logger.info("Gold daily summary written: %s", output)
    return output


def build_hourly_patterns(
    status_path: Path = SILVER_DIR / "station_status.parquet",
    force: bool = False,
) -> Path:
    """Build time-of-day utilization patterns across all stations.

    Aggregates by hour-of-day to show typical daily usage patterns.
    """
    output = GOLD_DIR / "hourly_patterns.parquet"

    if output.exists() and not force:
        logger.info("Gold hourly patterns already exists (use force=True)")
        return output

    status = pl.scan_parquet(status_path)

    hourly = (
        status
        .with_columns([
            pl.col("reported_at").dt.hour().alias("hour"),
            pl.col("reported_at").dt.date().alias("date"),
        ])
        .group_by(["station_id", "date", "hour"])
        .agg([
            pl.col("num_bikes_available").mean().alias("avg_bikes"),
            pl.col("num_docks_available").mean().alias("avg_docks"),
            pl.col("num_ebikes_available").mean().alias("avg_ebikes"),
            pl.col("reported_at").count().alias("num_readings"),
        ])
    )

    hourly.sink_parquet(output)
    logger.info("Gold hourly patterns written: %s", output)
    return output


def build_station_summary(
    status_path: Path = SILVER_DIR / "station_status.parquet",
    dim_path: Path = SILVER_DIR / "station_dimension.parquet",
    force: bool = False,
) -> Path:
    """Build overall station summary with lifetime statistics."""
    output = GOLD_DIR / "station_summary.parquet"

    if output.exists() and not force:
        logger.info("Gold station summary already exists (use force=True)")
        return output

    status = pl.scan_parquet(status_path)
    dim = pl.scan_parquet(dim_path)

    summary = (
        status
        .group_by("station_id")
        .agg([
            pl.col("reported_at").min().alias("first_seen"),
            pl.col("reported_at").max().alias("last_seen"),
            pl.col("reported_at").count().alias("total_snapshots"),
            pl.col("num_bikes_available").mean().round(2).alias("avg_bikes_available"),
            pl.col("num_bikes_available").max().alias("peak_bikes_available"),
            pl.col("num_docks_available").mean().round(2).alias("avg_docks_available"),
            pl.col("num_docks_available").min().alias("min_docks_available"),
            pl.col("missing_station_information").mean().alias("pct_missing_info"),
        ])
    )

    summary = summary.join(dim, on="station_id", how="left")
    summary.sink_parquet(output)

    logger.info("Gold station summary written: %s", output)
    return output


def process_gold(
    force: bool = False,
) -> dict[str, Path]:
    """Run the Silver → Gold transformation for all views.

    Args:
        force: If True, re-process even if Gold data exists.

    Returns:
        Dictionary mapping view name to Parquet path.
    """
    outputs = {
        "daily_station_summary": build_daily_station_summary(force=force),
        "hourly_patterns": build_hourly_patterns(force=force),
        "station_summary": build_station_summary(force=force),
    }

    logger.info("Gold layer complete: %d views", len(outputs))
    return outputs
