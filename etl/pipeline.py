"""ETL Pipeline Runner — Orchestrates Bronze → Silver → Gold."""

import logging
import sys
import time
from pathlib import Path

import polars as pl

from etl.bronze import ingest_bronze
from etl.silver import process_silver
from etl.gold import process_gold
from etl.config import BRONZE_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")


def load_bronze_sample(bronze_path: Path, n: int = 5) -> pl.DataFrame:
    """Utility to peek at bronze data (for exploration/verification)."""
    return pl.read_parquet(bronze_path).head(n)


def load_silver_sample(status_path: Path, dim_path: Path) -> dict[str, pl.DataFrame]:
    """Utility to peek at silver data."""
    return {
        "status": pl.read_parquet(status_path).head(5),
        "dimension": pl.read_parquet(dim_path),
    }


def run_pipeline(
    force_bronze: bool = False,
    force_silver: bool = False,
    force_gold: bool = False,
    csv_dir: Path | None = None,
    use_kafka: bool = False
) -> dict[str, Path]:
    """Run the full ETL pipeline (Bronze → Silver → Gold).

    Args:
        force_bronze: Re-ingest bronze layer.
        force_silver: Re-process silver layer.
        force_gold: Re-build gold views.
        csv_dir: Path to CSV directory (downloads if not provided).

    Returns:
        Dictionary with paths for all output artifacts.
    """
    start = time.perf_counter()

    # Step 1: Bronze
    logger.info("=" * 60)
    logger.info("STEP 1/3: Bronze Layer — Raw Ingestion")
    logger.info("=" * 60)
    if use_kafka:
        logger.info("Using Kafka Bronze ingestion")

        bronze_file = BRONZE_DIR / "kafka_data.parquet"

        if not bronze_file.exists():
            raise RuntimeError(
                "Kafka Bronze not found.\n"
                "Run consumer first:\n"
                "uv run python -m etl.consumer_bronze"
            )

    else:
        bronze_path = ingest_bronze(csv_dir=csv_dir, force=force_bronze)
        bronze_file = bronze_path / "data.parquet"

    # Step 2: Silver
    logger.info("=" * 60)
    logger.info("STEP 2/3: Silver Layer — Clean & Transform")
    logger.info("=" * 60)
    status_fact_path, station_dim_path = process_silver(
        bronze_path=bronze_file,
        force=force_silver,
    )

    # Step 3: Gold
    logger.info("=" * 60)
    logger.info("STEP 3/3: Gold Layer — Aggregated Views")
    logger.info("=" * 60)
    gold_outputs = process_gold(force=force_gold)

    elapsed = time.perf_counter() - start
    logger.info("=" * 60)
    logger.info("Pipeline complete in %.2f seconds", elapsed)
    logger.info("=" * 60)

    return {
        "bronze": bronze_file,
        "silver_status": status_fact_path,
        "silver_dimension": station_dim_path,
        **gold_outputs,
    }


def main():
    """Entry point: run the full ETL pipeline."""
    outputs = run_pipeline()
    print("\n📦 Outputs:")
    for name, path in outputs.items():
        print(f"   {name}: {path}")


if __name__ == "__main__":
    main()
