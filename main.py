"""BGD Project — ETL Pipeline Entry Point.

Usage:
    uv run python main.py                     # Run full pipeline
    uv run python main.py --force-bronze      # Re-ingest from Kaggle
    uv run python main.py --force-silver      # Re-process Silver
    uv run python main.py --force-gold        # Re-build Gold views
    uv run python main.py --force-all         # Re-run everything
"""

import argparse
import sys

from etl.pipeline import run_pipeline


def main():
    parser = argparse.ArgumentParser(description="BGD Project ETL Pipeline")
    parser.add_argument("--force-bronze", action="store_true", help="Re-ingest Bronze layer")
    parser.add_argument("--force-silver", action="store_true", help="Re-process Silver layer")
    parser.add_argument("--force-gold", action="store_true", help="Re-build Gold views")
    parser.add_argument("--force-all", action="store_true", help="Re-run all layers")
    parser.add_argument("--kafka", action="store_true", help="Use Kafka bronze instead of CSV")
    args = parser.parse_args()

    force_all = args.force_all

    outputs = run_pipeline(
        force_bronze=force_all or args.force_bronze,
        force_silver=force_all or args.force_silver,
        force_gold=force_all or args.force_gold,
        use_kafka=args.kafka
    )

    print("\n📦 Pipeline outputs:")
    for name, path in outputs.items():
        print(f"   {name}: {path}")


if __name__ == "__main__":
    main()
