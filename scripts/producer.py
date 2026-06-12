#!/usr/bin/env python3
"""Streams the raw Citi Bike CSVs into Kafka.

Every row goes to the status topic; rows that carry station metadata go to
the info topic as well. Rows are sent in batches of BATCH_SIZE per message.
"""

import argparse
import csv
import time
from dataclasses import dataclass, field
from pathlib import Path

import orjson
from kafka import KafkaProducer

from settings import (
    KAFKA_API_VERSION,
    KAFKA_BOOTSTRAP_SERVERS,
    RAW_DIR,
    TOPIC_INFO,
    TOPIC_STATUS,
)

BATCH_SIZE = 2000


@dataclass
class ProduceStats:
    status_rows: int = 0
    info_rows: int = 0
    files: list[str] = field(default_factory=list)
    elapsed_seconds: float = 0.0


def _normalize(row: dict) -> dict:
    return {k: (None if v in ("", r"\N") else v) for k, v in row.items()}


def _create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=orjson.dumps,
        api_version=KAFKA_API_VERSION,
        compression_type="lz4",
        linger_ms=200,
        batch_size=10 * 1024 * 1024,
    )


def _send_batch(producer: KafkaProducer, batch: list[dict], stats: ProduceStats) -> None:
    info_batch = [r for r in batch if r.get("missing_station_information") == "false"]
    producer.send(TOPIC_STATUS, value=batch)
    if info_batch:
        producer.send(TOPIC_INFO, value=info_batch)
    stats.status_rows += len(batch)
    stats.info_rows += len(info_batch)


def produce(files: list[Path] | None = None, max_rows: int | None = None) -> ProduceStats:
    """Send the given CSV files (default: all of data/raw) to Kafka.

    Returns row counts and elapsed time, so callers can benchmark the run.
    """
    if files is None:
        files = sorted(RAW_DIR.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {RAW_DIR}")

    producer = _create_producer()
    stats = ProduceStats(files=[f.name for f in files])
    start = time.perf_counter()
    buffer: list[dict] = []
    rows_read = 0

    try:
        for file in files:
            print(f"Producing {file.name}", flush=True)
            with open(file, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    buffer.append(_normalize(row))
                    rows_read += 1
                    if len(buffer) >= BATCH_SIZE:
                        _send_batch(producer, buffer, stats)
                        buffer = []
                    if max_rows is not None and rows_read >= max_rows:
                        break
            if max_rows is not None and rows_read >= max_rows:
                break

        if buffer:
            _send_batch(producer, buffer, stats)
        producer.flush()
    finally:
        producer.close()

    stats.elapsed_seconds = time.perf_counter() - start
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--files", type=int, default=None,
                        help="limit to the first N CSV files")
    parser.add_argument("--max-rows", type=int, default=None,
                        help="stop after sending this many rows")
    args = parser.parse_args()

    files = sorted(RAW_DIR.glob("*.csv"))
    if args.files is not None:
        files = files[: args.files]

    stats = produce(files=files or None, max_rows=args.max_rows)
    print(f"Done in {stats.elapsed_seconds:.1f}s: "
          f"{stats.status_rows:,} status rows, {stats.info_rows:,} info rows")


if __name__ == "__main__":
    main()
