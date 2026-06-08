#!/usr/bin/env python3

import csv
import time
from pathlib import Path
import orjson
from kafka import KafkaProducer

TOPIC = "citibike-raw"
RAW_DIR = Path("data/raw")

LOG_EVERY = 200_000

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=orjson.dumps,
    key_serializer=lambda k: k.encode() if k else None,
    acks=1,
    linger_ms=100,
    batch_size=5 * 1024 * 1024,
    buffer_memory=512 * 1024 * 1024,
)

def normalize(row):
    return {k: (None if v in ("", r"\N") else v) for k, v in row.items()}

def main():
    rows = 0
    start = time.time()

    for file in sorted(RAW_DIR.glob("*.csv")):
        print(f"Processing {file.name}")

        with open(file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                row = normalize(row)

                producer.send(
                    TOPIC,
                    key=row.get("station_id"),
                    value=row
                )

                rows += 1

                if rows % LOG_EVERY == 0:
                    rate = rows / (time.time() - start)
                    print(f"{rows:,} rows | {rate:,.0f} rows/sec")

    producer.flush()
    print("DONE")

if __name__ == "__main__":
    main()