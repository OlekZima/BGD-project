#!/usr/bin/env python3

import csv
from pathlib import Path
import orjson
from kafka import KafkaProducer

TOPIC_STATUS = "citibike-status"
TOPIC_INFO = "citibike-info"
RAW_DIR = Path("data/raw")

BATCH_SIZE = 2000

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=orjson.dumps,
    api_version=(3, 9, 0),
    compression_type="lz4",
    linger_ms=200,
    batch_size=10 * 1024 * 1024,
)

def normalize(row):
    return {k: (None if v in ("", r"\N") else v) for k, v in row.items()}

def send_batch(batch):
    status_batch = []
    info_batch = []

    for r in batch:
        status_batch.append(r)
        if r.get("missing_station_information") == "false":
            info_batch.append(r)

    if status_batch:
        producer.send(TOPIC_STATUS, value=status_batch)

    if info_batch:
        producer.send(TOPIC_INFO, value=info_batch)

    print(f"status_batch={len(status_batch)}, info_batch={len(info_batch)}")

def main():
    buffer = []

    for file in sorted(RAW_DIR.glob("*.csv")):
        print(f"Processing {file.name}")

        with open(file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                buffer.append(normalize(row))

                if len(buffer) >= BATCH_SIZE:
                    send_batch(buffer)
                    producer.flush()
                    buffer = []

    if buffer:
        send_batch(buffer)

    producer.flush()
    print("DONE")

if __name__ == "__main__":
    main()