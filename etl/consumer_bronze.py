#!/usr/bin/env python3

import json
import time

import polars as pl
from kafka import KafkaConsumer

from etl.config import BRONZE_SCHEMA_OVERRIDES, NULL_VALUES, BRONZE_DIR

# ---------------- CONFIG ----------------

TOPIC = "citibike-status"
BATCH_SIZE = 20000
FLUSH_INTERVAL = 10  # seconds

OUTPUT = BRONZE_DIR / "kafka_data.parquet"
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

print("Connecting to Kafka...")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:29092",
    api_version=(3, 9, 0),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    fetch_max_bytes=10 * 1024 * 1024,
    max_partition_fetch_bytes=5 * 1024 * 1024,
)

print("Kafka -> Bronze Parquet consumer started")

# ---------------- SCHEMA ----------------

SCHEMA_COLUMNS = list(BRONZE_SCHEMA_OVERRIDES.keys())

# ---------------- STATE ----------------

buffer = []
last_flush = time.time()

# ---------------- HELPERS ----------------

def normalize(row: dict) -> dict:
    return {
        col: (
            None if row.get(col) in NULL_VALUES or row.get(col) is None
            else str(row.get(col))
        )
        for col in SCHEMA_COLUMNS
    }


import os

def flush():
    global buffer

    if not buffer:
        return

    new_df = pl.DataFrame(
        buffer,
        schema=BRONZE_SCHEMA_OVERRIDES
    )

    if OUTPUT.exists():
        existing = pl.read_parquet(OUTPUT)
        full_df = pl.concat([existing, new_df])

        del existing

    else:
        full_df = new_df

    tmp_path = OUTPUT.with_suffix(".tmp.parquet")
    full_df.write_parquet(tmp_path)

    os.replace(tmp_path, OUTPUT)

    print(f"Wrote {len(buffer)} rows | total={full_df.height}")

    buffer = []


print("Waiting for data...")

# ---------------- MAIN LOOP ----------------

while True:
    records = consumer.poll(timeout_ms=1000)
    now = time.time()

    for tp, messages in records.items():
        for msg in messages:
            # print(f"msg size = {len(msg.value)}")

            for row in msg.value:
                buffer.append(normalize(row))

    if len(buffer) >= BATCH_SIZE or (buffer and now - last_flush > FLUSH_INTERVAL):
        flush()
        last_flush = now