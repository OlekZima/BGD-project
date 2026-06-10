#!/usr/bin/env python3

import os, json, signal, sys
import psycopg
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

import time

last_flush = time.time()
FLUSH_INTERVAL = 5  # seconds

TOPIC = "citibike-status"
BATCH_SIZE = 4000

print("Connecting to Kafka...")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:29092",
    api_version=(3, 9, 0),
    fetch_max_bytes=10 * 1024 * 1024,
    max_partition_fetch_bytes=5 * 1024 * 1024,
    group_id="citibike-status",
    enable_auto_commit=False,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

print("Connected, starting loop...")

conn = psycopg.connect(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname=os.getenv("DB_NAME", "citibike"),
    user=os.getenv("DB_USER", "citibike"),
    password=os.getenv("POSTGRES_PASSWORD"),
)

conn.execute("SET synchronous_commit TO OFF;")

# CREATE TABLE
with conn.cursor() as cur:
    cur.execute("""
        CREATE UNLOGGED TABLE IF NOT EXISTS staging_station_status (
            station_id TEXT,
            num_bikes_available TEXT,
            num_ebikes_available TEXT,
            num_bikes_disabled TEXT,
            num_docks_available TEXT,
            num_docks_disabled TEXT,
            is_installed TEXT,
            is_renting TEXT,
            is_returning TEXT,
            station_status_last_reported TEXT
        );
    """)
    conn.commit()

def flush(batch):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE staging_station_status")

        data = "\n".join(
            "\t".join("" if v is None else str(v) for v in [
                r["station_id"],
                r["num_bikes_available"],
                r["num_ebikes_available"],
                r["num_bikes_disabled"],
                r["num_docks_available"],
                r["num_docks_disabled"],
                r["is_installed"],
                r["is_renting"],
                r["is_returning"],
                r["station_status_last_reported"],
            ])
            for r in batch
        ).encode()

        with cur.copy("""
            COPY staging_station_status (
                station_id,
                num_bikes_available,
                num_ebikes_available,
                num_bikes_disabled,
                num_docks_available,
                num_docks_disabled,
                is_installed,
                is_renting,
                is_returning,
                station_status_last_reported
            ) FROM STDIN
        """) as cp:
            cp.write(data)

        cur.execute("""
            INSERT INTO bronze_station_status (
                station_id,
                num_bikes_available,
                num_ebikes_available,
                num_bikes_disabled,
                num_docks_available,
                num_docks_disabled,
                is_installed,
                is_renting,
                is_returning,
                station_status_last_reported,
                source_mode
            )
            SELECT
                station_id,
                NULLIF(num_bikes_available, '')::int,
                NULLIF(num_ebikes_available, '')::int,
                NULLIF(num_bikes_disabled, '')::int,
                NULLIF(num_docks_available, '')::int,
                NULLIF(num_docks_disabled, '')::int,
                is_installed,
                is_renting,
                is_returning,
                NULLIF(station_status_last_reported, '')::bigint,
                'stream'
            FROM staging_station_status
            ON CONFLICT DO NOTHING
        """)

    conn.commit()

buffer = []

def shutdown(*args):
    if buffer:
        print("Flushing remaining STATUS data...")
        flush(buffer)
        consumer.commit()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

print("STATUS consumer started")

while True:
    records = consumer.poll(timeout_ms=1000)

    now = time.time()

    for tp, messages in records.items():
        for msg in messages:
            print(f"msg size = {len(msg.value)}")
            for row in msg.value:
                buffer.append(row)

    if len(buffer) >= BATCH_SIZE or (buffer and now - last_flush > FLUSH_INTERVAL):
        flush(buffer)
        buffer.clear()
        consumer.commit()
        last_flush = now
        print("batch processed")