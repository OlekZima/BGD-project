#!/usr/bin/env python3

import os, json, signal, sys
import psycopg
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

import time

last_flush = time.time()
FLUSH_INTERVAL = 5  # seconds

TOPIC = "citibike-info"
BATCH_SIZE = 2000

print("Connecting to Kafka...")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:29092",
    api_version=(3, 9, 0),
    fetch_max_bytes=10 * 1024 * 1024,
    max_partition_fetch_bytes=5 * 1024 * 1024,
    group_id="citibike-info",
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
        CREATE UNLOGGED TABLE IF NOT EXISTS staging_station_info (
            station_id TEXT,
            station_name TEXT,
            lat TEXT,
            lon TEXT,
            region_id TEXT,
            capacity TEXT,
            has_kiosk TEXT,
            station_information_last_updated TEXT,
            missing_station_information TEXT
        );
    """)
    conn.commit()

def flush(batch):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE staging_station_info")

        data = "\n".join(
            "\t".join("" if v is None else str(v) for v in [
                r["station_id"],
                r["station_name"],
                r["lat"],
                r["lon"],
                r["region_id"],
                r["capacity"],
                r["has_kiosk"],
                r["station_information_last_updated"],
                r["missing_station_information"],
            ])
            for r in batch
        ).encode()

        with cur.copy("""
            COPY staging_station_info (
                station_id,
                station_name,
                lat,
                lon,
                region_id,
                capacity,
                has_kiosk,
                station_information_last_updated,
                missing_station_information
            ) FROM STDIN
        """) as cp:
            cp.write(data)

        cur.execute("""
            INSERT INTO bronze_station_information (
                station_id,
                station_name,
                lat,
                lon,
                region_id,
                capacity,
                has_kiosk,
                station_information_last_updated,
                missing_station_information
            )
            SELECT
                station_id,
                station_name,
                NULLIF(lat, '')::numeric,
                NULLIF(lon, '')::numeric,
                region_id,
                NULLIF(capacity, '')::int,
                has_kiosk,
                NULLIF(station_information_last_updated, '')::bigint,
                missing_station_information
            FROM staging_station_info
            WHERE missing_station_information = 'false'
            ON CONFLICT DO NOTHING
        """)

    conn.commit()

buffer = []

def shutdown(*args):
    if buffer:
        print("Flushing remaining INFO data...")
        flush(buffer)
        consumer.commit()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

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