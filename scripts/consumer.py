#!/usr/bin/env python3

import os
import psycopg
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

TOPIC = "citibike-raw"
BATCH_SIZE = 10_000

COLUMNS = [
    "station_id",
    "num_bikes_available",
    "num_ebikes_available",
    "num_bikes_disabled",
    "num_docks_available",
    "num_docks_disabled",
    "is_installed",
    "is_renting",
    "is_returning",
    "station_status_last_reported",
    "station_name",
    "lat",
    "lon",
    "region_id",
    "capacity",
    "has_kiosk",
    "station_information_last_updated",
    "missing_station_information",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
]

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:29092",
    group_id="citibike-loader",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

conn = psycopg.connect(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname=os.getenv("DB_NAME", "citibike"),
    user=os.getenv("DB_USER", "citibike"),
    password=os.getenv("POSTGRES_PASSWORD"),
)

def copy_batch(cur, batch):
    cols = ",".join(COLUMNS)

    data = "\n".join(
        "\t".join(
            "" if r.get(c) is None else str(r.get(c))
            for c in COLUMNS
        )
        for r in batch
    ).encode()

    with cur.copy(f"COPY staging_station ({cols}) FROM STDIN WITH (FORMAT text)") as cp:
        cp.write(data)

def run_inserts(cur):

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
            source_mode,
            kafka_topic,
            kafka_partition,
            kafka_offset
        )
        SELECT
            station_id,
            NULLIF(num_bikes_available,'')::int,
            NULLIF(num_ebikes_available,'')::int,
            NULLIF(num_bikes_disabled,'')::int,
            NULLIF(num_docks_available,'')::int,
            NULLIF(num_docks_disabled,'')::int,
            is_installed,
            is_renting,
            is_returning,
            station_status_last_reported::bigint,
            'stream',
            kafka_topic,
            kafka_partition::int,
            kafka_offset::bigint
        FROM staging_station
        WHERE station_id IS NOT NULL
        ON CONFLICT DO NOTHING;
    """)

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
            missing_station_information,
            source_mode,
            kafka_topic,
            kafka_partition,
            kafka_offset
        )
        SELECT DISTINCT ON (station_id)
            station_id,
            station_name,
            NULLIF(lat,'')::numeric,
            NULLIF(lon,'')::numeric,
            region_id,
            NULLIF(capacity,'')::int,
            has_kiosk,
            NULLIF(station_information_last_updated,'')::bigint,
            missing_station_information,
            'stream',
            kafka_topic,
            kafka_partition::int,
            kafka_offset::bigint
        FROM staging_station
        WHERE station_id IS NOT NULL
          AND missing_station_information = 'false'
        ORDER BY station_id
        ON CONFLICT DO NOTHING;
    """)

def flush(batch):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE staging_station")
        copy_batch(cur, batch)
        run_inserts(cur)
    conn.commit()

# staging table
with conn.cursor() as cur:
    cur.execute("""
        CREATE UNLOGGED TABLE IF NOT EXISTS staging_station (
            station_id TEXT,
            num_bikes_available TEXT,
            num_ebikes_available TEXT,
            num_bikes_disabled TEXT,
            num_docks_available TEXT,
            num_docks_disabled TEXT,
            is_installed TEXT,
            is_renting TEXT,
            is_returning TEXT,
            station_status_last_reported TEXT,
            station_name TEXT,
            lat TEXT,
            lon TEXT,
            region_id TEXT,
            capacity TEXT,
            has_kiosk TEXT,
            station_information_last_updated TEXT,
            missing_station_information TEXT,
            kafka_topic TEXT,
            kafka_partition TEXT,
            kafka_offset TEXT
        );
    """)
    conn.commit()

print("Consumer started")

buffer = []
count = 0

for msg in consumer:

    row = msg.value
    row["kafka_topic"] = msg.topic
    row["kafka_partition"] = msg.partition
    row["kafka_offset"] = msg.offset

    buffer.append(row)
    count += 1

    if len(buffer) >= BATCH_SIZE:
        flush(buffer)
        buffer.clear()
        print(f"Processed {count:,} rows")

if buffer:
    flush(buffer)

print("DONE")