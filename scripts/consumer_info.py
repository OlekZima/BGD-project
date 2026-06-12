#!/usr/bin/env python3
"""Kafka → PostgreSQL consumer for station information (metadata) events."""

from consumer_common import CopyConsumer
from settings import TOPIC_INFO

COLUMNS = (
    "station_id",
    "station_name",
    "lat",
    "lon",
    "region_id",
    "capacity",
    "has_kiosk",
    "station_information_last_updated",
    "missing_station_information",
)

STAGING_DDL = """
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
)
"""

INSERT_SQL = """
INSERT INTO bronze_station_information (
    station_id, station_name, lat, lon, region_id, capacity, has_kiosk,
    station_information_last_updated, missing_station_information, source_mode
)
SELECT DISTINCT ON (station_id)
    station_id,
    station_name,
    NULLIF(lat, '')::numeric,
    NULLIF(lon, '')::numeric,
    region_id,
    NULLIF(capacity, '')::int,
    has_kiosk,
    NULLIF(station_information_last_updated, '')::bigint,
    missing_station_information,
    'stream'
FROM staging_station_info
WHERE station_id IS NOT NULL
  AND missing_station_information = 'false'
ORDER BY station_id
ON CONFLICT DO NOTHING
"""


def main() -> None:
    CopyConsumer(
        topic=TOPIC_INFO,
        group_id="citibike-info",
        staging_table="staging_station_info",
        staging_ddl=STAGING_DDL,
        columns=COLUMNS,
        insert_sql=INSERT_SQL,
        batch_size=2000,
    ).run()


if __name__ == "__main__":
    main()
