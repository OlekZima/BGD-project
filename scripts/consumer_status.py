#!/usr/bin/env python3
"""Kafka → PostgreSQL consumer for station status events."""

from consumer_common import CopyConsumer
from settings import TOPIC_STATUS

COLUMNS = (
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
)

STAGING_DDL = """
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
)
"""

INSERT_SQL = """
INSERT INTO bronze_station_status (
    station_id, num_bikes_available, num_ebikes_available, num_bikes_disabled,
    num_docks_available, num_docks_disabled, is_installed, is_renting,
    is_returning, station_status_last_reported, source_mode
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
WHERE station_id IS NOT NULL
  AND NULLIF(station_status_last_reported, '') IS NOT NULL
ON CONFLICT DO NOTHING
"""


def main() -> None:
    CopyConsumer(
        topic=TOPIC_STATUS,
        group_id="citibike-status",
        staging_table="staging_station_status",
        staging_ddl=STAGING_DDL,
        columns=COLUMNS,
        insert_sql=INSERT_SQL,
        batch_size=4000,
    ).run()


if __name__ == "__main__":
    main()
