#!/usr/bin/env python3
"""
Loads Citi Bike Stations CSVs into bronze_station_information and
bronze_station_status via server-side COPY through a session staging table.
Re-runs are idempotent: existing keys are skipped (ON CONFLICT DO NOTHING).

Usage:
    python scripts/load_bronze.py data/raw/<filename>.csv [more.csv ...]
    python scripts/load_bronze.py data/raw            # every *.csv in a folder

Required env var:
    POSTGRES_PASSWORD  — must match the value in .env

The CSVs are read by the database server from the path it sees them at
(./data/raw is mounted to /data in the container), so only their file names
need to match between host and container.
"""
import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

_HOST     = os.getenv("DB_HOST", "localhost")
_PORT     = int(os.getenv("DB_PORT", "5432"))
_DBNAME   = os.getenv("DB_NAME", "citibike")
_USER     = os.getenv("DB_USER", "citibike")
_PASSWORD = os.getenv("POSTGRES_PASSWORD")

_CONTAINER_DATA_DIR = os.getenv("DB_DATA_DIR", "/data")

# CSV column order, landed untyped before projection into the bronze tables.
_STAGE_COLS = (
    "station_id", "num_bikes_available", "num_ebikes_available", "num_bikes_disabled",
    "num_docks_available", "num_docks_disabled", "is_installed", "is_renting", "is_returning",
    "station_status_last_reported", "station_name", "lat", "lon", "region_id", "capacity",
    "has_kiosk", "station_information_last_updated", "missing_station_information",
)
_STAGE_COLUMNS = ", ".join(_STAGE_COLS)
_STAGE_DDL = (
    "CREATE TEMP TABLE staging_station ("
    + ", ".join(f"{c} TEXT" for c in _STAGE_COLS)
    + ")"
)

_INSERT_STATUS = """
INSERT INTO bronze_station_status
    (station_id, num_bikes_available, num_ebikes_available, num_bikes_disabled,
     num_docks_available, num_docks_disabled, is_installed, is_renting, is_returning,
     station_status_last_reported, source_file, source_mode)
SELECT station_id,
       num_bikes_available::int,  num_ebikes_available::int, num_bikes_disabled::int,
       num_docks_available::int,  num_docks_disabled::int,
       is_installed, is_renting, is_returning,
       station_status_last_reported::bigint, %s, 'batch'
FROM   staging_station
WHERE  station_id IS NOT NULL
  AND  station_status_last_reported IS NOT NULL
ON CONFLICT (station_id, station_status_last_reported) DO NOTHING
"""

_INSERT_INFO = """
INSERT INTO bronze_station_information
    (station_id, station_name, lat, lon, region_id, capacity, has_kiosk,
     station_information_last_updated, missing_station_information,
     source_file, source_mode)
SELECT DISTINCT ON (station_id)
       station_id, station_name, lat::numeric, lon::numeric, region_id,
       capacity::int, has_kiosk, station_information_last_updated::bigint,
       missing_station_information, %s, 'batch'
FROM   staging_station
WHERE  station_id IS NOT NULL
  AND  missing_station_information = 'false'
ORDER BY station_id
ON CONFLICT (station_id) DO NOTHING
"""


def _container_path(csv_path: Path) -> str:
    name = csv_path.name
    if "'" in name:
        sys.exit(f"Refusing unsafe file name containing a quote: {name}")
    return f"{_CONTAINER_DATA_DIR}/{name}"


def _load_file(conn, csv_path: Path) -> tuple[int, int]:
    copy_sql = (
        f"COPY staging_station ({_STAGE_COLUMNS}) "
        f"FROM '{_container_path(csv_path)}' "
        r"WITH (FORMAT csv, HEADER true, NULL '\N')"
    )
    with conn.cursor() as cur:
        cur.execute("TRUNCATE staging_station")
        cur.execute(copy_sql)
        cur.execute(_INSERT_STATUS, (csv_path.name,))
        status_rows = cur.rowcount
        cur.execute(_INSERT_INFO, (csv_path.name,))
        info_rows = cur.rowcount
    conn.commit()
    return info_rows, status_rows


def _refresh_watermark(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_watermark w SET
                rows_loaded  = c.n,
                last_load_ts = now()
            FROM (
                SELECT 'bronze_station_information' AS source_name, count(*) AS n
                FROM bronze_station_information
                UNION ALL
                SELECT 'bronze_station_status', count(*)
                FROM bronze_station_status
            ) c
            WHERE w.source_name = c.source_name
        """)
    conn.commit()


def _resolve(args: list[str]) -> list[Path]:
    paths: list[Path] = []
    for arg in args:
        p = Path(arg)
        if p.is_dir():
            paths.extend(sorted(p.glob("*.csv")))
        elif p.is_file():
            paths.append(p)
        else:
            sys.exit(f"No such file or directory: {p}")
    if not paths:
        sys.exit("No CSV files found to load.")
    return paths


def load(paths: list[Path]) -> None:
    if not _PASSWORD:
        sys.exit("POSTGRES_PASSWORD environment variable is not set.")

    info_total = 0
    stat_total = 0
    with psycopg.connect(host=_HOST, port=_PORT, dbname=_DBNAME,
                         user=_USER, password=_PASSWORD) as conn:
        with conn.cursor() as cur:
            cur.execute(_STAGE_DDL)
        for i, csv_path in enumerate(paths, start=1):
            info, stat = _load_file(conn, csv_path)
            info_total += info
            stat_total += stat
            print(f"[{i}/{len(paths)}] {csv_path.name}: "
                  f"+{info:,} info | +{stat:,} status", flush=True)
        _refresh_watermark(conn)

    print("\nDone.")
    print(f"  bronze_station_information : +{info_total:,} new rows")
    print(f"  bronze_station_status      : +{stat_total:,} new rows")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(f"Usage: python {sys.argv[0]} data/raw/<filename>.csv [more.csv ...]")
    load(_resolve(sys.argv[1:]))
