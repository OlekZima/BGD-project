#!/bin/bash
# Runs once on first cluster init via /docker-entrypoint-initdb.d.
# Applies the medallion DDL in layer order, then seeds the watermark.
set -e

echo "================================================"
echo " BGD-project: Citi Bike DDL setup"
echo "================================================"

run() {
    psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" -f "${1}"
}

echo "[1/2] Creating tables..."
for dir in 00_infrastructure 01_bronze 02_silver 03_gold; do
    for sql_file in /sql/"${dir}"/*.sql; do
        [ -f "${sql_file}" ] || continue
        echo "  -> ${sql_file##*/}"
        run "${sql_file}"
    done
done

echo "[2/2] Seeding pipeline_watermark..."
psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<'SQL'
INSERT INTO pipeline_watermark (source_name, rows_loaded, last_load_ts)
VALUES ('bronze_station_information', 0, now()),
       ('bronze_station_status',      0, now())
ON CONFLICT (source_name) DO NOTHING;
SQL

echo ""
echo "Schema ready.  Connect with:"
echo "  psql postgresql://citibike:<password>@localhost:5432/citibike"
echo "================================================"
