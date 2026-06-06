# BGD Project Agent Documentation

## Repository Root

`/home/ozima/workspace/bgd/BGD-project`

## Project Overview

**BGD-project** builds a medallion-architecture ETL pipeline (Bronze → Silver → Gold) over the Citi Bike Stations Kaggle dataset (~3.85GB, 50 CSV files). The pipeline is designed to support downstream comparison of streaming solutions (Kafka / Redpanda / custom).

### Team Members

- **Arkadiusz**: Database — PostgreSQL schemas (Bronze→Silver→Gold in SQL), Docker Compose, Bronze COPY loader
- **Robert**: Kafka research
- **Olek**: Polars ETL pipeline (Bronze→Silver→Gold)

### Tech Stack

- **Python 3.13** with [Polars](https://pola.rs) for the ETL pipeline
- **PostgreSQL 16** (Docker) with medallion SQL schemas
- **KaggleHub** for dataset download
- **Parquet** as the structured data format at all ETL layers
- **uv** for package management (see `uv.lock`)
- **python-dotenv** for `.env` configuration

---

## Dataset — Citi Bike Stations

**Source**: [Kaggle: Citi Bike Stations](https://www.kaggle.com/datasets/rosenthal/citi-bike-stations)
**Download command**: `uv run python dataset.py` → puts CSVs in `data/raw/`

### Raw Data

- 50 CSV files (`citi_bike_data_00000.csv` … `citi_bike_data_00049.csv`)
- ~3.85GB total
- ~312 million rows (estimated)
- Uses MySQL-style `\N` for nulls

### Schema (18 columns)

| Column | Type in CSV | Description |
|---|---|---|
| `station_id` | String | Numeric (int) or UUID-like (e.g., `motivate_BKN_...`) |
| `num_bikes_available` | Integer | Classic bikes at station |
| `num_ebikes_available` | Integer | E-bikes at station |
| `num_bikes_disabled` | Integer | Disabled/docked bikes |
| `num_docks_available` | Integer | Free docks at station |
| `num_docks_disabled` | Integer | Disabled docks |
| `is_installed` | Integer (0/1) | Station physically installed |
| `is_renting` | Integer (0/1) | Station currently renting |
| `is_returning` | Integer (0/1) | Station currently accepting returns |
| `station_status_last_reported` | Integer (epoch) | Last status update timestamp |
| `station_name` | String | Human-readable station name |
| `lat` | Float | Latitude |
| `lon` | Float | Longitude |
| `region_id` | Integer | Region identifier |
| `capacity` | Integer | Total dock count |
| `has_kiosk` | Boolean | Whether station has a kiosk |
| `station_information_last_updated` | Integer (epoch) | Station info last update |
| `missing_station_information` | Boolean | Flag: no station metadata available |

### Key Characteristics

- **73 unique stations** — 50 with numeric IDs, 23 with UUID-style IDs
- ~81% of rows have `missing_station_information = true`
- Time range: up to late 2021
- Station status is polled frequently — many snapshots per station per day

---

## Data Flow

```
Kaggle (CSV)
    │
    ▼
data/raw/*.csv                      # Kaggle download (uv run python dataset.py)
    │
    ├──► Polars Pipeline (uv run python main.py)
    │       Bronze → Silver → Gold
    │       data/bronze/*.parquet
    │       data/silver/*.parquet
    │       data/gold/*.parquet
    │
    └──► PostgreSQL via Docker
            docker compose up -d
            uv run python scripts/load_bronze.py data/raw/
            Bronze tables → Silver views → Gold materialized views
```

---

## ETL Architecture: Two Medallion Implementations

### A) Polars Pipeline (`etl/`)

Designed for local development, sampling, and quick iteration.

| Layer | Code | Output |
|---|---|---|
| **Bronze** | `etl/bronze.py` — lazy CSV scan → `data/bronze/data.parquet` | All-string Parquet (faithful copy) |
| **Silver** | `etl/silver.py` — typed cast + split into fact/dim → `data/silver/` | `station_status.parquet` (fact), `station_dimension.parquet` (dim) |
| **Gold** | `etl/gold.py` — aggregations → `data/gold/` | `daily_station_summary`, `hourly_patterns`, `station_summary` |

Run: `uv run python main.py`

### B) PostgreSQL SQL Schemas (`sql/`)

Designed for batch loading and eventual Kafka streaming integration.

| Layer | SQL | Tables/Views |
|---|---|---|
| **Bronze** | `sql/01_bronze/` | `bronze_station_information`, `bronze_station_status` (+ Kafka columns) |
| **Silver** | `sql/02_silver/` | Cleaned/typed views |
| **Gold** | `sql/03_gold/` | Analytics aggregates + data quality |

Start: `docker compose up -d`
Load: `uv run python scripts/load_bronze.py data/raw/`

Both paths share the same source (`data/raw/*.csv`).

---

## Configuration (.env)

```bash
# Number of CSV files for the Polars ETL (default: 3). 0 or "all" = full dataset.
ETL_DATA_SAMPLE_SIZE=3

# PostgreSQL password (used by Docker Compose + Bronze loader).
POSTGRES_PASSWORD=ChangeMe123
```

---

## Usage

### Prerequisites

```bash
uv sync
```

### Download Dataset

```bash
uv run python dataset.py       # symlinks CSVs into data/raw/
```

### Run Polars ETL

```bash
uv run python main.py
uv run python main.py --force-all   # re-run all layers
```

### Start PostgreSQL + Load Bronze

```bash
docker compose up -d
export POSTGRES_PASSWORD=ChangeMe123
uv run python scripts/load_bronze.py data/raw/
```

### Connect to Database

```bash
psql postgresql://citibike:<password>@localhost:5432/citibike
```

---

## Notes

- **Data sampling**: `ETL_DATA_SAMPLE_SIZE` in `.env` limits CSV files for the Polars pipeline. Start with 1–3.
- **Two parallel medallions**: Polars (Parquet) for dev speed, PostgreSQL (SQL) for production/streaming. They share `data/raw/`.
- The Bronze SQL tables have Kafka metadata columns (`event_id`, `kafka_topic`, `kafka_partition`, `kafka_offset`) ready for streaming ingestion.
- The `station_id` column mixes numeric IDs and UUID-style string IDs. Both are preserved as strings in both pipelines.