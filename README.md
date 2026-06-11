# BGD-project

Custom solution vs. Apache Kafka (maybe additional vs. Redpanda later)

Dataset: [Citi Bike Stations](https://www.kaggle.com/datasets/rosenthal/citi-bike-stations/data)

| Who | Task |
|-----|------|
| Arkadiusz | DB (PostgreSQL schemas, Bronze loader) |
| Robert | Kafka research |
| Olek | Bronze → Silver → Gold (Polars ETL) |

---

## Quick Start

```bash
# 1. Install dependencies
uv sync

# 2. Download the dataset
uv run python dataset.py            # puts CSVs in data/raw/

# 3a. Start PostgreSQL (if loading into DB)
docker compose up -d

# 3b. Or run the Polars ETL pipeline directly
uv run python main.py               # uses data/raw/ by default
```

---

## Kafka (Streaming Ingestion)

Kafka is used as an alternative ingestion layer for both PostgreSQL and the Polars ETL pipeline.

**Start Kafka**
```bash
docker compose up -d kafka kafka-ui
uv run python scripts/create_topics.py
```

**Kafka → PostgreSQL pipeline**

If CSV files are available in data/raw/, stream them into the database:
```bash
# Producer (CSV → Kafka)
uv run python scripts/producer.py

# Consumers (Kafka → PostgreSQL)
uv run python scripts/consumer_status.py
uv run python scripts/consumer_info.py
```
This loads data into bronze tables in PostgreSQL using streaming ingestion.

**Kafka → ETL (Polars pipeline)**

Instead of loading CSVs directly, you can ingest data into Bronze Parquet via Kafka.
```bash
# Producer (CSV → Kafka)
uv run python scripts/producer.py

# Consumer (Kafka → Parquet Bronze)
uv run python -m etl.consumer_bronze
```
After Kafka ingestion completes:
```bash
uv run python main.py --kafka
```
This runs the ETL pipeline using Kafka-originated Bronze data instead of CSVs.

---

## ETL Pipeline (Polars)

The Polars pipeline reads CSVs from `data/raw/`, processes them through
Bronze → Silver → Gold, and writes Parquet files to `data/`.

**Run:**
```bash
uv run python main.py
```

**Sample size:** edit `.env` → `ETL_DATA_SAMPLE_SIZE=3` (default).
Set to `0` or `all` for the full dataset.

See [`agent.md`](agent.md) for full ETL architecture.

---

## PostgreSQL (Docker)

Start the database:
```bash
docker compose up -d                  # starts PostgreSQL on port 5432
```

Studio / CLI:
```bash
psql postgresql://citibike:<password>@localhost:5432/citibike
```

The init script (`docker/init/01_setup.sh`) runs the medallion SQL
schemas from `sql/` automatically on first start.

---

## Bronze Loader (DB)

After downloading the dataset and starting PostgreSQL, load the CSVs
into the bronze tables:

```bash
export POSTGRES_PASSWORD=ChangeMe123       # set your .env password
uv run python scripts/load_bronze.py data/raw/
```

The loader uses server-side `COPY` — `data/raw/` is mounted into the
container at `/data`, so the DB reads the files directly.

Loads are **idempotent**: re-running skips rows already present.

---

## Project Layout

```
BGD-project/
├── .env                    # local config (gitignored)
├── .env.example            # template for teammates
├── docker-compose.yml      # PostgreSQL container
├── docker/init/01_setup.sh # DDL init script
├── sql/
│   ├── 00_infrastructure/  # watermark, pipeline_log, dlq
│   ├── 01_bronze/          # raw station tables
│   ├── 02_silver/          # cleaned views
│   └── 03_gold/            # analytics aggregates
├── scripts/
│   └── load_bronze.py      # COPY-based DB loader
├── dataset.py              # Kaggle download → data/raw/
├── main.py                 # ETL pipeline entry point
├── etl/                    # Polars medallion pipeline
│   ├── bronze.py, silver.py, gold.py, ...
├── pyproject.toml          # dep: kagglehub, polars, python-dotenv, psycopg
└── data/
    ├── raw/                # CSV files (symlinked from Kaggle cache)
    ├── bronze/             # Parquet (Polars pipeline)
    ├── silver/
    └── gold/
```