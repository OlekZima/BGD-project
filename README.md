# BGD-project

**Topic: Real-time ingestion — with and without Apache Kafka — performance comparison.**

We ingest the same dataset into the same PostgreSQL bronze tables through two
paths and measure end-to-end throughput:

1. **Direct (without Kafka):** CSV → server-side `COPY` → bronze tables.
2. **Streaming (with Kafka):** CSV → producer → Kafka topics → consumers → `COPY` → bronze tables.

Dataset: [Citi Bike Stations](https://www.kaggle.com/datasets/rosenthal/citi-bike-stations/data)

## Team Contributions

| Who | Contribution |
|-----|--------------|
| Arkadiusz | PostgreSQL medallion schemas, Docker setup, direct `COPY` Bronze loader, benchmark harness |
| Robert | Kafka research, producer and consumers (PostgreSQL + Parquet pipelines), topics setup |
| Olek | Bronze → Silver → Gold Polars ETL pipeline, dataset tooling |

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

## Benchmark — With vs. Without Kafka

`scripts/benchmark.py` measures end-to-end ingestion into the bronze tables
for both paths and records the results. **It truncates the bronze tables
before every measured run**, so each run starts from zero rows.

```bash
docker compose up -d                      # PostgreSQL + Kafka must be running
uv run python scripts/create_topics.py

# Fair quick comparison: both modes ingest the same row-capped sample
uv run python scripts/benchmark.py direct --max-rows 1000000
uv run python scripts/benchmark.py kafka  --max-rows 1000000

# Or whole files
uv run python scripts/benchmark.py direct --files 2
uv run python scripts/benchmark.py kafka  --files 2

# Render benchmarks/RESULTS.md from all recorded runs
uv run python scripts/benchmark.py report
```

**Methodology**

- Both modes land identical rows in the same tables with the same
  deduplication (`ON CONFLICT DO NOTHING`), so timings are comparable.
- `direct` times `load_bronze.py` (server-side `COPY` from the mounted CSV).
- `kafka` spawns the two consumers, waits until their consumer groups are
  stable and any backlog is drained, truncates bronze, then starts the
  producer. The clock runs from the first produced row until the bronze row
  counts stop changing — i.e. it includes serialization, the broker hop, and
  consumer-side batching.
- Results are appended to `benchmarks/results.jsonl`; consumer output goes to
  `benchmarks/*.log`.

**Known asymmetries** (kept intentionally, documented for honesty): the
streaming consumers run with `synchronous_commit = OFF` and unlogged staging
tables; the direct path uses default commit settings and reads files the
database can access directly.

If port `29092` is taken on your machine, set `KAFKA_EXTERNAL_PORT` and
`KAFKA_BOOTSTRAP_SERVERS` in `.env` (see `.env.example`).

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
into the bronze tables (the password is read from `.env`):

```bash
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
│   ├── load_bronze.py      # COPY-based DB loader (direct path)
│   ├── producer.py         # CSV → Kafka producer
│   ├── consumer_status.py  # Kafka → bronze_station_status
│   ├── consumer_info.py    # Kafka → bronze_station_information
│   ├── benchmark.py        # direct vs. kafka ingestion benchmark
│   └── settings.py         # shared env-driven connection settings
├── benchmarks/             # results.jsonl + RESULTS.md (generated)
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