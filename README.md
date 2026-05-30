# BGD-project

Custom solution vs. Apache Kafka (maybe additional vs. Redpanda later)

Dataset: [Citi Bike Stations](https://www.kaggle.com/datasets/rosenthal/citi-bike-stations/data)

| Who | Task |
|-----|------|
| Arkadiusz | DB |
| Robert | Kafka research |
| Olek | Bronze → Silver → Gold |

## Run

**Prerequisites:** Docker Desktop, Python 3.10+

```bash
# 1. Start PostgreSQL (first run initialises the schema in a few seconds)
start.bat          # Windows
./start.sh         # Linux / Mac

# 2. Install loader dependency
pip install -r scripts/requirements.txt

# 3. Drop the Kaggle CSVs into data/raw/, then load Bronze
set POSTGRES_PASSWORD=<password>                               # Windows
export POSTGRES_PASSWORD=<password>                            # Linux / Mac
python scripts/load_bronze.py data/raw                         # all CSVs in the folder
python scripts/load_bronze.py data/raw/citi_bike_data_00000.csv  # or a single file

# 4. Connect
psql postgresql://citibike:<password>@localhost:5432/citibike
```

The loader uses server-side `COPY`: `data/raw` is mounted into the container at
`/data`, so the database reads the files directly. Loads are idempotent —
re-running skips rows already present.

Monitor first-time init: `docker logs -f bgd_postgres`
