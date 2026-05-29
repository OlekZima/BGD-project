"""BGD-project ETL Pipeline - Citi Bike Stations Dataset.

Medallion Architecture:
  Bronze  → Raw ingestion from Kaggle CSVs into Parquet
  Silver  → Cleaned, typed, validated data with schema enforcement
  Gold    → Aggregated analytics-ready views (daily metrics, station dimension)
"""

__version__ = "0.1.0"
