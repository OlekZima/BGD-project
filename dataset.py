"""Dataset download utility — Citi Bike Stations.

Downloads the Kaggle dataset and returns the path to the CSV directory.
"""

import kagglehub
from pathlib import Path


def download_citi_bike_dataset() -> Path:
    """Download the Citi Bike Stations dataset from Kaggle.

    Returns:
        Path to the directory containing the CSV files.
    """
    path = Path(kagglehub.dataset_download("rosenthal/citi-bike-stations"))
    print(f"Dataset downloaded to: {path}")
    print(f"CSV files: {len(list(path.glob('*.csv')))}")
    return path


if __name__ == "__main__":
    download_citi_bike_dataset()
