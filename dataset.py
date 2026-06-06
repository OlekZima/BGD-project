"""Dataset download utility — Citi Bike Stations.

Downloads the Kaggle dataset and moves CSVs into data/raw/
so they're accessible to both the Polars ETL pipeline and
the PostgreSQL Bronze loader.
"""

from pathlib import Path
import shutil

import kagglehub


def download_citi_bike_dataset(target_dir: Path | None = None) -> Path:
    """Download the Citi Bike Stations dataset from Kaggle.

    Args:
        target_dir: Where to symlink/copy the CSVs. Defaults to project_root/data/raw.

    Returns:
        Path to the directory containing the CSV files.
    """
    src = Path(kagglehub.dataset_download("rosenthal/citi-bike-stations"))

    if target_dir is None:
        target_dir = Path(__file__).resolve().parent / "data" / "raw"
    target_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(src.glob("*.csv"))
    for f in csv_files:
        link = target_dir / f.name
        if not link.exists():
            try:
                link.symlink_to(f.resolve())
            except OSError:
                shutil.copy2(f, link)

    print(f"Dataset downloaded and linked to: {target_dir}")
    print(f"CSV files: {len(csv_files)}")
    return target_dir


if __name__ == "__main__":
    download_citi_bike_dataset()
