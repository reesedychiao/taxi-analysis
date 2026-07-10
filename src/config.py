import re
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
FEATURES_DIR = DATA_DIR / "features"

ZONE_LOOKUP_PATH = RAW_DIR / "taxi_zone_lookup.csv"
ZONE_SHAPEFILE_PATH = RAW_DIR / "taxi_zones.shp"
CRZ_BOUNDARY_PATH = RAW_DIR / "mta_crz.geojson"

EXCLUDED_ZONE_IDS = {264, 265}

CRZ_START_DATE = date(2025, 1, 5)

STUDY_START_DATE = date(2023, 1, 1)

_TRIP_FILE_PATTERN = re.compile(r"yellow_tripdata_(\d{4})-(\d{2})\.parquet$")


def latest_available_month(raw_dir: Path = RAW_DIR) -> date:
    months = []
    for f in raw_dir.glob("yellow_tripdata_*.parquet"):
        match = _TRIP_FILE_PATTERN.search(f.name)
        if match:
            year, month = int(match.group(1)), int(match.group(2))
            months.append(date(year, month, 1))
    if not months:
        raise FileNotFoundError(f"No yellow_tripdata_*.parquet files found in {raw_dir}")
    return max(months)


STUDY_END_DATE = latest_available_month()
