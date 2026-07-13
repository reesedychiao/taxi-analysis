import sys
from datetime import date
from functools import reduce
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from config import RAW_DIR, STUDY_START_DATE, STUDY_END_DATE, _TRIP_FILE_PATTERN
from utils.spark import get_spark_session

RENAME_MAP = {"Airport_fee": "airport_fee"}
RECAST_COLS = ["VendorID", "PULocationID", "DOLocationID"]

def _files_in_study_window() -> list[Path]:
    files = []
    for f in RAW_DIR.glob("yellow_tripdata_*.parquet"):
        match = _TRIP_FILE_PATTERN.search(f.name)
        if not match:
            continue
        year, month = int(match.group(1)), int(match.group(2))
        if STUDY_START_DATE <= date(year, month, 1) <= STUDY_END_DATE:
            files.append(f)
    return sorted(files)


def _load_one_month(spark, path: Path) -> DataFrame:
    df = spark.read.parquet(str(path))
    df = df.withColumnsRenamed(RENAME_MAP)
    for c in RECAST_COLS:
        df = df.withColumn(c, col(c).cast("long"))
    return df


def load_all_raw_trips(spark) -> DataFrame:
    files = _files_in_study_window()
    if not files:
        raise FileNotFoundError(f"No trip files found in study window in {RAW_DIR}")
    monthly_dfs = [_load_one_month(spark, f) for f in files]
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), monthly_dfs)


if __name__ == "__main__":
    spark = get_spark_session("ingestion-smoke-test")
    df = load_all_raw_trips(spark)
    print("row count:", df.count())
    df.printSchema()
    df.selectExpr("min(tpep_pickup_datetime)", "max(tpep_pickup_datetime)").show(truncate=False)
    spark.stop()
