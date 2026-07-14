import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when
from pyspark.sql.functions import sum as spark_sum

from config import STUDY_START_DATE, STUDY_END_DATE
from ingestion.load_trips import load_all_raw_trips
from utils.spark import get_spark_session

_end_year = STUDY_END_DATE.year + (1 if STUDY_END_DATE.month == 12 else 0)
_end_month = 1 if STUDY_END_DATE.month == 12 else STUDY_END_DATE.month + 1
STUDY_WINDOW_END_EXCLUSIVE = date(_end_year, _end_month, 1)


def _violations(df: DataFrame) -> dict:
    invalid_zone = ~col("PULocationID").between(1, 263) | ~col("DOLocationID").between(1, 263)
    negative_fare = col("fare_amount") < 0
    negative_distance = col("trip_distance") < 0
    dropoff_before_pickup = col("tpep_dropoff_datetime") < col("tpep_pickup_datetime")
    bad_timestamp = (col("tpep_pickup_datetime") < str(STUDY_START_DATE)) | (
        col("tpep_pickup_datetime") >= str(STUDY_WINDOW_END_EXCLUSIVE)
    )

    return {
        "invalid_zone": invalid_zone,
        "negative_fare": negative_fare,
        "negative_distance": negative_distance,
        "dropoff_before_pickup": dropoff_before_pickup,
        "bad_timestamp": bad_timestamp,
    }


def clean_trips(df: DataFrame) -> DataFrame:
    violations = _violations(df)

    report = df.agg(
        *[spark_sum(when(cond, 1).otherwise(0)).alias(name) for name, cond in violations.items()]
    ).collect()[0]

    total = df.count()
    print(f"total rows before cleaning: {total}")
    for name in violations:
        count = report[name]
        print(f"  {name}: {count} ({100 * count / total:.3f}%)")

    combined_violation = violations["invalid_zone"]
    for cond in list(violations.values())[1:]:
        combined_violation = combined_violation | cond

    cleaned = df.filter(~combined_violation)
    print(f"total rows after cleaning: {cleaned.count()}")
    return cleaned


if __name__ == "__main__":
    spark = get_spark_session("cleaning-smoke-test")
    raw_df = load_all_raw_trips(spark)
    clean_trips(raw_df)
    spark.stop()
