import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import DataFrame

from cleaning.clean_trips import clean_trips
from ingestion.load_trips import load_all_raw_trips
from utils.spark import get_spark_session


FILL_VALUES = {
    "passenger_count": 1.0,  # empirical mode: 95.1M/122M non-null rows (~78%) are 1.0
    "RatecodeID": 99.0,  # TLC's own "Null/unknown" rate code, already used 2.1M times elsewhere
    "store_and_fwd_flag": "U",  # only Y/N otherwise; "U" = unknown, not a guessed Y or N
    "congestion_surcharge": 0.0,  # monetary component; null here doesn't mean total_amount is missing
    "airport_fee": 0.0,  # same reasoning as congestion_surcharge
    # Structural, not a data-quality gap: null = pre-CRZ (column didn't exist) or a trip that
    # never entered the CRZ -- either way the correct value is "$0 charged."
    "cbd_congestion_fee": 0.0,
}


def handle_nulls(df: DataFrame) -> DataFrame:
    return df.fillna(FILL_VALUES)


if __name__ == "__main__":
    spark = get_spark_session("null-handling-smoke-test")
    df = handle_nulls(clean_trips(load_all_raw_trips(spark)))
    df.select(list(FILL_VALUES)).summary("count").show()
    spark.stop()
