import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import DataFrame

from cleaning.clean_trips import clean_trips
from cleaning.enrich_trips import enrich_trips
from config import PROCESSED_DIR
from ingestion.load_trips import load_all_raw_trips
from utils.spark import get_spark_session

OUTPUT_PATH = PROCESSED_DIR / "trips"
PARTITION_COLUMNS = ["pickup_year", "pickup_month"]


def write_processed_trips(df: DataFrame) -> None:
    df.write.mode("overwrite").partitionBy(*PARTITION_COLUMNS).parquet(str(OUTPUT_PATH))


if __name__ == "__main__":
    spark = get_spark_session("write-processed-trips")
    df = enrich_trips(spark, clean_trips(load_all_raw_trips(spark)))
    write_processed_trips(df)

    written = spark.read.parquet(str(OUTPUT_PATH))
    print("rows written:", written.count())
    spark.stop()
