import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import geopandas as gpd
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col, dayofweek, hour, month, to_utc_timestamp, year

from cleaning.clean_trips import clean_trips
from config import CRZ_BOUNDARY_PATH, CRZ_START_DATE, ZONE_LOOKUP_PATH, ZONE_SHAPEFILE_PATH
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

NYC_TZ = "America/New_York"


WEEKEND_DAYOFWEEK_VALUES = (1, 7)


def handle_nulls(df: DataFrame) -> DataFrame:
    return df.fillna(FILL_VALUES)


def add_time_features(df: DataFrame) -> DataFrame:
    df = (
        df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
        .withColumn("pickup_weekday", dayofweek(col("tpep_pickup_datetime")))
        .withColumn("pickup_year", year(col("tpep_pickup_datetime")))
        .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
        .withColumn("pickup_is_weekend", col("pickup_weekday").isin(*WEEKEND_DAYOFWEEK_VALUES))
    )

    df = df.withColumn(
        "tpep_pickup_datetime_utc", to_utc_timestamp(col("tpep_pickup_datetime"), NYC_TZ)
    ).withColumn("tpep_dropoff_datetime_utc", to_utc_timestamp(col("tpep_dropoff_datetime"), NYC_TZ))

    df = df.withColumn("post_congestion_pricing", col("tpep_pickup_datetime") >= str(CRZ_START_DATE))

    return df


def join_zone_lookup(spark, df: DataFrame) -> DataFrame:
    zones = (
        spark.read.csv(str(ZONE_LOOKUP_PATH), header=True, inferSchema=True)
        .select(
            col("LocationID").alias("PULocationID"),
            col("Borough").alias("pickup_borough"),
            col("Zone").alias("pickup_zone_name"),
        )
    )
    return df.join(broadcast(zones), on="PULocationID", how="left")


def compute_crz_zone_flags():
    zones = gpd.read_file(str(ZONE_SHAPEFILE_PATH))
    crz_union = gpd.read_file(str(CRZ_BOUNDARY_PATH)).to_crs(zones.crs).union_all()

    overlap_fraction = zones.geometry.intersection(crz_union).area / zones.geometry.area
    result = zones[["LocationID"]].rename(columns={"LocationID": "PULocationID"}).copy()
    result["crz_overlap_fraction"] = overlap_fraction
    result["is_crz_zone"] = result["crz_overlap_fraction"] > 0.5
    return result


def join_crz_flag(spark, df: DataFrame) -> DataFrame:
    crz_flags = spark.createDataFrame(compute_crz_zone_flags()).select(
        "PULocationID", col("is_crz_zone").alias("pickup_is_crz_zone")
    )
    return df.join(broadcast(crz_flags), on="PULocationID", how="left")


def enrich_trips(spark, df: DataFrame) -> DataFrame:
    df = handle_nulls(df)
    df = add_time_features(df)
    df = join_zone_lookup(spark, df)
    df = join_crz_flag(spark, df)
    return df


if __name__ == "__main__":
    spark = get_spark_session("enrich-trips-smoke-test")
    df = enrich_trips(spark, clean_trips(load_all_raw_trips(spark)))
    print("unmatched pickup zones (should be 0):", df.filter(col("pickup_borough").isNull()).count())
    print("unmatched CRZ flags (should be 0):", df.filter(col("pickup_is_crz_zone").isNull()).count())
    print("CRZ zone count:", compute_crz_zone_flags()["is_crz_zone"].sum(), "/ 263")
    df.select(
        "PULocationID",
        "pickup_borough",
        "pickup_zone_name",
        "pickup_is_crz_zone",
        "pickup_hour",
        "pickup_weekday",
        "pickup_is_weekend",
        "post_congestion_pricing",
    ).show(10, truncate=False)
    df.groupBy("post_congestion_pricing").count().show()
    spark.stop()
