import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import great_expectations as gx

from config import PROCESSED_DIR, STUDY_END_DATE, STUDY_START_DATE
from utils.spark import get_spark_session

EXPECTED_COLUMNS = [
    "PULocationID", "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
    "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge",
    "airport_fee", "cbd_congestion_fee", "pickup_hour", "pickup_weekday",
    "pickup_is_weekend", "tpep_pickup_datetime_utc", "tpep_dropoff_datetime_utc",
    "post_congestion_pricing", "pickup_borough", "pickup_zone_name", "pickup_is_crz_zone",
    "pickup_year", "pickup_month",
]

NOT_NULL_COLUMNS = [
    "passenger_count", "RatecodeID", "store_and_fwd_flag", "congestion_surcharge",
    "airport_fee", "cbd_congestion_fee", "pickup_borough", "pickup_zone_name",
    "pickup_is_crz_zone",
]


def build_processed_trip_suite() -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name="processed_trip_gate")

    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchSet(column_set=EXPECTED_COLUMNS, exact_match=True)
    )
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1))

    for column in ("PULocationID", "DOLocationID"):
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column=column, min_value=1, max_value=263))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="fare_amount", min_value=0))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="trip_distance", min_value=0))
    suite.add_expectation(
        gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="tpep_dropoff_datetime", column_B="tpep_pickup_datetime", or_equal=True
        )
    )

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="pickup_hour", min_value=0, max_value=23))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="pickup_weekday", min_value=1, max_value=7))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="pickup_month", min_value=1, max_value=12))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="pickup_year", min_value=STUDY_START_DATE.year, max_value=STUDY_END_DATE.year
        )
    )

    for column in NOT_NULL_COLUMNS:
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=column))

    return suite


def validate_processed_trips():
    spark = get_spark_session("validate-processed-gate")
    df = spark.read.parquet(str(PROCESSED_DIR / "trips"))

    context = gx.get_context(mode="ephemeral")
    suite = context.suites.add(build_processed_trip_suite())

    data_source = context.data_sources.add_spark("processed_trips")
    asset = data_source.add_dataframe_asset("trips")
    batch = asset.add_batch_definition_whole_dataframe("whole").get_batch(batch_parameters={"dataframe": df})

    result = batch.validate(suite)
    spark.stop()
    return result


def _print_result(result) -> bool:
    print(f"success={result.success}  ({sum(r.success for r in result.results)}/{len(result.results)} expectations passed)")
    for r in result.results:
        if not r.success:
            detail = r.result
            if "unexpected_percent" in detail:
                print(
                    f"  FAILED: {r.expectation_config.type} {r.expectation_config.kwargs}"
                    f" -> {detail['unexpected_count']}/{detail['element_count']} rows"
                    f" ({detail['unexpected_percent']:.4f}%) violate this"
                )
            else:
                print(f"  FAILED: {r.expectation_config.type} {r.expectation_config.kwargs} -> {detail}")
    return result.success


if __name__ == "__main__":
    ok = _print_result(validate_processed_trips())
    if not ok:
        sys.exit(1)
