import sys
from pathlib import Path

import great_expectations as gx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import RAW_DIR

# TLC yellow trip schema, column -> acceptable pandas dtypes. Timestamp columns get both
# datetime64[ns] and datetime64[us] since pyarrow's parquet reader picks the unit per file.
RAW_TRIP_SCHEMA: dict[str, list[str]] = {
    "VendorID": ["int64"],
    "tpep_pickup_datetime": ["datetime64[ns]", "datetime64[us]"],
    "tpep_dropoff_datetime": ["datetime64[ns]", "datetime64[us]"],
    "passenger_count": ["float64"],
    "trip_distance": ["float64"],
    "RatecodeID": ["float64"],
    "store_and_fwd_flag": ["str"],
    "PULocationID": ["int64"],
    "DOLocationID": ["int64"],
    "payment_type": ["int64"],
    "fare_amount": ["float64"],
    "extra": ["float64"],
    "mta_tax": ["float64"],
    "tip_amount": ["float64"],
    "tolls_amount": ["float64"],
    "improvement_surcharge": ["float64"],
    "total_amount": ["float64"],
    "congestion_surcharge": ["float64"],
    "airport_fee": ["float64"],
}


def build_raw_trip_schema_suite() -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name="raw_trip_schema")

    suite.add_expectation(
        gx.expectations.ExpectTableColumnsToMatchSet(column_set=list(RAW_TRIP_SCHEMA), exact_match=False)
    )
    for column, type_list in RAW_TRIP_SCHEMA.items():
        suite.add_expectation(gx.expectations.ExpectColumnToExist(column=column))
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInTypeList(column=column, type_list=type_list))
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1))

    return suite


def build_raw_trip_validity_suite() -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name="raw_trip_validity")

    for column in ("PULocationID", "DOLocationID"):
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column=column, min_value=1, max_value=265))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="fare_amount", min_value=0))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="trip_distance", min_value=0))

    suite.add_expectation(
        gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="tpep_dropoff_datetime", column_B="tpep_pickup_datetime", or_equal=True
        )
    )

    return suite


def _get_batch(context: gx.data_context.AbstractDataContext, parquet_path: Path):
    data_source = context.data_sources.add_pandas(f"raw_trips_{parquet_path.stem}")
    asset = data_source.add_parquet_asset(parquet_path.stem, path=parquet_path)
    return asset.add_batch_definition_whole_dataframe("whole_file").get_batch()


def validate_month(parquet_path: Path, suite_builder=build_raw_trip_schema_suite):
    context = gx.get_context(mode="ephemeral")
    suite = context.suites.add(suite_builder())
    batch = _get_batch(context, parquet_path)
    return batch.validate(suite)


def _print_result(label: str, result) -> bool:
    print(f"{label}: success={result.success}  ({sum(r.success for r in result.results)}/{len(result.results)} expectations passed)")
    for r in result.results:
        if not r.success:
            detail = r.result
            if "unexpected_percent" in detail:
                print(
                    f"  FAILED: {r.expectation_config.type} {r.expectation_config.kwargs}"
                    f" -> {detail['unexpected_count']}/{detail['element_count']} rows"
                    f" ({detail['unexpected_percent']:.2f}%) violate this"
                )
            else:
                print(f"  FAILED: {r.expectation_config.type} {r.expectation_config.kwargs}")
    return result.success


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "yellow_tripdata_2023-01.parquet"
    schema_ok = _print_result("schema", validate_month(RAW_DIR / target, build_raw_trip_schema_suite))
    validity_ok = _print_result("validity", validate_month(RAW_DIR / target, build_raw_trip_validity_suite))
    if not (schema_ok and validity_ok):
        sys.exit(1)
