import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb

import config

# Deliberately independent of the Spark pipeline that produced this data: DuckDB reads the
# same Parquet files through a completely different engine, so a bug specific to Spark's
# behavior wouldn't be masked by re-checking with the same tool that made the mistake.

KNOWN_CRZ_ZONES = {
    "Battery Park City": True,
    "World Trade Center": True,
    "SoHo": True,
    "Greenwich Village South": True,
    "Times Sq/Theatre District": True,
    "Midtown Center": True,
    "Battery Park": False,
    "Central Park": False,
    "Upper East Side North": False,
    "Yorkville East": False,
    "JFK Airport": False,
    "LaGuardia Airport": False,
    "Astoria": False,
}


def row_counts(con) -> tuple[int, int]:
    raw = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{config.RAW_DIR}/yellow_tripdata_*.parquet', union_by_name=true)"
    ).fetchone()[0]
    processed = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{config.PROCESSED_DIR}/trips/*/*/*.parquet', hive_partitioning=true)"
    ).fetchone()[0]
    return raw, processed


def partition_structure(con):
    return con.execute(
        f"""
        SELECT pickup_year, pickup_month, COUNT(*) AS rows
        FROM read_parquet('{config.PROCESSED_DIR}/trips/*/*/*.parquet', hive_partitioning=true)
        GROUP BY pickup_year, pickup_month
        ORDER BY pickup_year, pickup_month
        """
    ).fetchdf()


def crz_flag_check(con):
    zone_list = "', '".join(KNOWN_CRZ_ZONES)
    return con.execute(
        f"""
        SELECT DISTINCT pickup_zone_name, pickup_is_crz_zone
        FROM read_parquet('{config.PROCESSED_DIR}/trips/*/*/*.parquet', hive_partitioning=true)
        WHERE pickup_zone_name IN ('{zone_list}')
        """
    ).fetchdf()


if __name__ == "__main__":
    con = duckdb.connect(str(config.DUCKDB_PATH))

    raw_count, processed_count = row_counts(con)
    print(f"raw row count:       {raw_count}")
    print(f"processed row count: {processed_count}")
    print(f"dropped by cleaning: {raw_count - processed_count} ({100 * (raw_count - processed_count) / raw_count:.2f}%)")

    partitions = partition_structure(con)
    print(f"\npartitions: {len(partitions)} (expected 41, one per month in the study window)")
    if partitions["rows"].min() < 1_000_000:
        print("  WARNING: at least one partition has a suspiciously low row count")

    print("\nCRZ flag check against known zones:")
    crz_result = crz_flag_check(con).set_index("pickup_zone_name")["pickup_is_crz_zone"].to_dict()
    all_correct = True
    for zone, expected in KNOWN_CRZ_ZONES.items():
        actual = crz_result.get(zone)
        ok = actual == expected
        all_correct &= ok
        print(f"  {'OK  ' if ok else 'FAIL'} {zone}: expected={expected} actual={actual}")

    con.close()

    if raw_count == 0 or processed_count == 0 or len(partitions) != 41 or not all_correct:
        sys.exit(1)
