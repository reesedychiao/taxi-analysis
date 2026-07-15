# NYC Taxi Analysis Project

## Problem

Every shift, a NYC taxi driver makes the same high-stakes bet dozens of times: where to go after dropping off a fare. Guess wrong, and they're circling empty streets while demand is stacking up a few zones over. There's no dispatcher solving this for them as yellow cabs are hailed off the street, not routed centrally. Hence, drivers are left to rely on gut feel and habit to decide where to position themselves next in order to maximize net earnings per hour.

At the same time, the city is in the middle of a contested policy debate: since January 5, 2025, the MTA's Manhattan Congestion Relief Zone (CRZ) has tolled vehicles entering lower Manhattan, and stakeholders disagree on whether it has helped or hurt the taxi market.

This project addresses both, on top of the same NYC TLC trip data:

1. A **driver earnings optimizer** that forecasts hourly demand and fares by taxi zone and turns that forecast into a ranked, uncertainty-aware recommendation of where to drive next.
2. A **congestion pricing impact study** that uses causal-inference methods (difference-in-differences, interrupted time series) to produce a statistically defensible answer to the CRZ toll's effect on demand, revenue, and trip patterns.

## Data

- **Source:** [NYC TLC Yellow Taxi trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (monthly parquet, ~2-month publish lag)
- **Window:** 2023-01 through the latest available month (~24 months pre / 14+ months post the Jan 5, 2025 CRZ toll start)
- **Grain:** raw trip-level records, aggregated to `(zone_id, hour)` for modeling
- **Supporting data:** TLC Taxi Zone Lookup + shapefile, CRZ boundary geojson, hourly weather (Open-Meteo/NOAA), NYC holiday calendar

## Architecture

```
Raw TLC + weather + zone/CRZ boundary data
  → ETL (PySpark) + Great Expectations validation
  → Clean partitioned Parquet
  → Feature engineering (temporal, spatial, weather, lag/rolling)
       ├─→ Forecasting: model training → MLflow → FastAPI /predict, /recommend
       └─→ Causal: DiD / interrupted time-series → effect estimates + diagnostics
  → Dashboard (both pillars)
```

## Environment Setup

**Steps**

```bash
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Run project code with `.venv/bin/python`, or `source .venv/bin/activate` first.

**Experiment tracking (MLflow)**

To view runs:

```bash
.venv/bin/mlflow ui --backend-store-uri "sqlite:///mlflow.db"
```

Then open http://127.0.0.1:5000. Any code that logs runs should call `mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)` first, rather than relying on MLflow's default.

## ETL Pipeline: Raw → Processed

Raw monthly parquet in `data/raw/` → one partitioned dataset at `data/processed/trips/`, built with PySpark (DuckDB is used separately, only as an independent spot-check).

- **Ingestion** (`src/ingestion/`) — `validate_raw.py` diagnostically checks each raw month against two GX suites (schema + validity); caught real drift across 2023→2025+ files. `load_trips.py` loads and harmonizes every month into **147,201,830 raw rows**.
- **Cleaning & enrichment** (`src/cleaning/`) — `clean_trips.py` drops invalid zone IDs, negative fare/distance, dropoff-before-pickup, and out-of-window timestamps → **141,606,032 rows (3.80% dropped)**. `enrich_trips.py` fills nulls (documented per-column, never row drops), derives `pickup_hour`/`weekday`/`month`/`year`/`is_weekend` + `post_congestion_pricing`, and joins zone/borough names plus a geopandas-derived `pickup_is_crz_zone` flag. `write_processed.py` writes it partitioned by `pickup_year`/`pickup_month` → **230 files, ~20MB avg, 4.6GB**. `validate_processed.py` is a hard gate (20/20 checks must pass) via GX's Spark datasource.
- **Independent verification** (`src/analytics/`) — `spot_check_processed.py` re-derives row counts, partitions, and the CRZ flag via DuckDB, deliberately a different engine than the pipeline itself.

```bash
.venv/bin/python src/cleaning/write_processed.py       # ingestion + cleaning + enrichment, writes output
.venv/bin/python src/cleaning/validate_processed.py    # gate: exits non-zero on failure
.venv/bin/python src/analytics/spot_check_processed.py # independent DuckDB re-check
```
