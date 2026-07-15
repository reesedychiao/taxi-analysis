import sys
from calendar import monthrange
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import requests

from config import NYC_LATITUDE, NYC_LONGITUDE, STUDY_END_DATE, STUDY_START_DATE, WEATHER_PATH

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARS = "temperature_2m,precipitation,snowfall"

_last_day_of_end_month = monthrange(STUDY_END_DATE.year, STUDY_END_DATE.month)[1]
STUDY_END_INCLUSIVE = date(STUDY_END_DATE.year, STUDY_END_DATE.month, _last_day_of_end_month)


def _year_chunks(start: date, end: date):
    for year in range(start.year, end.year + 1):
        chunk_start = max(start, date(year, 1, 1))
        chunk_end = min(end, date(year, 12, 31))
        yield chunk_start, chunk_end


def fetch_weather() -> pd.DataFrame:
    frames = []
    for chunk_start, chunk_end in _year_chunks(STUDY_START_DATE, STUDY_END_INCLUSIVE):
        resp = requests.get(
            ARCHIVE_URL,
            params={
                "latitude": NYC_LATITUDE,
                "longitude": NYC_LONGITUDE,
                "start_date": chunk_start.isoformat(),
                "end_date": chunk_end.isoformat(),
                "hourly": HOURLY_VARS,
                "timezone": "America/New_York",
            },
            timeout=60,
        )
        resp.raise_for_status()
        hourly = resp.json()["hourly"]
        frames.append(pd.DataFrame(hourly))

    weather = pd.concat(frames, ignore_index=True)
    weather["time"] = pd.to_datetime(weather["time"])
    weather = weather.rename(columns={"time": "weather_datetime"})
    return weather


if __name__ == "__main__":
    weather = fetch_weather()
    weather.to_parquet(WEATHER_PATH, index=False)
    print(f"rows: {len(weather)}")
    print(f"range: {weather['weather_datetime'].min()} to {weather['weather_datetime'].max()}")
    print(f"nulls per column:\n{weather.isnull().sum()}")
    print(f"saved to {WEATHER_PATH}")
