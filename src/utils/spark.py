import os
import sys

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "taxi-analysis", driver_memory: str = "8g") -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return SparkSession.builder.appName(app_name).config("spark.driver.memory", driver_memory).getOrCreate()
