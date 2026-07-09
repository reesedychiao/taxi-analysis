import os
import sys

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "taxi-analysis") -> SparkSession:
    # Spark's Python worker must match the driver's Python version exactly,
    # or every job fails with PYTHON_VERSION_MISMATCH. Pinning both to the
    # interpreter actually running this code (sys.executable) makes that
    # correct regardless of which python3 happens to be first on PATH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return SparkSession.builder.appName(app_name).getOrCreate()
