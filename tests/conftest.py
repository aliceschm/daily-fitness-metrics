import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("daily-fitness-metrics-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


def collect_one(df):
    rows = df.collect()
    assert len(rows) == 1
    return rows[0].asDict()
