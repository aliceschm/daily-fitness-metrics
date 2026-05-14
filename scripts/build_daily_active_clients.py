from pyspark.sql import SparkSession

from src.processing.daily_active_clients import (
    build_daily_active_clients,
)


METRIC_DATE = "2026-05-13"


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("build-daily-active-clients")
        .master("local[2]")
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark()

    build_daily_active_clients(
        spark=spark,
        metric_date=METRIC_DATE,
    )

    result_df = spark.read.parquet(
        f"staging_metrics/daily_active_clients/date={METRIC_DATE}"
    )

    result_df.show()

    spark.stop()


if __name__ == "__main__":
    main()