from pyspark.sql import SparkSession

from src.processing.daily_usage_rate import build_daily_usage_rate


METRIC_DATE = "2026-05-13"


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("build-daily-usage-rate")
        .master("local[2]")
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark()

    build_daily_usage_rate(
        spark=spark,
        metric_date=METRIC_DATE,
    )

    result_df = spark.read.parquet(
        f"staging_metrics/daily_usage_rate/date={METRIC_DATE}"
    )

    result_df.show()

    spark.stop()


if __name__ == "__main__":
    main()