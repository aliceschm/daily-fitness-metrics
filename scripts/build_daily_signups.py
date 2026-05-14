from pyspark.sql import SparkSession

from src.processing.daily_signups import build_daily_signups


METRIC_DATE = "2026-05-13"


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("build-daily-signups")
        .master("local[2]")
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark()

    build_daily_signups(
        spark=spark,
        metric_date=METRIC_DATE,
    )

    result_df = spark.read.parquet(
        f"staging_metrics/daily_signups/date={METRIC_DATE}"
    )

    result_df.show()

    spark.stop()


if __name__ == "__main__":
    main()