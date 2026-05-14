import argparse

from pyspark.sql import SparkSession

from src.processing.daily_checkins import (
    build_daily_checkins,
)


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("build-daily-checkins")
        .master("local[2]")
        .getOrCreate()
    )


def main(metric_date: str) -> None:
    spark = create_spark()

    build_daily_checkins(
        spark=spark,
        metric_date=metric_date,
    )

    result_df = spark.read.parquet(f"staging_metrics/daily_checkins/date={metric_date}")

    result_df.show()

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--metric-date",
        required=True,
    )

    args = parser.parse_args()

    main(metric_date=args.metric_date)
