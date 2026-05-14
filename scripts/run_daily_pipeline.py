import argparse

from pyspark.sql import SparkSession

from src.processing.daily_active_clients import (
    build_daily_active_clients,
)
from src.processing.daily_checkins import (
    build_daily_checkins,
)
from src.processing.daily_metrics import (
    build_daily_metrics,
)
from src.processing.daily_signups import (
    build_daily_signups,
)
from src.processing.daily_usage_rate import (
    build_daily_usage_rate,
)


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("daily-fitness-metrics-pipeline")
        .master("local[2]")
        .getOrCreate()
    )


def main(metric_date: str) -> None:
    spark = create_spark()

    build_daily_signups(
        spark=spark,
        metric_date=metric_date,
    )

    build_daily_checkins(
        spark=spark,
        metric_date=metric_date,
    )

    build_daily_active_clients(
        spark=spark,
        metric_date=metric_date,
    )

    build_daily_usage_rate(
        spark=spark,
        metric_date=metric_date,
    )

    build_daily_metrics(
        spark=spark,
        metric_date=metric_date,
    )

    final_df = spark.read.parquet(f"warehouse/daily_metrics/date={metric_date}")

    final_df.show()

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--metric-date",
        required=True,
    )

    args = parser.parse_args()

    main(metric_date=args.metric_date)
