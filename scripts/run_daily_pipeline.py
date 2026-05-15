import argparse
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession

from src.processing.daily_active_clients import build_daily_active_clients
from src.processing.daily_checkins import build_daily_checkins
from src.processing.daily_metrics import build_daily_metrics
from src.processing.daily_signups import build_daily_signups
from src.processing.daily_usage_rate import build_daily_usage_rate


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("daily-fitness-metrics-pipeline")
        .master("local[2]")
        .getOrCreate()
    )


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def iter_dates(start_date: str, end_date: str):
    current = parse_date(start_date)
    end = parse_date(end_date)

    if current > end:
        raise ValueError("start_date must be before or equal to end_date")

    while current <= end:
        yield current.isoformat()
        current += timedelta(days=1)


def run_for_date(spark: SparkSession, metric_date: str) -> None:
    build_daily_signups(spark=spark, metric_date=metric_date)
    build_daily_checkins(spark=spark, metric_date=metric_date)
    build_daily_active_clients(spark=spark, metric_date=metric_date)
    build_daily_usage_rate(spark=spark, metric_date=metric_date)
    build_daily_metrics(spark=spark, metric_date=metric_date)

    final_df = spark.read.parquet(f"warehouse/daily_metrics/date={metric_date}")
    final_df.show()


def main(metric_date: str | None, start_date: str | None, end_date: str | None) -> None:
    spark = create_spark()

    try:
        if metric_date:
            dates = [metric_date]
        else:
            dates = list(iter_dates(start_date, end_date))

        for current_date in dates:
            print(f"Processing metrics for {current_date}")
            run_for_date(spark=spark, metric_date=current_date)

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--metric-date")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")

    args = parser.parse_args()

    if args.metric_date and (args.start_date or args.end_date):
        parser.error("Use either --metric-date or --start-date/--end-date, not both.")

    if not args.metric_date and not (args.start_date and args.end_date):
        parser.error("Use --metric-date or both --start-date and --end-date.")

    main(
        metric_date=args.metric_date,
        start_date=args.start_date,
        end_date=args.end_date,
    )
