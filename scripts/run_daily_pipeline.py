import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

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


def resolve_metric_dates(
    metric_date: str | None,
    start_date: str | None,
    end_date: str | None,
) -> list[str]:
    if metric_date:
        return [metric_date]

    return list(iter_dates(start_date, end_date))


def run_for_date(
    spark: SparkSession,
    metric_date: str,
    sources_base_path: str = "sources",
    staging_base_path: str = "staging_metrics",
    warehouse_base_path: str = "warehouse",
    show_output: bool = True,
) -> None:
    sources_path = Path(sources_base_path)
    staging_path = Path(staging_base_path)
    warehouse_path = Path(warehouse_base_path)

    build_daily_signups(
        spark=spark,
        metric_date=metric_date,
        source_base_path=str(sources_path / "subscriptions"),
        output_base_path=str(staging_path / "daily_signups"),
    )
    build_daily_checkins(
        spark=spark,
        metric_date=metric_date,
        source_base_path=str(sources_path / "checkins"),
        output_base_path=str(staging_path / "daily_checkins"),
    )
    build_daily_active_clients(
        spark=spark,
        metric_date=metric_date,
        source_path=str(sources_path / "clients" / "clients.csv"),
        output_base_path=str(staging_path / "daily_active_clients"),
    )
    build_daily_usage_rate(
        spark=spark,
        metric_date=metric_date,
        daily_checkins_base_path=str(staging_path / "daily_checkins"),
        daily_active_clients_base_path=str(staging_path / "daily_active_clients"),
        output_base_path=str(staging_path / "daily_usage_rate"),
    )
    build_daily_metrics(
        spark=spark,
        metric_date=metric_date,
        daily_signups_base_path=str(staging_path / "daily_signups"),
        daily_checkins_base_path=str(staging_path / "daily_checkins"),
        daily_active_clients_base_path=str(staging_path / "daily_active_clients"),
        daily_usage_rate_base_path=str(staging_path / "daily_usage_rate"),
        output_base_path=str(warehouse_path / "daily_metrics"),
    )

    if show_output:
        final_df = spark.read.parquet(
            str(warehouse_path / "daily_metrics" / f"date={metric_date}")
        )
        final_df.show()


def main(
    args: list[str] | None = None,
    sources_base_path: str = "sources",
    staging_base_path: str = "staging_metrics",
    warehouse_base_path: str = "warehouse",
    show_output: bool = True,
) -> None:
    parsed_args = parse_args(args)
    metric_dates = resolve_metric_dates(
        metric_date=parsed_args.metric_date,
        start_date=parsed_args.start_date,
        end_date=parsed_args.end_date,
    )

    spark = create_spark()

    try:
        run_pipeline(
            spark=spark,
            metric_dates=metric_dates,
            sources_base_path=sources_base_path,
            staging_base_path=staging_base_path,
            warehouse_base_path=warehouse_base_path,
            show_output=show_output,
        )

    finally:
        spark.stop()


def run_pipeline(
    spark: SparkSession,
    metric_dates: list[str],
    sources_base_path: str = "sources",
    staging_base_path: str = "staging_metrics",
    warehouse_base_path: str = "warehouse",
    show_output: bool = True,
) -> None:
    for current_date in metric_dates:
        print(f"Processing metrics for {current_date}")
        run_for_date(
            spark=spark,
            metric_date=current_date,
            sources_base_path=sources_base_path,
            staging_base_path=staging_base_path,
            warehouse_base_path=warehouse_base_path,
            show_output=show_output,
        )


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("--metric-date")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")

    parsed_args = parser.parse_args(args)

    if parsed_args.metric_date and (parsed_args.start_date or parsed_args.end_date):
        parser.error("Use either --metric-date or --start-date/--end-date, not both.")

    if not parsed_args.metric_date and not (
        parsed_args.start_date and parsed_args.end_date
    ):
        parser.error("Use --metric-date or both --start-date and --end-date.")

    return parsed_args


if __name__ == "__main__":
    main()
