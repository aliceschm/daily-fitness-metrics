from pathlib import Path

from pyspark.sql import SparkSession


def build_daily_metrics(
    spark: SparkSession,
    metric_date: str,
    daily_signups_base_path: str = "staging_metrics/daily_signups",
    daily_checkins_base_path: str = "staging_metrics/daily_checkins",
    daily_active_clients_base_path: str = "staging_metrics/daily_active_clients",
    daily_usage_rate_base_path: str = "staging_metrics/daily_usage_rate",
    output_base_path: str = "warehouse/daily_metrics",
) -> None:
    signups_path = Path(daily_signups_base_path) / f"date={metric_date}"
    checkins_path = Path(daily_checkins_base_path) / f"date={metric_date}"
    active_clients_path = Path(daily_active_clients_base_path) / f"date={metric_date}"
    usage_rate_path = Path(daily_usage_rate_base_path) / f"date={metric_date}"

    output_path = Path(output_base_path) / f"date={metric_date}"

    daily_signups_df = spark.read.parquet(str(signups_path))
    daily_checkins_df = spark.read.parquet(str(checkins_path))
    daily_active_clients_df = spark.read.parquet(str(active_clients_path))
    daily_usage_rate_df = spark.read.parquet(str(usage_rate_path))

    daily_metrics_df = (
        daily_signups_df
        .join(daily_checkins_df, on="metric_date", how="inner")
        .join(daily_active_clients_df, on="metric_date", how="inner")
        .join(daily_usage_rate_df, on="metric_date", how="inner")
        .select(
            "metric_date",
            "signup_count",
            "checkin_count",
            "active_clients",
            "usage_rate",
        )
    )

    daily_metrics_df.write.mode("overwrite").parquet(str(output_path))