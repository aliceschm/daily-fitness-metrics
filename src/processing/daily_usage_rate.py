from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when


def build_daily_usage_rate_df(
    daily_checkins_df: DataFrame,
    daily_active_clients_df: DataFrame,
) -> DataFrame:
    return (
        daily_checkins_df.join(daily_active_clients_df, on="metric_date", how="inner")
        .withColumn(
            "usage_rate",
            when(
                col("active_clients") == 0,
                0.0,
            ).otherwise(col("checkin_count") / col("active_clients")),
        )
        .select("metric_date", "usage_rate")
    )


def build_daily_usage_rate(
    spark: SparkSession,
    metric_date: str,
    daily_checkins_base_path: str = "staging_metrics/daily_checkins",
    daily_active_clients_base_path: str = "staging_metrics/daily_active_clients",
    output_base_path: str = "staging_metrics/daily_usage_rate",
) -> None:
    checkins_path = Path(daily_checkins_base_path) / f"date={metric_date}"
    active_clients_path = Path(daily_active_clients_base_path) / f"date={metric_date}"
    output_path = Path(output_base_path) / f"date={metric_date}"

    daily_checkins_df = spark.read.parquet(str(checkins_path))
    daily_active_clients_df = spark.read.parquet(str(active_clients_path))

    daily_usage_rate_df = build_daily_usage_rate_df(
        daily_checkins_df=daily_checkins_df,
        daily_active_clients_df=daily_active_clients_df,
    )

    daily_usage_rate_df.write.mode("overwrite").parquet(str(output_path))
