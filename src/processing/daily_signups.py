from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, countDistinct


def build_daily_signups_df(signups_df: DataFrame, metric_date: str) -> DataFrame:
    return (
        signups_df.filter(col("signup_date") == metric_date)
        .groupBy("signup_date")
        .agg(countDistinct("client_id").alias("signup_count"))
        .withColumnRenamed("signup_date", "metric_date")
    )


def build_daily_signups(
    spark: SparkSession,
    metric_date: str,
    source_base_path: str = "sources/subscriptions",
    output_base_path: str = "staging_metrics/daily_signups",
) -> None:
    input_path = (
        Path(source_base_path) / f"date={metric_date}" / "client_signups.parquet"
    )
    output_path = Path(output_base_path) / f"date={metric_date}"

    signups_df = spark.read.parquet(str(input_path))

    daily_signups_df = build_daily_signups_df(
        signups_df=signups_df,
        metric_date=metric_date,
    )

    daily_signups_df.write.mode("overwrite").parquet(str(output_path))
