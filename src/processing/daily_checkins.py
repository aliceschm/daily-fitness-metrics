from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, countDistinct


def build_daily_checkins_df(checkins_df: DataFrame, metric_date: str) -> DataFrame:
    return (
        checkins_df.filter(col("checkin_date") == metric_date)
        .groupBy("checkin_date")
        .agg(countDistinct("client_id").alias("checkin_count"))
        .withColumnRenamed("checkin_date", "metric_date")
    )


def build_daily_checkins(
    spark: SparkSession,
    metric_date: str,
    source_base_path: str = "sources/checkins",
    output_base_path: str = "staging_metrics/daily_checkins",
) -> None:
    input_path = Path(source_base_path) / f"date={metric_date}" / "checkins.parquet"

    output_path = Path(output_base_path) / f"date={metric_date}"

    checkins_df = spark.read.parquet(str(input_path))

    daily_checkins_df = build_daily_checkins_df(
        checkins_df=checkins_df,
        metric_date=metric_date,
    )

    daily_checkins_df.write.mode("overwrite").parquet(str(output_path))
