from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct


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

    daily_signups_df = (
        signups_df.groupBy("signup_date")
        .agg(countDistinct("client_id").alias("signup_count"))
        .withColumnRenamed("signup_date", "metric_date")
    )

    daily_signups_df.write.mode("overwrite").parquet(str(output_path))
