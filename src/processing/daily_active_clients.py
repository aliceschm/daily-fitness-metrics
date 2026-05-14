from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    lit,
)


def build_daily_active_clients(
    spark: SparkSession,
    metric_date: str,
    source_path: str = "sources/clients/clients.csv",
    output_base_path: str = "staging_metrics/daily_active_clients",
) -> None:
    output_path = (
        Path(output_base_path)
        / f"date={metric_date}"
    )

    clients_df = (
        spark.read
        .option("header", True)
        .csv(source_path)
    )

    daily_active_clients_df = (
        clients_df
        .filter(col("status") == "active")
        .agg(
            countDistinct("client_id")
            .alias("active_clients")
        )
        .withColumn("metric_date", lit(metric_date))
        .select(
            "metric_date",
            "active_clients",
        )
    )

    daily_active_clients_df.write.mode(
        "overwrite"
    ).parquet(str(output_path))