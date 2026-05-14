from pyspark.sql import SparkSession

from src.processing.daily_metrics import build_daily_metrics


METRIC_DATE = "2026-05-13"


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("build-daily-metrics")
        .master("local[2]")
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark()

    build_daily_metrics(
        spark=spark,
        metric_date=METRIC_DATE,
    )

    result_df = spark.read.parquet(
        f"warehouse/daily_metrics/date={METRIC_DATE}"
    )

    result_df.show()

    spark.stop()


if __name__ == "__main__":
    main()