from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession

METRIC_DATE = "2026-05-13"

SIGNUP_COUNT = 10_000
CHECKIN_COUNT = 7_000


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("generate-fake-fitness-events")
        .master("local[2]")  # limit cpu core
        .getOrCreate()
    )


def generate_client_signups(spark: SparkSession):
    rows = []

    locations = ["SP", "RJ", "MG", "PR", "SC"]

    for i in range(SIGNUP_COUNT):
        signup_time = datetime.fromisoformat(METRIC_DATE) + timedelta(
            seconds=i % 86_400
        )

        rows.append(
            {
                "signup_id": f"signup_{i}",
                "client_id": f"client_{i}",
                "signup_at": signup_time,
                "signup_date": METRIC_DATE,
                "location_id": locations[i % len(locations)],
            }
        )

    return spark.createDataFrame(rows)


def generate_checkins(spark: SparkSession):
    rows = []

    locations = ["SP", "RJ", "MG", "PR", "SC"]

    for i in range(CHECKIN_COUNT):
        checkin_time = datetime.fromisoformat(METRIC_DATE) + timedelta(
            seconds=i % 86_400
        )

        rows.append(
            {
                "checkin_id": f"checkin_{i}",
                "client_id": f"client_{i}",
                "checkin_at": checkin_time,
                "checkin_date": METRIC_DATE,
                "location_id": locations[i % len(locations)],
            }
        )

    return spark.createDataFrame(rows)


def main() -> None:
    spark = create_spark()

    signup_output = Path(
        f"sources/subscriptions/date={METRIC_DATE}/client_signups.parquet"
    )

    checkin_output = Path(f"sources/checkins/date={METRIC_DATE}/checkins.parquet")

    signup_output.parent.mkdir(parents=True, exist_ok=True)
    checkin_output.parent.mkdir(parents=True, exist_ok=True)

    signups_df = generate_client_signups(spark)
    checkins_df = generate_checkins(spark)

    signups_df.write.mode("overwrite").parquet(str(signup_output))

    checkins_df.write.mode("overwrite").parquet(str(checkin_output))

    spark.stop()

    print(f"Generated signups: {signup_output}")
    print(f"Generated checkins: {checkin_output}")


if __name__ == "__main__":
    main()
