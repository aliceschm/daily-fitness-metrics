from src.processing.daily_signups import build_daily_signups_df
from tests.conftest import collect_one


def test_build_daily_signups_df_counts_distinct_clients_for_date(spark):
    input_df = spark.createDataFrame(
        [
            {"client_id": "client_1", "signup_date": "2026-05-13"},
            {"client_id": "client_1", "signup_date": "2026-05-13"},
            {"client_id": "client_2", "signup_date": "2026-05-13"},
            {"client_id": "client_3", "signup_date": "2026-05-14"},
        ]
    )

    result = collect_one(build_daily_signups_df(input_df, "2026-05-13"))

    assert result == {
        "metric_date": "2026-05-13",
        "signup_count": 2,
    }
