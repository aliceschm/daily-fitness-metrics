from src.processing.daily_checkins import build_daily_checkins_df
from tests.conftest import collect_one


def test_build_daily_checkins_df_counts_distinct_clients_for_date(spark):
    input_df = spark.createDataFrame(
        [
            {"client_id": "client_1", "checkin_date": "2026-05-13"},
            {"client_id": "client_1", "checkin_date": "2026-05-13"},
            {"client_id": "client_2", "checkin_date": "2026-05-13"},
            {"client_id": "client_3", "checkin_date": "2026-05-14"},
        ]
    )

    result = collect_one(build_daily_checkins_df(input_df, "2026-05-13"))

    assert result == {
        "metric_date": "2026-05-13",
        "checkin_count": 2,
    }
