from src.processing.daily_usage_rate import build_daily_usage_rate_df
from tests.conftest import collect_one


def test_build_daily_usage_rate_df_divides_checkins_by_active_clients(spark):
    checkins_df = spark.createDataFrame(
        [{"metric_date": "2026-05-13", "checkin_count": 3}]
    )
    active_clients_df = spark.createDataFrame(
        [{"metric_date": "2026-05-13", "active_clients": 6}]
    )

    result = collect_one(build_daily_usage_rate_df(checkins_df, active_clients_df))

    assert result["metric_date"] == "2026-05-13"
    assert result["usage_rate"] == 0.5


def test_build_daily_usage_rate_df_returns_zero_when_no_active_clients(spark):
    checkins_df = spark.createDataFrame(
        [{"metric_date": "2026-05-13", "checkin_count": 3}]
    )
    active_clients_df = spark.createDataFrame(
        [{"metric_date": "2026-05-13", "active_clients": 0}]
    )

    result = collect_one(build_daily_usage_rate_df(checkins_df, active_clients_df))

    assert result["metric_date"] == "2026-05-13"
    assert result["usage_rate"] == 0.0
