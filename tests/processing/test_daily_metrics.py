from src.processing.daily_metrics import build_daily_metrics_df
from tests.conftest import collect_one


def test_build_daily_metrics_df_combines_daily_metric_inputs(spark):
    signups_df = spark.createDataFrame(
        [{"metric_date": "2026-05-13", "signup_count": 2}]
    )
    checkins_df = spark.createDataFrame(
        [{"metric_date": "2026-05-13", "checkin_count": 1}]
    )
    active_clients_df = spark.createDataFrame(
        [{"metric_date": "2026-05-13", "active_clients": 4}]
    )
    usage_rate_df = spark.createDataFrame(
        [{"metric_date": "2026-05-13", "usage_rate": 0.25}]
    )

    result = collect_one(
        build_daily_metrics_df(
            daily_signups_df=signups_df,
            daily_checkins_df=checkins_df,
            daily_active_clients_df=active_clients_df,
            daily_usage_rate_df=usage_rate_df,
        )
    )

    assert result == {
        "metric_date": "2026-05-13",
        "signup_count": 2,
        "checkin_count": 1,
        "active_clients": 4,
        "usage_rate": 0.25,
    }
