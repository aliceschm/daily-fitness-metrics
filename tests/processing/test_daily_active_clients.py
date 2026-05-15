from src.processing.daily_active_clients import build_daily_active_clients_df
from tests.conftest import collect_one


def test_build_daily_active_clients_df_counts_active_clients(spark):
    input_df = spark.createDataFrame(
        [
            {"client_id": "client_1", "status": "active"},
            {"client_id": "client_1", "status": "active"},
            {"client_id": "client_2", "status": "inactive"},
            {"client_id": "client_3", "status": "active"},
        ]
    )

    result = collect_one(build_daily_active_clients_df(input_df, "2026-05-13"))

    assert result == {
        "metric_date": "2026-05-13",
        "active_clients": 2,
    }
