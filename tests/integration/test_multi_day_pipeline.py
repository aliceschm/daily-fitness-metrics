from scripts.run_daily_pipeline import run_pipeline
from tests.integration.test_daily_pipeline import (
    read_daily_metric,
    write_clients_csv,
    write_events,
)


def test_multi_day_pipeline_generates_metrics_for_each_date(spark, tmp_path):
    sources_path = tmp_path / "sources"
    staging_path = tmp_path / "staging_metrics"
    warehouse_path = tmp_path / "warehouse"

    write_clients_csv(sources_path / "clients" / "clients.csv")
    write_events(
        spark=spark,
        sources_path=sources_path,
        metric_date="2026-05-13",
        signups=["client_1", "client_2"],
        checkins=["client_1"],
    )
    write_events(
        spark=spark,
        sources_path=sources_path,
        metric_date="2026-05-14",
        signups=["client_3"],
        checkins=["client_1", "client_2", "client_2"],
    )

    run_pipeline(
        spark=spark,
        metric_dates=["2026-05-13", "2026-05-14"],
        sources_base_path=str(sources_path),
        staging_base_path=str(staging_path),
        warehouse_base_path=str(warehouse_path),
        show_output=False,
    )

    first_day = read_daily_metric(spark, warehouse_path, "2026-05-13")
    second_day = read_daily_metric(spark, warehouse_path, "2026-05-14")

    assert first_day["signup_count"] == 2
    assert first_day["usage_rate"] == 0.5
    assert second_day["signup_count"] == 1
    assert second_day["usage_rate"] == 1.0
