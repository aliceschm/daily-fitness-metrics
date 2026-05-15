from scripts.run_daily_pipeline import run_pipeline


def write_clients_csv(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "client_id,status,location_id,created_at\n"
        "client_1,active,SP,2026-05-01\n"
        "client_2,active,RJ,2026-05-01\n"
        "client_3,inactive,SP,2026-05-01\n",
        encoding="utf-8",
    )


def write_events(spark, sources_path, metric_date, signups, checkins):
    signup_rows = [
        {
            "signup_id": f"signup_{index}",
            "client_id": client_id,
            "signup_date": metric_date,
        }
        for index, client_id in enumerate(signups)
    ]
    checkin_rows = [
        {
            "checkin_id": f"checkin_{index}",
            "client_id": client_id,
            "checkin_date": metric_date,
        }
        for index, client_id in enumerate(checkins)
    ]

    spark.createDataFrame(signup_rows).write.mode("overwrite").parquet(
        str(
            sources_path
            / "subscriptions"
            / f"date={metric_date}"
            / "client_signups.parquet"
        )
    )
    spark.createDataFrame(checkin_rows).write.mode("overwrite").parquet(
        str(sources_path / "checkins" / f"date={metric_date}" / "checkins.parquet")
    )


def read_daily_metric(spark, warehouse_path, metric_date):
    rows = spark.read.parquet(
        str(warehouse_path / "daily_metrics" / f"date={metric_date}")
    ).collect()
    assert len(rows) == 1
    return rows[0].asDict()


def test_daily_pipeline_generates_metrics_for_one_date(spark, tmp_path):
    sources_path = tmp_path / "sources"
    staging_path = tmp_path / "staging_metrics"
    warehouse_path = tmp_path / "warehouse"
    metric_date = "2026-05-13"

    write_clients_csv(sources_path / "clients" / "clients.csv")
    write_events(
        spark=spark,
        sources_path=sources_path,
        metric_date=metric_date,
        signups=["client_1", "client_1", "client_2"],
        checkins=["client_1"],
    )

    run_pipeline(
        spark=spark,
        metric_dates=[metric_date],
        sources_base_path=str(sources_path),
        staging_base_path=str(staging_path),
        warehouse_base_path=str(warehouse_path),
        show_output=False,
    )

    result = read_daily_metric(spark, warehouse_path, metric_date)

    assert result["metric_date"] == metric_date
    assert result["signup_count"] == 2
    assert result["checkin_count"] == 1
    assert result["active_clients"] == 2
    assert result["usage_rate"] == 0.5
