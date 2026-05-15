# Daily Fitness Metrics

Data engineering project focused on processing daily fitness platform activity into analytical metrics.

The pipeline simulates operational data from a fitness marketplace (like Wellhub or TotalPass), generating daily metrics from user subscription and check-in events.

## Stack

- Python
- PySpark
- Parquet

Planned:

- Airflow
- PostgreSQL
- Docker Compose

## Pipeline

```text
subscriptions/checkins
        ↓
Spark transformations
        ↓
staging metrics
        ↓
daily_metrics
```

## Current Metrics

- daily signups
- daily checkins
- active clients
- usage rate

## Run Locally

Generate fake data:

```bash
python -m scripts.generate_fake_events
```

Run pipeline:

```bash
python -m scripts.run_daily_pipeline \
  --metric-date 2026-05-13
```

## Learning Goals

- Spark DataFrames
- Parquet datasets
- aggregations
- orchestration concepts
- idempotent pipelines
- analytical modeling

## Roadmap

- Airflow orchestration
- PostgreSQL warehouse
- incremental processing
- observability

