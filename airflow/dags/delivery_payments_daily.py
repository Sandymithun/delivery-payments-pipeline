"""
Daily delivery & payments ELT.

    generate_raw ─► land_in_storage ─► load_to_warehouse ─► dbt_source_freshness
        ─► dbt_build_staging (models + tests) ─► dbt_build_marts (models + tests)

* One DAG run = one business day (the run's data interval start, `ds`).
* catchup=True + idempotent tasks means `airflow dags backfill` just works.
* max_active_runs=1 keeps runs in order: fact_orders is incremental, and DuckDB
  only allows one writer at a time.
* dbt tests run inside `dbt build`, so a failing quality check stops the run
  before bad data reaches the marts.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, dag, task

PROJECT_DIR = os.getenv("PIPELINE_HOME", "/opt/pipeline")
DBT_DIR = f"{PROJECT_DIR}/dbt"
DBT_BIN = os.getenv("DBT_BIN", "/opt/airflow/dbt-venv/bin/dbt")
DBT = f"cd {DBT_DIR} && {DBT_BIN} --no-use-colors"
DBT_FLAGS = "--profiles-dir . --target ${DBT_TARGET:-duckdb}"

FACT_ORDERS = Asset("warehouse://analytics_marts/fact_orders")


@dag(
    dag_id="delivery_payments_daily",
    description="Generate -> S3/MinIO -> warehouse -> dbt star schema",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/Dublin"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["delivery", "payments", "dbt"],
    doc_md=__doc__,
)
def delivery_payments_daily():

    @task
    def generate_raw(ds: str | None = None) -> str:  # `ds` is injected from the run context
        """Stand-in for extracting from the source systems: write the day's CSVs."""
        from pathlib import Path

        from generator.generate_data import generate
        from ingestion.config import Settings

        n_orders = int(os.getenv("ORDERS_PER_DAY", "2000"))
        paths = generate(datetime.fromisoformat(ds).date(), Path(Settings().local_raw_dir), n_orders)
        print({k: str(v) for k, v in paths.items()})
        return ds

    @task
    def land_in_storage(run_date: str) -> list[str]:
        from ingestion.upload_to_storage import upload

        return upload(run_date)

    @task
    def load_to_warehouse(run_date: str, landed: list[str]) -> dict[str, int]:
        from ingestion.load_warehouse import load

        counts = load(run_date)
        print(f"loaded {counts} from {len(landed)} files")
        return counts

    freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT} source freshness {DBT_FLAGS}",
    )

    staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command=f"{DBT} build --select staging intermediate {DBT_FLAGS}",
    )

    marts = BashOperator(
        task_id="dbt_build_marts",
        bash_command=f"{DBT} build --select marts {DBT_FLAGS}",
        outlets=[FACT_ORDERS],
    )

    day = generate_raw()
    landed = land_in_storage(day)
    loaded = load_to_warehouse(day, landed)
    loaded >> freshness >> staging >> marts


delivery_payments_daily()
