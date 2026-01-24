from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone

import pendulum

from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from common import (
    AIRFLOW_HOME,
    COMMON_ENV,
    PIPELINE_ENV,
    SettingsConfig,
    get_dim_specs,
    get_dim_table_names,
    get_retry_config,
    resolve_dims_base_path,
)
from src.runners.dims_snapshot import snapshot_dims

# --- Task Callables ---


def load_config_to_xcom(**kwargs):
    """Loads configuration and pushes paths to XCom."""
    config = SettingsConfig()
    pl = config.pipeline
    p_env = config.resolve_pipeline_env()

    return {
        "bronze": config.resolve_path(
            pl.bronze_bucket, pl.bronze_prefix, "BRONZE_BASE_PATH"
        ),
        "silver": config.resolve_path(
            pl.silver_bucket, pl.silver_base_prefix, "SILVER_BASE_PATH"
        ),
        "env": p_env,
    }


def publish_dims_latest(**context) -> None:
    """Persist dims freshness pointer for the current run_date."""
    latest_base = resolve_dims_base_path()
    if not latest_base:
        return

    payload = {
        "run_date": context["ds"],
        "run_id": context["run_id"],
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    latest_path = f"{latest_base.rstrip('/')}/_latest.json"

    if latest_base.startswith("gs://"):
        tmp_file = "/tmp/dims_latest.json"
        with open(tmp_file, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        subprocess.run(
            ["gcloud", "storage", "cp", tmp_file, latest_path],
            check=True,
        )
        return

    os.makedirs(latest_base, exist_ok=True)
    with open(latest_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


# --- DAG Definition ---

with DAG(
    dag_id="ecom_dim_refresh_pipeline",
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=get_retry_config(),
    tags=["ecom", "silver", "dims"],
) as dag:
    dim_specs = get_dim_specs()
    dim_tables = get_dim_table_names()
    dim_table_csv = ",".join(dim_tables)
    dim_tables_arg = f"--tables {dim_table_csv} " if dim_table_csv else ""

    # 1. Setup Config
    setup_config = PythonOperator(
        task_id="setup_pipeline_config", python_callable=load_config_to_xcom
    )

    # 2. Validate Bronze Dims
    validate_bronze_dims = BashOperator(
        task_id="validate_bronze_dims",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && python -m src.validation.bronze_quality "
            f"--bronze-path {{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}}} "
            f"{dim_tables_arg}"
            f"--partition-date {{{{ ds }}}} "
            f"--lookback-days {os.getenv('BRONZE_VALIDATION_LOOKBACK_DAYS', '0')} "
            f"--output-report docs/validation_reports/BRONZE_DIMS_{{{{ run_id | replace(':', '') }}}}.md "
            f"--run-id {{{{ run_id }}}} "
            + (" --enforce-quality" if PIPELINE_ENV in {"dev", "prod"} else "")
        ),
    )

    # 3. Refresh Dims
    # DBT_DUCKDB_PATH unique per task to avoid lock contention
    with TaskGroup("refresh_dims") as refresh_dims_group:
        for dim in dim_specs:
            table = dim["name"]
            model = dim["dbt_model"]
            BashOperator(
                task_id=f"refresh_{table}",
                env=COMMON_ENV,
                bash_command=(
                    f"cd {AIRFLOW_HOME} && "
                    f"export BRONZE_BASE_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}}}\" "
                    f'&& export BRONZE_SYNC_TABLES="{table}" '
                    f"&& export SILVER_BASE_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}}}\" "
                    f"&& export DBT_DUCKDB_PATH=\"/tmp/dbt_duckdb/ecom_{table}_{{{{ run_id | replace(':', '') }}}}.duckdb\" "
                    f"&& python -m src.runners.base_silver "
                    f"--select {model} {model}_quarantine"
                ),
            )

    validate_dims_snapshot = BashOperator(
        task_id="validate_dims_snapshot",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && python -m src.validation.dims_snapshot "
            f"--run-date {{{{ ds }}}} "
            f"--output-report docs/validation_reports/DIMS_SNAPSHOT_{{{{ run_id | replace(':', '') }}}}.md "
            + (" --enforce-quality" if PIPELINE_ENV == "prod" else "")
        ),
    )
    snapshot_dims_task = PythonOperator(
        task_id="snapshot_dims",
        python_callable=snapshot_dims,
        op_kwargs={
            "run_date": "{{ ds }}",
            "silver_base_path": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}",
        },
    )
    publish_dims_latest_task = PythonOperator(
        task_id="publish_dims_latest",
        python_callable=publish_dims_latest,
    )

    # Flow
    (
        setup_config
        >> validate_bronze_dims
        >> refresh_dims_group
        >> snapshot_dims_task
        >> validate_dims_snapshot
        >> publish_dims_latest_task
    )
