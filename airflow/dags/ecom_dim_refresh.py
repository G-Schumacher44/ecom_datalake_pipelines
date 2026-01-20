from __future__ import annotations

import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from common import (
    AIRFLOW_HOME,
    COMMON_ENV,
    PIPELINE_ENV,
    SettingsConfig,
    get_retry_config,
    get_dim_specs,
    get_dim_table_names,
)

from airflow import DAG

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
                    f"&& export BRONZE_SYNC_TABLES=\"{table}\" "
                    f"&& export SILVER_BASE_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}}}\" "
                    f"&& export DBT_DUCKDB_PATH=\"/tmp/dbt_duckdb/ecom_{table}_{{{{ run_id | replace(':', '') }}}}.duckdb\" "
                    f"&& python -m src.runners.base_silver "
                    f"--select {model} {model}_quarantine"
                ),
            )

    # 5. Validate Dim Quality
    validate_dim_quality = BashOperator(
        task_id="validate_dim_quality",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && "
            f"BRONZE_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}}}\" "
            f"SILVER_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}}}\" "
            f'&& if [[ "$BRONZE_PATH" == gs://* ]]; then '
            f'BRONZE_PATH="${{BRONZE_LOCAL_BASE_PATH:-{AIRFLOW_HOME}/data/bronze}}"; fi '
            f'&& if [[ "$SILVER_PATH" == gs://* ]]; then '
            f'SILVER_PATH="${{SILVER_LOCAL_BASE_PATH:-{AIRFLOW_HOME}/data/silver/base}}"; fi '
            f"&& python -m src.validation.silver "
            f'--bronze-path "$BRONZE_PATH" '
            f'--silver-path "$SILVER_PATH" '
            f'--quarantine-path "$SILVER_PATH/quarantine" '
            f"{dim_tables_arg}"
            f"--run-id {{{{ run_id }}}} "
            f"--output-report docs/validation_reports/SILVER_DIMS_{{{{ run_id | replace(':', '') }}}}.md "
            + (" --enforce-quality" if PIPELINE_ENV == "prod" else "")
        ),
    )

    # Flow
    (
        setup_config
        >> validate_bronze_dims
        >> refresh_dims_group
        >> validate_dim_quality
    )
