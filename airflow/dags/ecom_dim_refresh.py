from __future__ import annotations

import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from common import (
    AIRFLOW_HOME,
    COMMON_ENV,
    PIPELINE_ENV,
    SettingsConfig,
    get_retry_config,
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

    # 1. Setup Config
    setup_config = PythonOperator(
        task_id="setup_pipeline_config",
        python_callable=load_config_to_xcom
    )

    # 2. Validate Bronze Dims
    validate_bronze_dims = BashOperator(
        task_id="validate_bronze_dims",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && python -m src.validation.bronze_quality "
            f"--bronze-path {{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}}} "
            f"--tables customers,product_catalog "
            f"--output-report docs/validation_reports/BRONZE_DIMS_{{{{ run_id | replace(':', '') }}}}.md "
            f"--run-id {{{{ run_id }}}} "
            + (
                " --enforce-quality"
                if PIPELINE_ENV in {"dev", "prod"}
                else ""
            )
        ),
    )

    # 3. Refresh Customers
    # Samples baked into image; GCS uses httpfs (no locking issues)
    # DBT_DUCKDB_PATH unique per task to avoid lock contention
    refresh_customers = BashOperator(
        task_id="refresh_customers",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && "
            f"export BRONZE_BASE_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}}}\" "
            f"&& export SILVER_BASE_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}}}\" "
            f"&& python -m src.runners.base_silver "
            "--select stg_ecommerce__customers stg_ecommerce__customers_quarantine"
        ),
    )

    # 4. Refresh Product Catalog
    refresh_product_catalog = BashOperator(
        task_id="refresh_product_catalog",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && "
            f"export BRONZE_BASE_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}}}\" "
            f"&& export SILVER_BASE_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}}}\" "
            f"&& python -m src.runners.base_silver "
            "--select stg_ecommerce__product_catalog stg_ecommerce__product_catalog_quarantine"
        ),
    )

    # 5. Validate Dim Quality
    validate_dim_quality = BashOperator(
        task_id="validate_dim_quality",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && python -m src.validation.silver "
            f"--bronze-path {{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}}} "
            f"--silver-path {{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}}} "
            f"--quarantine-path {{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}}}/quarantine "
            f"--tables customers,product_catalog "
            f"--run-id {{{{ run_id }}}} "
            f"--output-report docs/validation_reports/SILVER_DIMS_{{{{ run_id | replace(':', '') }}}}.md "
            + (
                " --enforce-quality"
                if PIPELINE_ENV == "prod"
                else ""
            )
        ),
    )

    # Flow
    setup_config >> validate_bronze_dims >> refresh_customers >> refresh_product_catalog >> validate_dim_quality