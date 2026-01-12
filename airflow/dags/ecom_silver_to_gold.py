from __future__ import annotations

import os
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from src.runners.enriched_silver import (
    run_cart_attribution,
    run_customer_retention,
    run_inventory_risk,
    run_regional_financials,
    run_sales_velocity,
)
from src.settings import load_settings


def _resolve_pipeline_env(config_env: str | None) -> str:
    env_override = os.getenv("PIPELINE_ENV")
    return (env_override or config_env or "local").lower()


def _resolve_bool(env_key: str, default: bool = False) -> bool:
    env_override = os.getenv(env_key)
    if env_override is None:
        return default
    return env_override.lower() in {"true", "1", "yes"}


def _resolve_path(
    bucket: str,
    prefix: str,
    env_key: str | None = None,
    pipeline_env: str = "local",
) -> str:
    if env_key:
        override = os.getenv(env_key)
        if override:
            return override
    if bucket == "local" or pipeline_env == "local":
        return prefix
    return f"gs://{bucket}/{prefix}"


def _is_gcs_path(path: str) -> bool:
    return path.startswith("gs://")


settings = load_settings()
pipeline = settings.pipeline
PIPELINE_ENV = _resolve_pipeline_env(pipeline.environment)
BRONZE_PATH = _resolve_path(
    pipeline.bronze_bucket,
    pipeline.bronze_prefix,
    "BRONZE_BASE_PATH",
    PIPELINE_ENV,
)
SILVER_BASE_PATH = _resolve_path(
    pipeline.silver_bucket,
    pipeline.silver_base_prefix,
    "SILVER_BASE_PATH",
    PIPELINE_ENV,
)
SILVER_ENRICHED_PATH = _resolve_path(
    pipeline.silver_bucket,
    pipeline.silver_enriched_prefix,
    "SILVER_ENRICHED_PATH",
    PIPELINE_ENV,
)
QUARANTINE_PATH = f"{SILVER_BASE_PATH.rstrip('/')}/quarantine"
SILVER_GCS_TARGET = os.getenv(
    "SILVER_GCS_TARGET",
    f"gs://{pipeline.silver_bucket}/{pipeline.silver_base_prefix}",
)
DEFAULT_INGEST_DT = pipeline.default_ingest_dt
GOLD_PIPELINE_ENABLED = _resolve_bool(
    "GOLD_PIPELINE_ENABLED",
    default=PIPELINE_ENV in {"dev", "prod"},
)

COMMON_ENV = {
    "PIPELINE_ENV": PIPELINE_ENV,
    "BRONZE_BASE_PATH": BRONZE_PATH,
    "SILVER_BASE_PATH": SILVER_BASE_PATH,
    "SILVER_QUARANTINE_PATH": QUARANTINE_PATH,
    "SILVER_ENRICHED_PATH": SILVER_ENRICHED_PATH,
}

SHOULD_SYNC_SILVER_BASE = (
    PIPELINE_ENV in {"dev", "prod"}
    and not _is_gcs_path(SILVER_BASE_PATH)
    and pipeline.silver_bucket != "local"
)


def load_table_to_bigquery(table_name: str, gcs_path: str, dataset: str = "silver"):
    """Load parquet from GCS to BigQuery native table."""
    import subprocess

    result = subprocess.run(
        [
            "bq",
            "load",
            "--source_format=PARQUET",
            "--replace",
            f"{dataset}.{table_name}",
            f"{gcs_path}/*.parquet",
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"bq load failed: {result.stderr}")

    return {"table": table_name, "status": "loaded", "stdout": result.stdout}


with DAG(
    dag_id="ecom_silver_to_gold_pipeline",
    start_date=datetime(2026, 1, 10),
    schedule=None,
    catchup=False,
    default_args={"retries": 1},
    tags=["ecom", "silver", "gold"],
) as dag:
    # Phase 0: Bronze Quality Validation
    validate_bronze_quality = BashOperator(
        task_id="validate_bronze_quality",
        env=COMMON_ENV,
        bash_command=(
            "python src/validation/bronze_quality.py "
            f"--bronze-path {BRONZE_PATH} "
            "--output-report docs/validation_reports/BRONZE_QUALITY.md "
            "--run-id {{ run_id }} "
            + ("--fail-on-issues " if PIPELINE_ENV in {"dev", "prod"} else "")
        ),
    )

    # Phase 1: Base Silver (8 parallel dbt-duckdb tasks)
    with TaskGroup("base_silver") as base_silver_group:
        bronze_tables = [
            "orders",
            "order_items",
            "customers",
            "product_catalog",
            "shopping_carts",
            "cart_items",
            "returns",
            "return_items",
        ]

        for table in bronze_tables:
            BashOperator(
                task_id=f"stg_ecommerce_{table}",
                env=COMMON_ENV,
                bash_command=(
                    "dbt run --project-dir dbt_duckdb --profiles-dir dbt_duckdb "
                    f"--select stg_ecommerce__{table}"
                ),
            )

    # Phase 1.5: Silver Quality Validation
    validate_silver_quality = BashOperator(
        task_id="validate_silver_quality",
        env=COMMON_ENV,
        bash_command=(
            "python src/validation/silver_quality.py "
            f"--bronze-path {BRONZE_PATH} "
            f"--silver-path {SILVER_BASE_PATH} "
            f"--quarantine-path {QUARANTINE_PATH} "
            "--run-id {{ run_id }} "
            + ("--fail-on-sla-breach " if PIPELINE_ENV == "prod" else "")
            # Optional: Add --fail-on-sla-breach to stop pipeline on quality issues
        ),
    )

    if SHOULD_SYNC_SILVER_BASE:
        sync_silver_base = BashOperator(
            task_id="sync_silver_base_to_gcs",
            env=COMMON_ENV,
            bash_command=(f"gsutil -m rsync -r {SILVER_BASE_PATH} {SILVER_GCS_TARGET}"),
        )
    else:
        sync_silver_base = EmptyOperator(task_id="sync_silver_base_to_gcs")

    # Phase 2: Enriched Silver (5 parallel Polars runner tasks)
    with TaskGroup("enriched_silver") as enriched_silver_group:
        BASE_PATH = SILVER_BASE_PATH
        ENRICHED_PATH = SILVER_ENRICHED_PATH
        INGEST_DT = DEFAULT_INGEST_DT

        cart_attr = PythonOperator(
            task_id="int_attributed_purchases",
            python_callable=run_cart_attribution,
            op_kwargs={
                "base_silver_path": BASE_PATH,
                "output_path": ENRICHED_PATH,
                "ingest_dt": INGEST_DT,
            },
        )

        inv_risk = PythonOperator(
            task_id="int_inventory_risk",
            python_callable=run_inventory_risk,
            op_kwargs={
                "base_silver_path": BASE_PATH,
                "output_path": ENRICHED_PATH,
                "ingest_dt": INGEST_DT,
            },
        )

        cust_ret = PythonOperator(
            task_id="int_customer_retention_signals",
            python_callable=run_customer_retention,
            op_kwargs={
                "base_silver_path": BASE_PATH,
                "output_path": ENRICHED_PATH,
                "ingest_dt": INGEST_DT,
            },
        )

        sales_vel = PythonOperator(
            task_id="int_sales_velocity",
            python_callable=run_sales_velocity,
            op_kwargs={
                "base_silver_path": BASE_PATH,
                "output_path": ENRICHED_PATH,
                "ingest_dt": INGEST_DT,
            },
        )

        regional_fin = PythonOperator(
            task_id="int_regional_financials",
            python_callable=run_regional_financials,
            op_kwargs={
                "base_silver_path": BASE_PATH,
                "output_path": ENRICHED_PATH,
                "ingest_dt": INGEST_DT,
            },
        )

    if GOLD_PIPELINE_ENABLED:
        # Phase 3: Load Enriched Silver to BigQuery
        with TaskGroup("load_to_bigquery") as load_bigquery_group:
            enriched_tables = [
                "int_attributed_purchases",
                "int_inventory_risk",
                "int_customer_retention_signals",
                "int_sales_velocity",
                "int_regional_financials",
            ]

            for table in enriched_tables:
                PythonOperator(
                    task_id=f"load_{table}",
                    python_callable=load_table_to_bigquery,
                    op_kwargs={
                        "table_name": table,
                        "gcs_path": f"{ENRICHED_PATH}/{table}/ingest_dt={INGEST_DT}",
                        "dataset": "silver",
                    },
                )

        # Phase 4: Gold Marts (dbt-bigquery SQL models)
        gold_marts_build = BashOperator(
            task_id="gold_marts_build",
            bash_command=(
                "dbt run --project-dir dbt_bigquery --profiles-dir dbt_bigquery "
                "--select tag:gold"
            ),
        )
    else:
        load_bigquery_group = EmptyOperator(task_id="load_to_bigquery")
        gold_marts_build = EmptyOperator(task_id="gold_marts_build")

    # Pipeline flow
    (
        validate_bronze_quality
        >> base_silver_group
        >> validate_silver_quality
        >> sync_silver_base
        >> enriched_silver_group
        >> load_bigquery_group
        >> gold_marts_build
    )
