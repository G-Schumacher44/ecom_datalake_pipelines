from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="ecom_silver_to_gold_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1},
    tags=["ecom", "silver", "gold"],
) as dag:
    validate_bronze_schema = BashOperator(
        task_id="validate_bronze_schema",
        bash_command="python scripts/describe_parquet_samples.py --output docs/planning/BRONZE_SCHEMA_SAMPLE.md",
    )

    base_silver_transform = BashOperator(
        task_id="base_silver_transform",
        bash_command="dbt run --project-dir dbt_duckdb --profiles-dir dbt_duckdb",
    )

    enriched_silver_transform = BashOperator(
        task_id="enriched_silver_transform",
        bash_command="dbt run --project-dir dbt_bigquery --profiles-dir dbt_bigquery --select enriched_silver",
    )

    load_to_bigquery = BashOperator(
        task_id="load_to_bigquery",
        bash_command="echo 'Load enriched parquet to BigQuery via GCSToBigQueryOperator stub'",
    )

    gold_marts_build = BashOperator(
        task_id="gold_marts_build",
        bash_command="dbt run --project-dir dbt_bigquery --profiles-dir dbt_bigquery --select gold_marts",
    )

    (
        validate_bronze_schema
        >> base_silver_transform
        >> enriched_silver_transform
        >> load_to_bigquery
        >> gold_marts_build
    )
