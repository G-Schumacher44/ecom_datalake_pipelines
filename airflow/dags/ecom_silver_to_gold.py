from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from src.runners.enriched_silver import (
    run_cart_attribution,
    run_inventory_risk,
    run_customer_retention,
    run_sales_velocity,
    run_regional_financials,
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
    # Phase 0: Validation
    validate_bronze_schema = BashOperator(
        task_id="validate_bronze_schema",
        bash_command="python scripts/describe_parquet_samples.py --output docs/planning/BRONZE_SCHEMA_SAMPLE.md",
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
                task_id=f"stg_ecommerce__{table}",
                bash_command=f"dbt run --project-dir dbt_duckdb --profiles-dir dbt_duckdb --select stg_ecommerce__{table}",
            )

    # Phase 2: Enriched Silver (5 parallel Polars runner tasks)
    with TaskGroup("enriched_silver") as enriched_silver_group:
        BASE_PATH = "gs://acme-analytics-silver/ecom/base"
        ENRICHED_PATH = "gs://acme-analytics-silver/ecom/enriched"
        INGEST_DT = "2020-01-01"

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
        bash_command="dbt run --project-dir dbt_bigquery --profiles-dir dbt_bigquery --select tag:gold",
    )

    # Pipeline flow
    (
        validate_bronze_schema
        >> base_silver_group
        >> enriched_silver_group
        >> load_bigquery_group
        >> gold_marts_build
    )
