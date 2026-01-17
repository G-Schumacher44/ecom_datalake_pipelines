from __future__ import annotations

import os

import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
from common import (
    AIRFLOW_HOME,
    COMMON_ENV,
    PIPELINE_ENV,
    SettingsConfig,
    make_runner_callable,
    resolve_bool,
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
        "enriched": config.resolve_path(
            pl.silver_bucket, pl.silver_enriched_prefix, "SILVER_ENRICHED_PATH"
        ),
        "project_id": pl.project_id,
        "env": p_env,
        "bucket": pl.silver_bucket,
    }


from src.runners.enriched import (
    run_cart_attribution,
    run_cart_attribution_summary,
    run_customer_lifetime_value,
    run_customer_retention,
    run_daily_business_metrics,
    run_inventory_risk,
    run_product_performance,
    run_regional_financials,
    run_sales_velocity,
    run_shipping_economics,
)

_run_cart_attribution = make_runner_callable(run_cart_attribution)
_run_cart_attribution_summary = make_runner_callable(run_cart_attribution_summary)
_run_inventory_risk = make_runner_callable(run_inventory_risk)
_run_customer_retention = make_runner_callable(run_customer_retention)
_run_sales_velocity = make_runner_callable(run_sales_velocity)
_run_product_performance = make_runner_callable(run_product_performance)
_run_regional_financials = make_runner_callable(run_regional_financials)
_run_customer_lifetime_value = make_runner_callable(run_customer_lifetime_value)
_run_daily_business_metrics = make_runner_callable(run_daily_business_metrics)
_run_shipping_economics = make_runner_callable(run_shipping_economics)


# --- DAG Definition ---

with DAG(
    dag_id="ecom_silver_to_gold_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={"retries": 1},
    tags=["ecom", "silver", "gold"],
) as dag:

    # 1. Setup Config Task (Push paths to XCom)
    setup_config = PythonOperator(
        task_id="setup_pipeline_config", python_callable=load_config_to_xcom
    )

    trigger_dim_refresh = TriggerDagRunOperator(
        task_id="trigger_dim_refresh",
        trigger_dag_id="ecom_dim_refresh_pipeline",
        execution_date="{{ data_interval_start }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # 2. Phase 0: Bronze Quality
    validate_bronze_quality = BashOperator(
        task_id="validate_bronze_quality",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && python -m src.validation.bronze_quality "
            f"--bronze-path {{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}}} "
            f"--output-report docs/validation_reports/BRONZE_QUALITY_{{{{ run_id | replace(':', '') }}}}.md "
            f"--run-id {{{{ run_id }}}} "
            + (" --fail-on-issues" if PIPELINE_ENV in {"dev", "prod"} else "")
        ),
    )

    # 3. Phase 1: Base Silver (dbt)
    base_silver_group = BashOperator(
        task_id="base_silver",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && "
            f"export BRONZE_BASE_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}}}\" "
            f"&& export SILVER_BASE_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}}}\" "
            f"&& export SILVER_ENRICHED_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['enriched'] }}}}\" "
            f"&& python -m src.runners.base_silver "
            f"--vars '{{\"run_date\": \"{{{{ ds }}}}\", \"lookback_days\": {os.getenv('BASE_SILVER_LOOKBACK_DAYS', '0')}}}' "
            "--select path:models/base_silver "
            "--exclude stg_ecommerce__customers "
            "stg_ecommerce__customers_quarantine "
            "stg_ecommerce__product_catalog "
            "stg_ecommerce__product_catalog_quarantine"
        ),
    )

    # 4. Phase 1.5: Silver Quality
    validate_silver_quality = BashOperator(
        task_id="validate_silver_quality",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && python -m src.validation.silver "
            f"--bronze-path {{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}}} "
            f"--silver-path {{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}}} "
            f"--quarantine-path {{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}}}/quarantine "
            f"--run-id {{{{ run_id }}}} "
            f"--output-report docs/validation_reports/SILVER_QUALITY_{{{{ run_id | replace(':', '') }}}}.md "
            + (" --enforce-quality" if PIPELINE_ENV == "prod" else "")
        ),
    )

    # 5. Sync Silver Base
    sync_silver_base = BashOperator(
        task_id="sync_silver_base_to_gcs",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && "
            f"SILVER_BASE_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}}}\" "
            f"BUCKET=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bucket'] }}}}\" "
            f"ENV=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['env'] }}}}\" "
            f'SILVER_GCS_TARGET="gs://$BUCKET/data/silver/base" '
            '&& if [[ "$ENV" =~ ^(dev|prod)$ ]] && [[ "$SILVER_BASE_PATH" != gs://* ]] && [[ "$BUCKET" != "local" ]]; then '
            'gcloud storage rsync -r --delete-unmatched-destination-objects "$SILVER_BASE_PATH" "$SILVER_GCS_TARGET"; '
            "else echo 'sync skipped'; fi"
        ),
    )

    # 6. Phase 2: Enriched Silver
    with TaskGroup("enriched_silver") as enriched_silver_group:
        PythonOperator(
            task_id="int_attributed_purchases",
            python_callable=_run_cart_attribution,
            op_kwargs={"ingest_dt": "{{ ds }}"},
        )
        PythonOperator(
            task_id="int_cart_attribution",
            python_callable=_run_cart_attribution_summary,
            op_kwargs={"ingest_dt": "{{ ds }}"},
        )
        PythonOperator(
            task_id="int_inventory_risk",
            python_callable=_run_inventory_risk,
            op_kwargs={"ingest_dt": "{{ ds }}"},
        )
        PythonOperator(
            task_id="int_product_performance",
            python_callable=_run_product_performance,
            op_kwargs={"ingest_dt": "{{ ds }}"},
        )
        PythonOperator(
            task_id="int_customer_retention_signals",
            python_callable=_run_customer_retention,
            op_kwargs={"ingest_dt": "{{ ds }}"},
        )
        PythonOperator(
            task_id="int_sales_velocity",
            python_callable=_run_sales_velocity,
            op_kwargs={"ingest_dt": "{{ ds }}"},
        )
        PythonOperator(
            task_id="int_regional_financials",
            python_callable=_run_regional_financials,
            op_kwargs={"ingest_dt": "{{ ds }}"},
        )
        PythonOperator(
            task_id="int_customer_lifetime_value",
            python_callable=_run_customer_lifetime_value,
            op_kwargs={"ingest_dt": "{{ ds }}"},
        )
        PythonOperator(
            task_id="int_daily_business_metrics",
            python_callable=_run_daily_business_metrics,
            op_kwargs={"ingest_dt": "{{ ds }}"},
        )
        PythonOperator(
            task_id="int_shipping_economics",
            python_callable=_run_shipping_economics,
            op_kwargs={"ingest_dt": "{{ ds }}"},
        )

    # 7. Validate Enriched
    validate_enriched_quality = BashOperator(
        task_id="validate_enriched_quality",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && python -m src.validation.enriched "
            f"--enriched-path {{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['enriched'] }}}} "
            f"--run-id {{{{ run_id }}}} "
            f"--ingest-dt {{{{ ds }}}} "
            f"--output-report docs/validation_reports/ENRICHED_QUALITY_{{{{ run_id | replace(':', '') }}}}.md "
            + (" --enforce-quality" if PIPELINE_ENV == "prod" else "")
        ),
    )

    # 8. Sync Enriched
    sync_silver_enriched = BashOperator(
        task_id="sync_silver_enriched_to_gcs",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && "
            f"SILVER_ENRICHED_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['enriched'] }}}}\" "
            f"BUCKET=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bucket'] }}}}\" "
            f"ENV=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['env'] }}}}\" "
            f'SILVER_ENRICHED_GCS_TARGET="gs://$BUCKET/data/silver/enriched" '
            '&& if [[ "$ENV" =~ ^(dev|prod)$ ]] && [[ "$SILVER_ENRICHED_PATH" != gs://* ]] && [[ "$BUCKET" != "local" ]]; then '
            'gcloud storage rsync -r --delete-unmatched-destination-objects "$SILVER_ENRICHED_PATH" "$SILVER_ENRICHED_GCS_TARGET"; '
            "else echo 'sync skipped'; fi"
        ),
    )

    # 9. Gates
    should_run_gold = ShortCircuitOperator(
        task_id="should_run_gold",
        python_callable=lambda: resolve_bool(
            "GOLD_PIPELINE_ENABLED", PIPELINE_ENV in {"dev", "prod"}
        ),
    )

    should_load_bigquery = ShortCircuitOperator(
        task_id="should_load_bigquery",
        python_callable=lambda: resolve_bool(
            "BQ_LOAD_ENABLED", PIPELINE_ENV in {"dev", "prod"}
        ),
    )

    # 10. Load to BQ
    with TaskGroup("load_to_bigquery") as load_bigquery_group:
        from src.runners.enriched.shared import get_enriched_partitions

        enriched_partitions = get_enriched_partitions()
        enriched_tables = list(enriched_partitions.keys())

        for table in enriched_tables:
            partition_key = enriched_partitions.get(table, "ingest_dt")
            BigQueryInsertJobOperator(
                task_id=f"load_{table}",
                location=os.getenv("BQ_LOCATION", "US"),
                configuration={
                    "load": {
                        "destinationTable": {
                            "projectId": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['project_id'] }}",
                            "datasetId": "silver",
                            "tableId": table,
                        },
                        "sourceUris": [
                            "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['enriched'] }}/"
                            + f"{table}/{partition_key}={{{{ ds }}}}/*.parquet"
                        ],
                        "sourceFormat": "PARQUET",
                        "writeDisposition": "WRITE_TRUNCATE",
                        "createDisposition": "CREATE_IF_NEEDED",
                    }
                },
            )

    # 11. Gold Marts
    gold_marts_build = BashOperator(
        task_id="gold_marts_build",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && dbt run --project-dir dbt_bigquery --profiles-dir dbt_bigquery "
            f"--select tag:gold"
        ),
    )

    # Flow
    (
        setup_config
        >> trigger_dim_refresh
        >> validate_bronze_quality
        >> base_silver_group
        >> validate_silver_quality
        >> sync_silver_base
        >> enriched_silver_group
        >> validate_enriched_quality
        >> sync_silver_enriched
        >> should_load_bigquery
        >> load_bigquery_group
        >> should_run_gold
        >> gold_marts_build
    )
