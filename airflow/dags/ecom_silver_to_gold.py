from __future__ import annotations

import json
import os
import subprocess

import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
from common import (
    AIRFLOW_HOME,
    COMMON_ENV,
    PIPELINE_ENV,
    SettingsConfig,
    get_retry_config,
    get_dim_specs,
    get_dim_table_names,
    get_enriched_table_names,
    get_silver_base_table_names,
    make_runner_callable,
    resolve_dims_base_path,
    resolve_bool,
)

from airflow import DAG
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
from src.runners.mock_bq_load import mock_bigquery_load

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
        "bq_dataset": pl.bigquery_dataset,
        "env": p_env,
        "bucket": pl.silver_bucket,
    }


def _read_dims_latest_payload(path: str) -> dict | None:
    if path.startswith("gs://"):
        result = subprocess.run(
            ["gcloud", "storage", "cat", path],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            return None
        payload = result.stdout
    else:
        try:
            with open(path, "r", encoding="utf-8") as handle:
                payload = handle.read()
        except OSError:
            return None

    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return None

    return data if isinstance(data, dict) else None


def _extract_dims_snapshot_date(payload: dict) -> str | None:
    for key in ("run_date", "snapshot_dt", "as_of_dt", "ds"):
        value = payload.get(key)
        if value:
            return str(value)
    return None


def choose_dim_refresh_task(**context) -> str:
    latest_base = resolve_dims_base_path()
    if not latest_base:
        return "trigger_dim_refresh"

    latest_path = f"{latest_base.rstrip('/')}/_latest.json"
    payload = _read_dims_latest_payload(latest_path)
    latest_date = _extract_dims_snapshot_date(payload or {})
    if latest_date == context["ds"]:
        return "skip_dim_refresh"
    return "trigger_dim_refresh"


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
    default_args=get_retry_config(),
    tags=["ecom", "silver", "gold"],
) as dag:
    dim_specs = get_dim_specs()
    dim_table_names = set(get_dim_table_names())
    silver_base_tables = get_silver_base_table_names()
    silver_quality_tables = [
        table for table in silver_base_tables if table not in dim_table_names
    ]
    if not silver_quality_tables:
        silver_quality_tables = silver_base_tables
    silver_quality_csv = ",".join(silver_quality_tables)
    dim_excludes = []
    for dim in dim_specs:
        model = dim["dbt_model"]
        dim_excludes.extend([model, f"{model}_quarantine"])
    dim_exclude_args = " ".join(dim_excludes)
    dim_exclude_clause = f"--exclude {dim_exclude_args}" if dim_exclude_args else ""

    enriched_table_names = get_enriched_table_names()
    enriched_callable_map = {
        "int_attributed_purchases": _run_cart_attribution,
        "int_cart_attribution": _run_cart_attribution_summary,
        "int_inventory_risk": _run_inventory_risk,
        "int_product_performance": _run_product_performance,
        "int_customer_retention_signals": _run_customer_retention,
        "int_sales_velocity": _run_sales_velocity,
        "int_regional_financials": _run_regional_financials,
        "int_customer_lifetime_value": _run_customer_lifetime_value,
        "int_daily_business_metrics": _run_daily_business_metrics,
        "int_shipping_economics": _run_shipping_economics,
    }

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
    skip_dim_refresh = EmptyOperator(task_id="skip_dim_refresh")
    dims_refresh_done = EmptyOperator(
        task_id="dims_refresh_done",
        trigger_rule="none_failed_min_one_success",
    )
    dims_freshness_gate = BranchPythonOperator(
        task_id="dims_freshness_gate",
        python_callable=choose_dim_refresh_task,
    )

    # 2. Phase 0: Bronze Quality
    validate_bronze_quality = BashOperator(
        task_id="validate_bronze_quality",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && python -m src.validation.bronze_quality "
            f"--bronze-path {{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}}} "
            f"--partition-date {{{{ ds }}}} "
            f"--lookback-days {os.getenv('BRONZE_VALIDATION_LOOKBACK_DAYS', '0')} "
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
            f"&& DIMS_PATH=\"${{SILVER_DIMS_PATH:-data/silver/dims}}\" "
            f"&& if [[ \"$DIMS_PATH\" == gs://* ]]; then "
            f"DIMS_PATH=\"${{SILVER_DIMS_LOCAL_PATH:-{AIRFLOW_HOME}/data/silver/dims}}\"; fi "
            f"&& export SILVER_DIMS_PATH=\"$DIMS_PATH\" "
            f"&& export DBT_DUCKDB_PATH=\"/tmp/dbt_duckdb/ecom_base_silver_{{{{ run_id | replace(':', '') }}}}.duckdb\" "
            f"&& python -m src.runners.base_silver "
            f"--vars '{{\"run_date\": \"{{{{ ds }}}}\", \"lookback_days\": {os.getenv('BASE_SILVER_LOOKBACK_DAYS', '0')}}}' "
            "--select path:models/base_silver "
            f"{dim_exclude_clause}"
        ),
    )

    # 4. Phase 1.5: Silver Quality
    validate_silver_quality = BashOperator(
        task_id="validate_silver_quality",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && "
            f"BRONZE_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}}}\" "
            f"SILVER_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}}}\" "
            f"&& python -m src.validation.silver "
            f'--bronze-path "$BRONZE_PATH" '
            f'--silver-path "$SILVER_PATH" '
            f'--quarantine-path "$SILVER_PATH/quarantine" '
            f"--partition-date {{{{ ds }}}} "
            f"--lookback-days {os.getenv('SILVER_VALIDATION_LOOKBACK_DAYS', '0')} "
            f"--run-id {{{{ run_id }}}} "
            f"--tables {silver_quality_csv} "
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
            'gcloud storage rsync -r "$SILVER_BASE_PATH" "$SILVER_GCS_TARGET"; '
            "else echo 'sync skipped'; fi"
        ),
    )

    # 6. Phase 2: Enriched Silver
    with TaskGroup("enriched_silver") as enriched_silver_group:
        for table in enriched_table_names:
            runner = enriched_callable_map.get(table)
            if not runner:
                continue
            PythonOperator(
                task_id=table,
                python_callable=runner,
                op_kwargs={"ingest_dt": "{{ ds }}"},
            )

    # 7. Validate Enriched
    validate_enriched_quality = BashOperator(
        task_id="validate_enriched_quality",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && "
            f"ENRICHED_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['enriched'] }}}}\" "
            "&& python -m src.validation.enriched "
            f'--enriched-path "$ENRICHED_PATH" '
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
            f'SILVER_ENRICHED_GCS_TARGET="$SILVER_ENRICHED_PATH" '
            f'SILVER_ENRICHED_LOCAL_PATH="${{SILVER_ENRICHED_LOCAL_PATH:-/opt/airflow/data/silver/enriched}}" '
            '&& if [[ "$ENV" =~ ^(dev|prod)$ ]] && [[ "$SILVER_ENRICHED_PATH" == gs://* ]] && [[ "$BUCKET" != "local" ]]; then '
            'gcloud storage rsync -r "$SILVER_ENRICHED_LOCAL_PATH" "$SILVER_ENRICHED_GCS_TARGET"; '
            "else echo 'sync skipped (ENV=$ENV, PATH=$SILVER_ENRICHED_PATH)'; fi"
        ),
    )

    # 9. Validate Enriched Parquet Files (Mock BQ Load)
    with TaskGroup("validate_enriched_parquet") as validate_parquet_group:
        from src.runners.enriched.shared import get_enriched_partitions

        enriched_partitions = get_enriched_partitions()

        for table in enriched_partitions.keys():
            partition_key = enriched_partitions.get(table, "ingest_dt")
            PythonOperator(
                task_id=f"validate_{table}",
                python_callable=mock_bigquery_load,
                op_kwargs={
                    "enriched_path": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['enriched'] }}",
                    "table": table,
                    "partition_key": partition_key,
                    "partition_value": "{{ ds }}",
                    "project_id": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['project_id'] }}",
                    "dataset": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bq_dataset'] }}",
                },
            )

    # 10. Gates
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

    # 11. Load to BQ
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
                            "datasetId": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bq_dataset'] }}",
                            "tableId": table,
                        },
                        "sourceUris": [
                            "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['enriched'] }}/"
                            + f"{table}/{partition_key}={{{{ ds }}}}/*.parquet"
                        ],
                        "sourceFormat": "PARQUET",
                        "writeDisposition": "WRITE_APPEND",
                        "createDisposition": "CREATE_IF_NEEDED",
                        "timePartitioning": {
                            "type": "DAY",
                            "field": partition_key,
                        },
                    }
                },
            )

    # 12. Gold Marts
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
        >> dims_freshness_gate
        >> [trigger_dim_refresh, skip_dim_refresh]
        >> dims_refresh_done
        >> validate_bronze_quality
        >> base_silver_group
        >> validate_silver_quality
        >> sync_silver_base
        >> enriched_silver_group
        >> validate_enriched_quality
        >> sync_silver_enriched
        >> validate_parquet_group
        >> should_load_bigquery
        >> load_bigquery_group
        >> should_run_gold
        >> gold_marts_build
    )
