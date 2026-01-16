from __future__ import annotations

import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
import pendulum

from src.settings import load_settings

# --- Configuration Helper (Lazy Loading) ---


class SettingsConfig:
    """Lazy configuration loader for Airflow DAGs."""

    def __init__(self):
        self._settings = None
        self._airflow_home = os.getenv("AIRFLOW_HOME", "/opt/airflow")
        self._config_path = os.getenv(
            "ECOM_CONFIG_PATH", f"{self._airflow_home}/config/config.yml"
        )

    @property
    def settings(self):
        if self._settings is None:
            self._settings = load_settings(self._config_path)
        return self._settings

    @property
    def pipeline(self):
        return self.settings.pipeline

    @property
    def airflow_home(self):
        return self._airflow_home

    def resolve_pipeline_env(self) -> str:
        env_override = os.getenv("PIPELINE_ENV")
        return (env_override or self.pipeline.environment or "local").lower()

    def resolve_path(self, bucket: str, prefix: str, env_key: str | None = None) -> str:
        pipeline_env = self.resolve_pipeline_env()
        if env_key:
            override = os.getenv(env_key)
            if override:
                return (
                    self._resolve_local_path(override)
                    if bucket == "local" or pipeline_env == "local"
                    else override
                )
        if bucket == "local" or pipeline_env == "local":
            return self._resolve_local_path(prefix)
        return f"gs://{bucket}/{prefix}"

    def _resolve_local_path(self, path: str) -> str:
        if path.startswith("gs://") or os.path.isabs(path):
            return path
        return os.path.join(self.airflow_home, path)


# --- Top-Level Constants (Lightweight Only) ---

PIPELINE_ENV = os.getenv("PIPELINE_ENV", "local").lower()
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")

COMMON_ENV = {
    "PIPELINE_ENV": PIPELINE_ENV,
    "OBSERVABILITY_ENV": os.getenv("OBSERVABILITY_ENV", ""),
    "PYTHONPATH": os.getenv("PYTHONPATH", AIRFLOW_HOME),
    "PATH": f"{os.getenv('PATH', '')}:/home/airflow/.local/bin",
    "HOME": os.getenv("HOME", "/home/airflow"),
}

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


def _resolve_bool(env_key: str, default: bool = False) -> bool:
    env_override = os.getenv(env_key)
    if env_override is None:
        return default
    return env_override.lower() in {"true", "1", "yes"}


def _run_enriched_runner(func, **kwargs):
    # Resolve paths at runtime via helper (avoids top-level load)
    config = SettingsConfig()
    pl = config.pipeline

    kwargs["base_silver_path"] = config.resolve_path(
        pl.silver_bucket, pl.silver_base_prefix, "SILVER_BASE_PATH"
    )
    kwargs["output_path"] = config.resolve_path(
        pl.silver_bucket, pl.silver_enriched_prefix, "SILVER_ENRICHED_PATH"
    )

    allowed_keys = {"base_silver_path", "output_path", "ingest_dt"}
    filtered = {key: value for key, value in kwargs.items() if key in allowed_keys}
    return func(**filtered)


def _run_cart_attribution(**kwargs):
    from src.runners.enriched import run_cart_attribution

    return _run_enriched_runner(run_cart_attribution, **kwargs)


def _run_cart_attribution_summary(**kwargs):
    from src.runners.enriched import run_cart_attribution_summary

    return _run_enriched_runner(run_cart_attribution_summary, **kwargs)


def _run_inventory_risk(**kwargs):
    from src.runners.enriched import run_inventory_risk

    return _run_enriched_runner(run_inventory_risk, **kwargs)


def _run_customer_retention(**kwargs):
    from src.runners.enriched import run_customer_retention

    return _run_enriched_runner(run_customer_retention, **kwargs)


def _run_sales_velocity(**kwargs):
    from src.runners.enriched import run_sales_velocity

    return _run_enriched_runner(run_sales_velocity, **kwargs)


def _run_product_performance(**kwargs):
    from src.runners.enriched import run_product_performance

    return _run_enriched_runner(run_product_performance, **kwargs)


def _run_regional_financials(**kwargs):
    from src.runners.enriched import run_regional_financials

    return _run_enriched_runner(run_regional_financials, **kwargs)


def _run_customer_lifetime_value(**kwargs):
    from src.runners.enriched import run_customer_lifetime_value

    return _run_enriched_runner(run_customer_lifetime_value, **kwargs)


def _run_daily_business_metrics(**kwargs):
    from src.runners.enriched import run_daily_business_metrics

    return _run_enriched_runner(run_daily_business_metrics, **kwargs)


def _run_shipping_economics(**kwargs):
    from src.runners.enriched import run_shipping_economics

    return _run_enriched_runner(run_shipping_economics, **kwargs)


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
            f"&& ./scripts/run_base_silver.sh "
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
            f"cd {AIRFLOW_HOME} && python -m src.validation.silver_quality "
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
            'gsutil -m rsync -r "$SILVER_BASE_PATH" "$SILVER_GCS_TARGET"; '
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
            f"cd {AIRFLOW_HOME} && python -m src.validation.enriched_quality "
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
            'gsutil -m rsync -r "$SILVER_ENRICHED_PATH" "$SILVER_ENRICHED_GCS_TARGET"; '
            "else echo 'sync skipped'; fi"
        ),
    )

    # 9. Gates
    should_run_gold = ShortCircuitOperator(
        task_id="should_run_gold",
        python_callable=lambda: _resolve_bool(
            "GOLD_PIPELINE_ENABLED", PIPELINE_ENV in {"dev", "prod"}
        ),
    )

    should_load_bigquery = ShortCircuitOperator(
        task_id="should_load_bigquery",
        python_callable=lambda: _resolve_bool(
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
