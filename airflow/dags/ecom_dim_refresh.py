from __future__ import annotations

import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum

from src.settings import load_settings

# --- Configuration Helper (Lazy Loading) ---

class SettingsConfig:
    """Lazy configuration loader for Airflow DAGs."""
    
    def __init__(self):
        self._settings = None
        self._airflow_home = os.getenv("AIRFLOW_HOME", "/opt/airflow")
        self._config_path = os.getenv("ECOM_CONFIG_PATH", f"{self._airflow_home}/config/config.yml")

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

# --- Top-Level Constants ---

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
        "bronze": config.resolve_path(pl.bronze_bucket, pl.bronze_prefix, "BRONZE_BASE_PATH"),
        "silver": config.resolve_path(pl.silver_bucket, pl.silver_base_prefix, "SILVER_BASE_PATH"),
        "env": p_env,
    }

# --- DAG Definition ---

with DAG(
    dag_id="ecom_dim_refresh_pipeline",
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={"retries": 1},
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
            f"&& ./scripts/run_base_silver.sh "
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
            f"&& ./scripts/run_base_silver.sh "
            "--select stg_ecommerce__product_catalog stg_ecommerce__product_catalog_quarantine"
        ),
    )

    # 5. Validate Dim Quality
    validate_dim_quality = BashOperator(
        task_id="validate_dim_quality",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && python -m src.validation.silver_quality "
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