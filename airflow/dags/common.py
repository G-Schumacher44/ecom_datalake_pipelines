from __future__ import annotations

import os
from datetime import timedelta

from src.settings import load_settings, RetryConfig

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

# --- Helpers ---

def resolve_bool(env_key: str, default: bool = False) -> bool:
    """Resolve a boolean environment variable."""
    env_override = os.getenv(env_key)
    if env_override is None:
        return default
    return env_override.lower() in {"true", "1", "yes"}

def run_enriched_runner(func, **kwargs):
    """Wrapper to run enriched runners with resolved paths."""
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

def make_runner_callable(runner_func):
    """Factory to create Airflow-compatible callables for runners."""
    def wrapper(**kwargs):
        return run_enriched_runner(runner_func, **kwargs)
    return wrapper

def get_retry_config(environment: str | None = None) -> dict:
    """Get Airflow retry configuration for the current environment.

    Args:
        environment: Environment name (local/dev/prod). If None, resolves from PIPELINE_ENV
                    or config.

    Returns:
        Dictionary with Airflow default_args retry settings:
        - retries: Number of retry attempts
        - retry_delay: timedelta for initial delay
        - retry_exponential_backoff: Whether to use exponential backoff
        - max_retry_delay: timedelta for maximum retry delay
    """
    config = SettingsConfig()
    env = environment or config.resolve_pipeline_env()

    # Get retry config for this environment
    retry_cfg: RetryConfig = config.pipeline.retry_config.get(env)
    if not retry_cfg:
        raise ValueError(
            f"No retry_config found for environment '{env}'. "
            f"Available: {list(config.pipeline.retry_config.keys())}"
        )

    return {
        "retries": retry_cfg.retries,
        "retry_delay": timedelta(minutes=retry_cfg.retry_delay_minutes),
        "retry_exponential_backoff": retry_cfg.retry_exponential_backoff,
        "max_retry_delay": timedelta(minutes=retry_cfg.max_retry_delay_minutes),
    }
