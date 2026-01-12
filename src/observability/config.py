"""Environment-aware configuration for observability."""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class Environment(Enum):
    """Deployment environment."""

    LOCAL = "local"
    DEV = "dev"
    PROD = "prod"


@dataclass(frozen=True)
class ObservabilityConfig:
    """Configuration for metrics and logging storage.

    Adapts to local development vs production environments:
    - Local: Writes to data/metrics/ and data/logs/
    - Production: Writes to GCS buckets

    Environment is determined by PIPELINE_ENV environment variable.
    """

    environment: Environment
    project_root: Path
    metrics_bucket: str | None = None
    logs_bucket: str | None = None

    @classmethod
    def from_env(cls) -> ObservabilityConfig:
        """Create config from environment variables with fallback to config.yml.

        Priority:
        1. PIPELINE_ENV environment variable
        2. config.yml pipeline.environment
        3. Default: "local"
        """
        project_root = Path(__file__).parent.parent.parent

        # Try loading from config.yml first
        config_env = "local"
        config_metrics_bucket = "ecom-datalake-metrics"
        config_logs_bucket = "ecom-datalake-logs"

        try:
            from ..settings import load_settings

            settings = load_settings()
            config_env = settings.pipeline.environment
            config_metrics_bucket = settings.pipeline.metrics_bucket
            config_logs_bucket = settings.pipeline.logs_bucket
        except Exception:
            # Config file not found or invalid - use defaults
            pass

        # Environment variable overrides config file
        env_str = os.getenv("PIPELINE_ENV", config_env).lower()
        environment = Environment(env_str)

        # In production, use GCS buckets
        if environment in (Environment.DEV, Environment.PROD):
            metrics_bucket = os.getenv("METRICS_BUCKET", config_metrics_bucket)
            logs_bucket = os.getenv("LOGS_BUCKET", config_logs_bucket)
        else:
            metrics_bucket = None
            logs_bucket = None

        return cls(
            environment=environment,
            project_root=project_root,
            metrics_bucket=metrics_bucket,
            logs_bucket=logs_bucket,
        )

    @property
    def metrics_base_path(self) -> str:
        """Base path for metrics storage."""
        if self.environment == Environment.LOCAL:
            return str(self.project_root / "data" / "metrics")
        return f"gs://{self.metrics_bucket}/pipeline_metrics"

    @property
    def logs_base_path(self) -> str:
        """Base path for logs storage."""
        if self.environment == Environment.LOCAL:
            return str(self.project_root / "data" / "logs")
        return f"gs://{self.logs_bucket}/pipeline_logs"

    def get_metrics_path(self, metric_type: str) -> str:
        """Get path for specific metric type.

        Args:
            metric_type: Type of metric (e.g., 'pipeline_runs', 'data_quality')

        Returns:
            Full path for storing metrics of this type
        """
        return f"{self.metrics_base_path}/{metric_type}"

    def get_logs_path(self, log_type: str) -> str:
        """Get path for specific log type.

        Args:
            log_type: Type of log (e.g., 'errors', 'audit')

        Returns:
            Full path for storing logs of this type
        """
        return f"{self.logs_base_path}/{log_type}"

    @property
    def is_local(self) -> bool:
        """Whether running in local development mode."""
        return self.environment == Environment.LOCAL

    @property
    def use_cloud_logging(self) -> bool:
        """Whether to send logs to Cloud Logging (Stackdriver)."""
        return self.environment in (Environment.DEV, Environment.PROD)

    def ensure_local_dirs(self) -> None:
        """Create local directories if in local mode."""
        if not self.is_local:
            return

        # Create metrics subdirectories
        metrics_types = [
            "pipeline_runs",
            "data_quality",
            "silver_quality",
            "enriched_silver",
        ]
        for metric_type in metrics_types:
            path = Path(self.get_metrics_path(metric_type))
            path.mkdir(parents=True, exist_ok=True)

        # Create logs subdirectories
        log_types = ["errors", "audit", "debug"]
        for log_type in log_types:
            path = Path(self.get_logs_path(log_type))
            path.mkdir(parents=True, exist_ok=True)


def get_config() -> ObservabilityConfig:
    """Get the observability configuration for current environment."""
    return ObservabilityConfig.from_env()


def get_metrics_writer(metric_type: str):
    """Get a metrics writer for the current environment.

    Args:
        metric_type: Type of metric to write

    Returns:
        MetricsWriter instance
    """
    from .metrics import MetricsWriter

    config = get_config()
    return MetricsWriter(config, metric_type)
