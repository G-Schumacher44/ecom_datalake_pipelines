"""Typed settings loaded from config files and environment."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineConfig(BaseModel):
    # GCP Configuration
    project_id: str = Field(..., description="GCP project id")

    # Data Lake Buckets
    bronze_bucket: str = Field(..., description="Bronze GCS bucket name")
    bronze_prefix: str = "bronze"
    silver_bucket: str = Field(..., description="Silver GCS bucket name")
    silver_base_prefix: str = "silver/base"
    silver_enriched_prefix: str = "silver/enriched"

    # BigQuery Datasets
    bigquery_dataset: str = "silver"
    gold_dataset: str = "gold_marts"

    # Observability & Metrics
    environment: str = Field(
        default="local",
        description="Deployment environment: local, dev, or prod",
    )
    metrics_bucket: str = Field(
        default="ecom-datalake-metrics",
        description="GCS bucket for metrics",
    )
    logs_bucket: str = Field(
        default="ecom-datalake-logs",
        description="GCS bucket for logs",
    )

    # Business Logic Configuration
    default_ingest_dt: str = "2020-01-01"
    attribution_tolerance_hours: int = 48
    churn_danger_window_days: list[int] = Field(default_factory=lambda: [30, 90])
    sales_velocity_window_days: int = 7
    sla_thresholds: dict[str, float] = Field(
        default_factory=lambda: {
            "orders": 0.95,
            "customers": 0.98,
            "product_catalog": 0.99,
            "shopping_carts": 0.95,
            "cart_items": 0.95,
            "order_items": 0.95,
            "returns": 0.95,
            "return_items": 0.95,
        }
    )


class Settings(BaseSettings):
    pipeline: PipelineConfig

    model_config = SettingsConfigDict(env_prefix="ECOM_", extra="ignore")

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Settings":
        payload = yaml.safe_load(Path(path).read_text()) or {}
        return cls.model_validate(payload)


def load_settings(config_path: str | Path = "config/config.yml") -> Settings:
    return Settings.from_yaml(config_path)
