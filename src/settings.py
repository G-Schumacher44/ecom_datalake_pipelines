"""Typed settings loaded from config files and environment."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class ValidationConfig(BaseModel):
    key_fields: dict[str, list[str]] = Field(default_factory=dict)
    sanity_checks: dict[str, list[str]] = Field(default_factory=dict)
    semantic_checks: dict[str, list[dict[str, str]]] = Field(default_factory=dict)

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
    max_quarantine_pct: float = Field(
        default=5.0,
        description="Max overall quarantine percentage before flagging in validation.",
    )
    max_row_loss_pct: float = Field(
        default=1.0,
        description="Max overall row loss percentage before flagging in validation.",
    )
    min_return_id_distinct_ratio: float = Field(
        default=0.001,
        description="Minimum distinct return_id ratio for contract-level checks.",
    )
    expected_bronze_partitions: list[str] = Field(
        default_factory=list,
        description="Expected bronze ingest_dt partitions for completeness checks.",
    )
    min_table_rows: dict[str, int] = Field(
        default_factory=dict,
        description="Minimum processed row counts per table before enriched runs.",
    )
    enriched_tables: list[str] = Field(
        default_factory=lambda: [
            "int_attributed_purchases",
            "int_cart_attribution",
            "int_inventory_risk",
            "int_customer_retention_signals",
            "int_customer_lifetime_value",
            "int_daily_business_metrics",
            "int_product_performance",
            "int_sales_velocity",
            "int_regional_financials",
            "int_shipping_economics",
        ],
        description="Expected Enriched Silver tables for validation.",
    )
    enriched_min_table_rows: dict[str, int] = Field(
        default_factory=dict,
        description="Minimum row counts per enriched table.",
    )
    enriched_lookback_days: int = Field(
        default=0,
        description="Days of ingest_dt partitions to include in enriched reads.",
    )
    enriched_max_rows_per_file: int = Field(
        default=500_000,
        description="Max rows per Parquet file for enriched outputs.",
    )
    enriched_ratio_epsilon: float = Field(
        default=0.0001,
        description="Tolerance for enriched ratio checks (floating-point drift).",
    )
    table_partitions: dict[str, str | None] = Field(default_factory=dict)
    enriched_partitions: dict[str, str] = Field(default_factory=dict)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)


class Settings(BaseSettings):
    pipeline: PipelineConfig

    model_config = SettingsConfigDict(env_prefix="ECOM_", extra="ignore")

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Settings":
        try:
            payload = yaml.safe_load(Path(path).read_text()) or {}
        except OSError as exc:
            payload = {}
            logger.warning(f"Failed to read config {path}: {exc}")
        try:
            return cls.model_validate(payload)
        except ValidationError as exc:
            logger.warning(f"Invalid config in {path}; falling back to defaults: {exc}")
            return cls(
                pipeline=PipelineConfig(
                    project_id=os.getenv(
                        "GOOGLE_CLOUD_PROJECT",
                        os.getenv("ECOM_PROJECT_ID", "local"),
                    ),
                    bronze_bucket=os.getenv("ECOM_BRONZE_BUCKET", "local"),
                    silver_bucket=os.getenv("ECOM_SILVER_BUCKET", "local"),
                )
            )


def load_settings(config_path: str | Path | None = None) -> Settings:
    if config_path is None:
        config_path = "config/config.yml"
    return Settings.from_yaml(config_path)
