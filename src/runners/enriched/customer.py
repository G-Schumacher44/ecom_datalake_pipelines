"""Customer-focused Enriched Silver runners."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict

import polars as pl

from src.settings import load_settings
from src.transforms.churn_detection import compute_customer_retention_signals
from src.transforms.customer_lifetime_value import compute_customer_lifetime_value

from .shared import get_enriched_partitions, read_partitioned, write_partitioned_shards


def run_customer_retention(
    base_silver_path: str,
    output_path: str,
    ingest_dt: str = "2020-01-01",
) -> Dict[str, Any]:
    """Customer retention signals transform."""
    start_time = datetime.now()
    settings = load_settings()
    lookback_days = settings.pipeline.enriched_lookback_days

    customers = read_partitioned(
        base_silver_path, "customers", ingest_dt, lookback_days
    )
    orders = read_partitioned(
        base_silver_path, "orders", ingest_dt, lookback_days
    )

    result_lazy = compute_customer_retention_signals(
        customers=customers,
        orders=orders,
        lookback_days=settings.pipeline.churn_danger_window_days,
        reference_date=date.fromisoformat(ingest_dt),
    ).with_columns(ingest_dt=pl.lit(ingest_dt))

    result = result_lazy.collect()

    write_partitioned_shards(
        result,
        output_path,
        "int_customer_retention_signals",
        get_enriched_partitions()["int_customer_retention_signals"],
        settings.pipeline.enriched_max_rows_per_file,
    )

    elapsed = (datetime.now() - start_time).total_seconds()
    return {
        "table": "int_customer_retention_signals",
        "output_rows": len(result),
        "processing_time_seconds": elapsed,
        "output_path": f"{output_path}/int_customer_retention_signals",
    }


def run_customer_lifetime_value(
    base_silver_path: str,
    output_path: str,
    ingest_dt: str = "2020-01-01",
) -> Dict[str, Any]:
    """Customer lifetime value transform."""
    start_time = datetime.now()
    settings = load_settings()
    lookback_days = settings.pipeline.enriched_lookback_days

    customers = read_partitioned(
        base_silver_path, "customers", ingest_dt, lookback_days
    )
    orders = read_partitioned(
        base_silver_path, "orders", ingest_dt, lookback_days
    )
    returns = read_partitioned(
        base_silver_path, "returns", ingest_dt, lookback_days
    )

    result_lazy = compute_customer_lifetime_value(
        customers=customers,
        orders=orders,
        returns=returns,
        reference_date=date.fromisoformat(ingest_dt),
    ).with_columns(ingest_dt=pl.lit(ingest_dt))

    result = result_lazy.collect()

    write_partitioned_shards(
        result,
        output_path,
        "int_customer_lifetime_value",
        get_enriched_partitions()["int_customer_lifetime_value"],
        settings.pipeline.enriched_max_rows_per_file,
    )

    elapsed = (datetime.now() - start_time).total_seconds()
    return {
        "table": "int_customer_lifetime_value",
        "output_rows": len(result),
        "processing_time_seconds": elapsed,
        "output_path": f"{output_path}/int_customer_lifetime_value",
    }
