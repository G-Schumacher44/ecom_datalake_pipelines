"""Finance-focused Enriched Silver runners."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

import polars as pl

from src.settings import load_settings
from src.transforms.regional_financials import compute_regional_financials
from src.transforms.shipping_economics import compute_shipping_economics

from .shared import get_enriched_partitions, read_partitioned, write_partitioned_shards


def run_regional_financials(
    base_silver_path: str,
    output_path: str,
    ingest_dt: str = "2020-01-01",
) -> Dict[str, Any]:
    """Regional financials transform."""
    start_time = datetime.now()
    settings = load_settings()
    lookback_days = settings.pipeline.enriched_lookback_days

    orders = read_partitioned(
        base_silver_path, "orders", ingest_dt, lookback_days
    )
    customers = read_partitioned(
        base_silver_path, "customers", ingest_dt, lookback_days
    )

    result_lazy = compute_regional_financials(
        orders=orders,
        customers=customers,
    ).with_columns(ingest_dt=pl.lit(ingest_dt))

    result = result_lazy.collect()

    write_partitioned_shards(
        result,
        output_path,
        "int_regional_financials",
        get_enriched_partitions()["int_regional_financials"],
        settings.pipeline.enriched_max_rows_per_file,
    )

    elapsed = (datetime.now() - start_time).total_seconds()
    return {
        "table": "int_regional_financials",
        "output_rows": len(result),
        "processing_time_seconds": elapsed,
        "output_path": f"{output_path}/int_regional_financials",
    }


def run_shipping_economics(
    base_silver_path: str,
    output_path: str,
    ingest_dt: str = "2020-01-01",
) -> Dict[str, Any]:
    """Shipping economics transform."""
    start_time = datetime.now()
    settings = load_settings()
    lookback_days = settings.pipeline.enriched_lookback_days

    orders = read_partitioned(
        base_silver_path, "orders", ingest_dt, lookback_days
    )

    result_lazy = compute_shipping_economics(
        orders=orders,
    ).with_columns(ingest_dt=pl.lit(ingest_dt))

    result = result_lazy.collect()

    write_partitioned_shards(
        result,
        output_path,
        "int_shipping_economics",
        get_enriched_partitions()["int_shipping_economics"],
        settings.pipeline.enriched_max_rows_per_file,
    )

    elapsed = (datetime.now() - start_time).total_seconds()
    return {
        "table": "int_shipping_economics",
        "output_rows": len(result),
        "processing_time_seconds": elapsed,
        "output_path": f"{output_path}/int_shipping_economics",
    }
