"""Operations-focused Enriched Silver runners."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

import polars as pl

from src.settings import load_settings
from src.transforms.daily_business_metrics import compute_daily_business_metrics

from .shared import get_enriched_partitions, read_partitioned, write_partitioned_shards


def run_daily_business_metrics(
    base_silver_path: str,
    output_path: str,
    ingest_dt: str = "2020-01-01",
) -> Dict[str, Any]:
    """Daily business metrics transform."""
    start_time = datetime.now()
    settings = load_settings()
    lookback_days = settings.pipeline.enriched_lookback_days

    orders = read_partitioned(
        base_silver_path, "orders", ingest_dt, lookback_days
    )
    returns = read_partitioned(
        base_silver_path, "returns", ingest_dt, lookback_days
    )
    carts = read_partitioned(
        base_silver_path, "shopping_carts", ingest_dt, lookback_days
    )

    result_lazy = compute_daily_business_metrics(
        orders=orders,
        returns=returns,
        carts=carts,
        ratio_epsilon=settings.pipeline.enriched_ratio_epsilon,
    ).with_columns(ingest_dt=pl.lit(ingest_dt))

    result = result_lazy.collect()

    write_partitioned_shards(
        result,
        output_path,
        "int_daily_business_metrics",
        get_enriched_partitions()["int_daily_business_metrics"],
        settings.pipeline.enriched_max_rows_per_file,
    )

    elapsed = (datetime.now() - start_time).total_seconds()
    return {
        "table": "int_daily_business_metrics",
        "output_rows": len(result),
        "processing_time_seconds": elapsed,
        "output_path": f"{output_path}/int_daily_business_metrics",
    }
