"""Commerce-focused Enriched Silver runners."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

import polars as pl

from src.settings import load_settings
from src.transforms.cart_attribution import (
    compute_cart_attribution,
    compute_cart_attribution_summary,
)
from src.transforms.inventory_risk import compute_inventory_risk
from src.transforms.product_performance import compute_product_performance
from src.transforms.sales_velocity import compute_sales_velocity

from .shared import get_enriched_partitions, read_partitioned, write_partitioned_shards


def run_cart_attribution(
    base_silver_path: str,
    output_path: str,
    ingest_dt: str = "2020-01-01",
) -> Dict[str, Any]:
    """Cart attribution transform."""
    start_time = datetime.now()
    settings = load_settings()
    lookback_days = settings.pipeline.enriched_lookback_days

    carts = read_partitioned(
        base_silver_path, "shopping_carts", ingest_dt, lookback_days
    )
    orders = read_partitioned(
        base_silver_path, "orders", ingest_dt, lookback_days
    )

    # Transform stays lazy
    result_lazy = compute_cart_attribution(
        carts=carts,
        orders=orders,
        tolerance_hours=settings.pipeline.attribution_tolerance_hours,
    ).with_columns(
        order_dt=pl.col("order_date").cast(pl.Date),
        ingest_dt=pl.lit(ingest_dt),
    )

    # Execution happens here
    result = result_lazy.collect()

    write_partitioned_shards(
        result,
        output_path,
        "int_attributed_purchases",
        get_enriched_partitions()["int_attributed_purchases"],
        settings.pipeline.enriched_max_rows_per_file,
    )

    elapsed = (datetime.now() - start_time).total_seconds()
    return {
        "table": "int_attributed_purchases",
        "output_rows": len(result),
        "processing_time_seconds": elapsed,
        "output_path": f"{output_path}/int_attributed_purchases",
    }


def run_cart_attribution_summary(
    base_silver_path: str,
    output_path: str,
    ingest_dt: str = "2020-01-01",
) -> Dict[str, Any]:
    """Cart attribution summary (cart-level)."""
    start_time = datetime.now()
    settings = load_settings()
    lookback_days = settings.pipeline.enriched_lookback_days

    carts = read_partitioned(
        base_silver_path, "shopping_carts", ingest_dt, lookback_days
    )
    cart_items = read_partitioned(
        base_silver_path, "cart_items", ingest_dt, lookback_days
    )
    orders = read_partitioned(
        base_silver_path, "orders", ingest_dt, lookback_days
    )

    result_lazy = compute_cart_attribution_summary(
        carts=carts,
        cart_items=cart_items,
        orders=orders,
        tolerance_hours=settings.pipeline.attribution_tolerance_hours,
    ).with_columns(
        cart_dt=pl.col("created_at").cast(pl.Date),
        ingest_dt=pl.lit(ingest_dt),
    )

    result = result_lazy.collect()

    write_partitioned_shards(
        result,
        output_path,
        "int_cart_attribution",
        get_enriched_partitions()["int_cart_attribution"],
        settings.pipeline.enriched_max_rows_per_file,
    )

    elapsed = (datetime.now() - start_time).total_seconds()
    return {
        "table": "int_cart_attribution",
        "output_rows": len(result),
        "processing_time_seconds": elapsed,
        "output_path": f"{output_path}/int_cart_attribution",
    }


def run_inventory_risk(
    base_silver_path: str,
    output_path: str,
    ingest_dt: str = "2020-01-01",
) -> Dict[str, Any]:
    """Inventory risk scoring transform."""
    start_time = datetime.now()
    settings = load_settings()
    lookback_days = settings.pipeline.enriched_lookback_days

    products = read_partitioned(
        base_silver_path, "product_catalog", ingest_dt, lookback_days
    )
    order_items = read_partitioned(
        base_silver_path, "order_items", ingest_dt, lookback_days
    )
    return_items = read_partitioned(
        base_silver_path, "return_items", ingest_dt, lookback_days
    )

    result_lazy = compute_inventory_risk(
        products=products,
        order_items=order_items,
        return_items=return_items,
    ).with_columns(ingest_dt=pl.lit(ingest_dt))

    result = result_lazy.collect()

    write_partitioned_shards(
        result,
        output_path,
        "int_inventory_risk",
        get_enriched_partitions()["int_inventory_risk"],
        settings.pipeline.enriched_max_rows_per_file,
    )

    elapsed = (datetime.now() - start_time).total_seconds()
    return {
        "table": "int_inventory_risk",
        "output_rows": len(result),
        "processing_time_seconds": elapsed,
        "output_path": f"{output_path}/int_inventory_risk",
    }


def run_product_performance(
    base_silver_path: str,
    output_path: str,
    ingest_dt: str = "2020-01-01",
) -> Dict[str, Any]:
    """Product performance (profitability) transform."""
    start_time = datetime.now()
    settings = load_settings()
    lookback_days = settings.pipeline.enriched_lookback_days

    products = read_partitioned(
        base_silver_path, "product_catalog", ingest_dt, lookback_days
    )
    order_items = read_partitioned(
        base_silver_path, "order_items", ingest_dt, lookback_days
    )
    return_items = read_partitioned(
        base_silver_path, "return_items", ingest_dt, lookback_days
    )
    cart_items = read_partitioned(
        base_silver_path, "cart_items", ingest_dt, lookback_days
    )

    result_lazy = compute_product_performance(
        products=products,
        order_items=order_items,
        return_items=return_items,
        cart_items=cart_items,
    ).with_columns(ingest_dt=pl.lit(ingest_dt))

    result = result_lazy.collect()

    write_partitioned_shards(
        result,
        output_path,
        "int_product_performance",
        get_enriched_partitions()["int_product_performance"],
        settings.pipeline.enriched_max_rows_per_file,
    )

    elapsed = (datetime.now() - start_time).total_seconds()
    return {
        "table": "int_product_performance",
        "output_rows": len(result),
        "processing_time_seconds": elapsed,
        "output_path": f"{output_path}/int_product_performance",
    }


def run_sales_velocity(
    base_silver_path: str,
    output_path: str,
    ingest_dt: str = "2020-01-01",
) -> Dict[str, Any]:
    """Sales velocity transform (rolling windows)."""
    start_time = datetime.now()
    settings = load_settings()
    lookback_days = settings.pipeline.enriched_lookback_days

    orders = read_partitioned(
        base_silver_path, "orders", ingest_dt, lookback_days
    )
    order_items = read_partitioned(
        base_silver_path, "order_items", ingest_dt, lookback_days
    )

    result_lazy = compute_sales_velocity(
        orders=orders,
        order_items=order_items,
        window_days=settings.pipeline.sales_velocity_window_days,
    ).with_columns(ingest_dt=pl.lit(ingest_dt))

    result = result_lazy.collect()

    write_partitioned_shards(
        result,
        output_path,
        "int_sales_velocity",
        get_enriched_partitions()["int_sales_velocity"],
        settings.pipeline.enriched_max_rows_per_file,
    )

    elapsed = (datetime.now() - start_time).total_seconds()
    return {
        "table": "int_sales_velocity",
        "output_rows": len(result),
        "processing_time_seconds": elapsed,
        "output_path": f"{output_path}/int_sales_velocity",
    }
