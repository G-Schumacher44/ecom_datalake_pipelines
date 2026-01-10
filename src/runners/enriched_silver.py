"""
Enriched Silver runner functions.

Each function:
1. Reads required Base Silver tables from GCS
2. Calls transform logic from src/transforms/
3. Writes Enriched Silver parquet to GCS
4. Returns metadata (row counts, processing time)
"""

import polars as pl
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

from src.transforms.cart_attribution import compute_cart_attribution
from src.transforms.inventory_risk import compute_inventory_risk
from src.transforms.churn_detection import compute_churn_signals
from src.transforms.sales_velocity import compute_sales_velocity
from src.transforms.regional_financials import compute_regional_financials
from src.settings import get_settings


def run_cart_attribution(
    base_silver_path: str,
    output_path: str,
    ingest_dt: str = "2020-01-01",
) -> Dict[str, Any]:
    """
    Cart attribution transform.

    Reads: shopping_carts, orders
    Writes: int_attributed_purchases

    Args:
        base_silver_path: GCS path to Base Silver (e.g., gs://bucket/ecom/base)
        output_path: GCS path for Enriched Silver (e.g., gs://bucket/ecom/enriched)
        ingest_dt: Partition date for output

    Returns:
        Metadata dict with row counts and processing time
    """
    start_time = datetime.now()
    settings = get_settings()

    # Read Base Silver from GCS
    carts = pl.read_parquet(f"{base_silver_path}/shopping_carts/*.parquet")
    orders = pl.read_parquet(f"{base_silver_path}/orders/*.parquet")

    # Apply transform
    result = compute_cart_attribution(
        carts=carts,
        orders=orders,
        tolerance_hours=settings.pipeline.attribution_tolerance_hours,
    )

    # Write to GCS
    output_uri = f"{output_path}/int_attributed_purchases/ingest_dt={ingest_dt}/"
    result.write_parquet(output_uri)

    elapsed = (datetime.now() - start_time).total_seconds()

    return {
        "table": "int_attributed_purchases",
        "input_rows": {"carts": len(carts), "orders": len(orders)},
        "output_rows": len(result),
        "processing_time_seconds": elapsed,
        "output_path": output_uri,
    }


def run_inventory_risk(
    base_silver_path: str,
    output_path: str,
    ingest_dt: str = "2020-01-01",
) -> Dict[str, Any]:
    """
    Inventory risk scoring transform.

    Reads: product_catalog, order_items, return_items
    Writes: int_inventory_risk
    """
    start_time = datetime.now()

    products = pl.read_parquet(f"{base_silver_path}/product_catalog/*.parquet")
    order_items = pl.read_parquet(f"{base_silver_path}/order_items/*.parquet")
    return_items = pl.read_parquet(f"{base_silver_path}/return_items/*.parquet")

    result = compute_inventory_risk(
        products=products,
        order_items=order_items,
        return_items=return_items,
    )

    output_uri = f"{output_path}/int_inventory_risk/ingest_dt={ingest_dt}/"
    result.write_parquet(output_uri)

    elapsed = (datetime.now() - start_time).total_seconds()

    return {
        "table": "int_inventory_risk",
        "input_rows": {
            "products": len(products),
            "order_items": len(order_items),
            "return_items": len(return_items),
        },
        "output_rows": len(result),
        "processing_time_seconds": elapsed,
        "output_path": output_uri,
    }


def run_customer_retention(
    base_silver_path: str,
    output_path: str,
    ingest_dt: str = "2020-01-01",
) -> Dict[str, Any]:
    """
    Customer retention signals transform.

    Reads: customers, orders
    Writes: int_customer_retention_signals
    """
    start_time = datetime.now()
    settings = get_settings()

    customers = pl.read_parquet(f"{base_silver_path}/customers/*.parquet")
    orders = pl.read_parquet(f"{base_silver_path}/orders/*.parquet")

    result = compute_churn_signals(
        customers=customers,
        orders=orders,
        churn_windows_days=settings.pipeline.churn_danger_window_days,
    )

    output_uri = f"{output_path}/int_customer_retention_signals/ingest_dt={ingest_dt}/"
    result.write_parquet(output_uri)

    elapsed = (datetime.now() - start_time).total_seconds()

    return {
        "table": "int_customer_retention_signals",
        "input_rows": {"customers": len(customers), "orders": len(orders)},
        "output_rows": len(result),
        "processing_time_seconds": elapsed,
        "output_path": output_uri,
    }


def run_sales_velocity(
    base_silver_path: str,
    output_path: str,
    ingest_dt: str = "2020-01-01",
) -> Dict[str, Any]:
    """
    Sales velocity transform (rolling windows).

    Reads: orders, order_items
    Writes: int_sales_velocity
    """
    start_time = datetime.now()
    settings = get_settings()

    orders = pl.read_parquet(f"{base_silver_path}/orders/*.parquet")
    order_items = pl.read_parquet(f"{base_silver_path}/order_items/*.parquet")

    result = compute_sales_velocity(
        orders=orders,
        order_items=order_items,
        window_days=settings.pipeline.sales_velocity_window_days,
    )

    output_uri = f"{output_path}/int_sales_velocity/ingest_dt={ingest_dt}/"
    result.write_parquet(output_uri)

    elapsed = (datetime.now() - start_time).total_seconds()

    return {
        "table": "int_sales_velocity",
        "input_rows": {"orders": len(orders), "order_items": len(order_items)},
        "output_rows": len(result),
        "processing_time_seconds": elapsed,
        "output_path": output_uri,
    }


def run_regional_financials(
    base_silver_path: str,
    output_path: str,
    ingest_dt: str = "2020-01-01",
) -> Dict[str, Any]:
    """
    Regional financials transform (tax + currency).

    Reads: orders, customers
    Writes: int_regional_financials
    """
    start_time = datetime.now()

    orders = pl.read_parquet(f"{base_silver_path}/orders/*.parquet")
    customers = pl.read_parquet(f"{base_silver_path}/customers/*.parquet")

    result = compute_regional_financials(
        orders=orders,
        customers=customers,
    )

    output_uri = f"{output_path}/int_regional_financials/ingest_dt={ingest_dt}/"
    result.write_parquet(output_uri)

    elapsed = (datetime.now() - start_time).total_seconds()

    return {
        "table": "int_regional_financials",
        "input_rows": {"orders": len(orders), "customers": len(customers)},
        "output_rows": len(result),
        "processing_time_seconds": elapsed,
        "output_path": output_uri,
    }
