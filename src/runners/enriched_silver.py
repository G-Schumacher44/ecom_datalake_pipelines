"""Compatibility wrapper for Enriched Silver runners."""

from __future__ import annotations

from src.runners.enriched import (
    run_cart_attribution,
    run_cart_attribution_summary,
    run_inventory_risk,
    run_product_performance,
    run_sales_velocity,
    run_customer_lifetime_value,
    run_customer_retention,
    run_regional_financials,
    run_shipping_economics,
    run_daily_business_metrics,
)

__all__ = [
    "run_cart_attribution",
    "run_cart_attribution_summary",
    "run_inventory_risk",
    "run_product_performance",
    "run_sales_velocity",
    "run_customer_lifetime_value",
    "run_customer_retention",
    "run_regional_financials",
    "run_shipping_economics",
    "run_daily_business_metrics",
]
