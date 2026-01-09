"""Inventory risk scoring logic."""

from __future__ import annotations

import polars as pl


def compute_inventory_risk(inventory: pl.DataFrame) -> pl.DataFrame:
    """Compute utilization, locked capital, and risk tiers."""
    with_metrics = inventory.with_columns(
        utilization_ratio=(pl.col("sales_volume") / pl.col("stock_level")).fill_nan(0),
        locked_capital=pl.col("unit_cost") * pl.col("stock_level"),
    )
    return with_metrics.with_columns(
        attention_score=(
            pl.col("utilization_signal") + pl.col("return_signal")
        ).clip(0, 1),
        risk_tier=pl.when(pl.col("attention_score") >= 0.8)
        .then("HIGH")
        .when(pl.col("attention_score") >= 0.5)
        .then("MODERATE")
        .otherwise("HEALTHY"),
    )
