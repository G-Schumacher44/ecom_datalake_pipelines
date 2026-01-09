"""Churn risk detection logic."""

from __future__ import annotations

from datetime import date

import polars as pl


def compute_churn_signals(
    customers: pl.DataFrame, current_date: date
) -> pl.DataFrame:
    """Flag customers in danger windows or stalled in bronze tier."""
    return customers.with_columns(
        days_since_first_buy=(pl.lit(current_date) - pl.col("first_purchase_date")).dt.days(),
        days_since_last_buy=(pl.lit(current_date) - pl.col("last_purchase_date")).dt.days(),
    ).with_columns(
        is_in_danger_zone=(pl.col("days_since_first_buy").is_between(30, 90))
        & (pl.col("total_orders") == 1),
        needs_bronze_nudge=(pl.col("loyalty_tier") == "Bronze")
        & (pl.col("days_since_last_buy") > 14),
    )
