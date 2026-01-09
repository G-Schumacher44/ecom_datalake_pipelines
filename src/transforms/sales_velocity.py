"""Sales velocity calculations."""

from __future__ import annotations

import polars as pl


def compute_sales_velocity(sales: pl.DataFrame, window_days: int = 7) -> pl.DataFrame:
    """Calculate rolling averages and trend signals."""
    return sales.with_columns(
        velocity_avg=pl.col("quantity").rolling_mean(
            window_size=f"{window_days}d", by="product_id"
        )
    ).with_columns(
        trend_signal=pl.when(pl.col("quantity") > pl.col("velocity_avg") * 1.2)
        .then("UP")
        .when(pl.col("quantity") < pl.col("velocity_avg") * 0.8)
        .then("DOWN")
        .otherwise("STABLE")
    )
