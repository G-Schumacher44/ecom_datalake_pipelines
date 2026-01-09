"""Cart attribution logic using Polars."""

from __future__ import annotations

import polars as pl


def compute_cart_attribution(
    carts: pl.DataFrame,
    purchases: pl.DataFrame,
    tolerance_hours: int = 48,
) -> pl.DataFrame:
    """Link purchases to most recent carts within a tolerance window."""
    return (
        purchases.join_asof(
            carts,
            on="timestamp",
            by="customer_id",
            strategy="backward",
            tolerance=f"{tolerance_hours}h",
        )
        .with_columns(is_recovered=pl.col("cart_id").is_not_null())
    )
