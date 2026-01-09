"""Regional financial enrichment."""

from __future__ import annotations

import polars as pl


def compute_regional_financials(orders: pl.DataFrame, tax_rates: pl.DataFrame) -> pl.DataFrame:
    """Attach tax rates and net revenue calculations."""
    return orders.join(tax_rates, on="region").with_columns(
        tax_amount=pl.col("total_price") * pl.col("tax_rate"),
        net_revenue=pl.col("total_price") - (pl.col("total_price") * pl.col("tax_rate")),
    )
