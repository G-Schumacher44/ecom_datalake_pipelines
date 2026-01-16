"""Regional financial enrichment."""

from __future__ import annotations

import polars as pl


def compute_regional_financials(
    orders: pl.LazyFrame,
    customers: pl.LazyFrame,
) -> pl.LazyFrame:
    """Analyze financial performance by region and customer tier."""
    # Ensure inputs are lazy
    orders = orders.lazy() if isinstance(orders, pl.DataFrame) else orders
    customers = customers.lazy() if isinstance(customers, pl.DataFrame) else customers

    enriched = orders
    if "region" not in enriched.columns and "region" in customers.columns:
        enriched = enriched.join(
            customers.select(["customer_id", "region"]),
            on="customer_id",
            how="left",
        )
    if "region" in enriched.columns:
        address = pl.coalesce(
            [pl.col("shipping_address"), pl.col("billing_address")]
        )
        state = address.str.extract(r",\\s*([A-Z]{2})\\s\\d{5}", 1)
        enriched = enriched.with_columns(
            pl.when(pl.col("region").is_null())
            .then(pl.concat_str([pl.lit("US-"), state]))
            .otherwise(pl.col("region"))
            .alias("region")
        )
    else:
        address = pl.coalesce(
            [pl.col("shipping_address"), pl.col("billing_address")]
        )
        state = address.str.extract(r",\\s*([A-Z]{2})\\s\\d{5}", 1)
        enriched = enriched.with_columns(
            pl.concat_str([pl.lit("US-"), state]).alias("region")
        )

    return enriched
