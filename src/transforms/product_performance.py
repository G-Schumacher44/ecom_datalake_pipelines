"""Product performance (profitability) logic using Polars."""

from __future__ import annotations

import polars as pl

from src.transforms.common import _date_or_null


def compute_product_performance(
    products: pl.LazyFrame,
    order_items: pl.LazyFrame,
    return_items: pl.LazyFrame,
    cart_items: pl.LazyFrame,
) -> pl.LazyFrame:
    """Calculate detailed product-level profitability and conversion."""
    # Ensure inputs are lazy
    products = products.lazy() if isinstance(products, pl.DataFrame) else products
    order_items = order_items.lazy() if isinstance(order_items, pl.DataFrame) else order_items
    return_items = return_items.lazy() if isinstance(return_items, pl.DataFrame) else return_items
    cart_items = cart_items.lazy() if isinstance(cart_items, pl.DataFrame) else cart_items

    product_cols = products.collect_schema().names()
    order_cols = order_items.collect_schema().names()
    return_cols = return_items.collect_schema().names()
    cart_cols = cart_items.collect_schema().names()

    category_expr = (
        pl.col("category") if "category" in product_cols else pl.lit(None)
    ).alias("category")

    products_trim = products.select(
        "product_id",
        "product_name",
        category_expr,
        pl.col("cost_price").alias("catalog_cost_price"),
        pl.col("unit_price").alias("catalog_unit_price"),
        "inventory_quantity",
    )

    order_items = (
        order_items.with_columns(
            product_dt=_date_or_null(order_cols, "order_dt", "ingestion_ts")
        )
        .join(products_trim.select("product_id", "catalog_cost_price"), on="product_id", how="left")
        .with_columns(
            effective_cost_price=pl.coalesce(
                [pl.col("cost_price"), pl.col("catalog_cost_price")]
            )
        )
    )
    return_items = return_items.with_columns(
        product_dt=_date_or_null(return_cols, "return_dt", "ingestion_ts")
    )
    cart_items = cart_items.with_columns(
        product_dt=_date_or_null(cart_cols, "added_dt", "ingestion_ts")
    )

    order_agg = (
        order_items.group_by(["product_id", "product_dt"])
        .agg(
            units_sold=pl.col("quantity").sum(),
            gross_revenue=(pl.col("unit_price") * pl.col("quantity")).sum(),
            gross_margin=(
                (pl.col("unit_price") - pl.col("effective_cost_price").fill_null(0))
                * pl.col("quantity")
            ).sum(),
            order_item_count=pl.len(),
        )
    )

    return_agg = (
        return_items.group_by(["product_id", "product_dt"])
        .agg(
            units_returned=pl.col("quantity_returned").sum(),
            refunded_amount=pl.col("refunded_amount").sum(),
        )
    )

    cart_agg = (
        cart_items.group_by(["product_id", "product_dt"])
        .agg(units_in_carts=pl.col("quantity").sum())
    )

    combined = (
        order_agg.join(
            cart_agg, on=["product_id", "product_dt"], how="full", coalesce=True
        )
        .join(
            return_agg, on=["product_id", "product_dt"], how="full", coalesce=True
        )
    )

    net_margin_expr = pl.col("gross_margin") - pl.col("refunded_amount")

    combined = combined.with_columns(
        units_sold=pl.col("units_sold").fill_null(0),
        units_returned=pl.col("units_returned").fill_null(0),
        units_in_carts=pl.col("units_in_carts").fill_null(0),
        gross_revenue=pl.col("gross_revenue").fill_null(0.0),
        gross_margin=pl.col("gross_margin").fill_null(0.0),
        refunded_amount=pl.col("refunded_amount").fill_null(0.0),
        order_item_count=pl.col("order_item_count").fill_null(0),
    ).with_columns(
        net_revenue=pl.col("gross_revenue") - pl.col("refunded_amount"),
        gross_profit=pl.col("gross_margin"),
        net_margin=net_margin_expr,
        return_rate=pl.when(pl.col("units_sold") > 0)
        .then(pl.col("units_returned") / pl.col("units_sold"))
        .otherwise(None),
        cart_to_order_rate=pl.when(pl.col("units_in_carts") > 0)
        .then(pl.col("units_sold") / pl.col("units_in_carts"))
        .otherwise(None),
        margin_pct=pl.when(pl.col("gross_revenue") > 0)
        .then(net_margin_expr / pl.col("gross_revenue"))
        .otherwise(None),
    )

    return (
        combined.join(products_trim, on="product_id", how="left")
        .select(
            "product_id",
            "product_name",
            "category",
            "product_dt",
            "units_sold",
            "units_returned",
            "units_in_carts",
            "gross_revenue",
            "net_revenue",
            "gross_margin",
            "gross_profit",
            "net_margin",
            "refunded_amount",
            "return_rate",
            "cart_to_order_rate",
            "margin_pct",
            "catalog_unit_price",
            "catalog_cost_price",
            "inventory_quantity",
        )
        .sort(["product_id", "product_dt"])
    )
