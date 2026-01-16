from datetime import date

import polars as pl

from src.validation.enriched_quality import evaluate_semantic_checks


def test_semantic_checks_cart_attribution() -> None:
    df = pl.DataFrame(
        {
            "cart_status": ["converted", "abandoned", "abandoned"],
            "order_id": [None, "ORD-1", None],
            "abandoned_value": [0.0, 0.0, 0.0],
            "time_to_purchase_hours": [1, -1, None],
        }
    )

    issues = evaluate_semantic_checks(df, "int_cart_attribution", 0.0001)

    assert "converted_requires_order_id: 1 rows" in issues
    assert "abandoned_requires_null_order_id: 1 rows" in issues
    assert "abandoned_value_positive_for_abandoned: 1 rows" in issues
    assert "time_to_purchase_hours_non_negative: 1 rows" in issues


def test_semantic_checks_product_performance() -> None:
    df = pl.DataFrame(
        {
            "units_sold": [10],
            "units_returned": [12],
            "return_rate": [1.2],
            "units_in_carts": [5],
            "cart_to_order_rate": [2.0],
            "gross_margin": [100.0],
            "net_margin": [120.0],
        }
    )

    issues = evaluate_semantic_checks(df, "int_product_performance", 0.0001)

    assert "units_returned_le_units_sold: 1 rows" in issues
    assert "return_rate_le_one: 1 rows" in issues
    assert "cart_to_order_rate_le_one: 1 rows" in issues
    assert "net_margin_le_gross_margin: 1 rows" in issues


def test_semantic_checks_shipping_economics() -> None:
    df = pl.DataFrame(
        {
            "shipping_cost": [0.0, 10.0],
            "actual_shipping_cost": [5.0, 3.0],
            "shipping_margin": [-5.0, 8.0],
            "shipping_margin_pct": [0.1, 0.7],
        }
    )

    issues = evaluate_semantic_checks(df, "int_shipping_economics", 0.0001)

    assert "shipping_margin_matches_components: 1 rows" in issues
    assert "shipping_margin_pct_null_when_zero_cost: 1 rows" in issues
