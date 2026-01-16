import polars as pl

from src.transforms.regional_financials import compute_regional_financials


def test_compute_regional_financials_defaults_tax_rate() -> None:
    orders = pl.DataFrame(
        {
            "order_id": ["o-1"],
            "customer_id": ["c-1"],
            "gross_total": [100.0],
        }
    )
    customers = pl.DataFrame(
        {
            "customer_id": ["c-1"],
            "region": ["west"],
        }
    )

    result = compute_regional_financials(orders, customers)
    row = result.row(0, named=True)

    assert row["region"] == "west"
    assert row["tax_rate"] == 0.0
    assert row["tax_amount"] == 0.0
    assert row["net_revenue"] == 100.0
