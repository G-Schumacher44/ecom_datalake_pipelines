import polars as pl

from src.transforms.cart_attribution import compute_cart_attribution


def test_compute_cart_attribution_flags_recovered() -> None:
    carts = pl.DataFrame(
        {
            "cart_id": ["c1"],
            "customer_id": ["cust-1"],
            "timestamp": [pl.datetime(2020, 1, 1, 10, 0)],
        }
    )
    purchases = pl.DataFrame(
        {
            "order_id": ["o1"],
            "customer_id": ["cust-1"],
            "timestamp": [pl.datetime(2020, 1, 1, 20, 0)],
        }
    )

    result = compute_cart_attribution(carts, purchases, tolerance_hours=48)

    assert result.shape[0] == 1
    assert result[0, "is_recovered"] is True
    assert result[0, "cart_id"] == "c1"
