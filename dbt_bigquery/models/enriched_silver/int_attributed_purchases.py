from src.transforms.cart_attribution import compute_cart_attribution


def model(dbt, _session):
    carts = dbt.ref("stg_ecommerce__shopping_carts").pl()
    purchases = dbt.ref("stg_ecommerce__orders").pl()

    result = compute_cart_attribution(carts, purchases)
    return result
