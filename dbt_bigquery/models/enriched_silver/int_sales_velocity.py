from src.transforms.sales_velocity import compute_sales_velocity


def model(dbt, _session):
    order_items = dbt.ref("stg_ecommerce__order_items").pl()
    return compute_sales_velocity(order_items)
