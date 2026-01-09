from src.transforms.inventory_risk import compute_inventory_risk


def model(dbt, _session):
    inventory = dbt.ref("stg_ecommerce__product_catalog").pl()
    return compute_inventory_risk(inventory)
