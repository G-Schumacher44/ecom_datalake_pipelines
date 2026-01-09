from src.transforms.regional_financials import compute_regional_financials


def model(dbt, _session):
    orders = dbt.ref("stg_ecommerce__orders").pl()
    tax_rates = dbt.ref("stg_tax_rates").pl()
    return compute_regional_financials(orders, tax_rates)
