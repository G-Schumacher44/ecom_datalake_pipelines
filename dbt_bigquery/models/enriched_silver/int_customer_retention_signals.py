from datetime import date

from src.transforms.churn_detection import compute_churn_signals


def model(dbt, _session):
    customers = dbt.ref("stg_ecommerce__customers").pl()
    return compute_churn_signals(customers, current_date=date.today())
