# Spec-Driven Orchestration Overview

This pipeline uses layered YAML specs to define what should exist per layer.
The runtime spec drives table lists, partitions, quality gates, and paths.

## Diagram (Spec-Driven Flow)

```
config/specs/*.yml
  ├─ bronze.yml
  ├─ silver_base.yml
  ├─ dims.yml
  ├─ enriched.yml
  └─ validation.yml
        |
        v
merge + expand env vars
        |
        v
runtime spec object
        |
        +--> runners (dbt + polars)
        |
        +--> validation gates
        |
        +--> publish semantics (_staging + _latest)
```

## Minimal Example Spec

```yaml
bronze:
  base_path: "${BRONZE_BASE_PATH:-samples/bronze}"
  tables:
    - name: "customers"
      partition_key: "signup_date"

silver_base:
  base_path: "${SILVER_BASE_PATH:-data/silver/base}"
  quarantine_path: "${SILVER_QUARANTINE_PATH:-data/silver/base/quarantine}"
  tables:
    - name: "orders"
      partition_key: "ingestion_dt"
      source: "bronze.orders"
      dbt_model: "stg_ecommerce__orders"
      quality:
        sla: 0.95
        min_rows: 1

dims:
  base_path: "${SILVER_DIMS_PATH:-data/silver/dims}"
  tables:
    - name: "customers"
      partition_key: "snapshot_dt"
      dbt_model: "stg_ecommerce__customers"

silver_enriched:
  base_path: "${SILVER_ENRICHED_PATH:-data/silver/enriched}"
  lookback_days: 0
  tables:
    - name: "int_sales_velocity"
      partition_key: "order_dt"
      inputs:
        - "silver_base.orders"
        - "silver_base.order_items"
      min_rows: 1

validation:
  reports_enabled: true
  output_dir: "docs/validation_reports"
```

## Notes
- Paths come from the spec first, then env overrides, then config defaults.
- Validation gates use spec-derived table lists + partition keys.
- Reports can be disabled via `REPORTS_ENABLED=false` or spec flag.
