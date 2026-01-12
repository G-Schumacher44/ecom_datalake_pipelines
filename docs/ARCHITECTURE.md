# Architecture Overview

```
Bronze (Parquet in GCS or local)
    ↓  [Bronze Quality Validation]
Base Silver (dbt + DuckDB)
    ↓  [Silver Quality Validation + Optional Profile]
Enriched Silver (Polars transforms)
    ↓  [Sync to GCS + BQ load]
Gold Marts (dbt + BigQuery)
```

## Key Layers

- **Bronze**: Raw, partitioned parquet files.
- **Base Silver**: Cleaned/typed tables and quarantine outputs (dbt).
- **Enriched Silver**: Derived features/aggregations (Polars).
- **Gold**: Business-facing marts (dbt + BigQuery).

## Quality Gates

- Bronze validation pre‑compute.
- Silver validation post‑compute (soft in local/dev, strict in prod).
