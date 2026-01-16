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
- **Enriched Silver**: Derived features/aggregations using **Polars Lazy API** for optimized query plans and memory efficiency.
- **Gold**: Business-facing marts (dbt + BigQuery).

## Configuration & Gating

- **Centralized Config**: All SLAs, table partitions, and semantic checks are managed in `config/config.yml`.
- **Quality Gates**:
    - **Bronze**: Pre-compute checks for manifests and empty partitions.
    - **Silver**: Post-compute checks for pass rates and SLAs.
    - **Gating**: Strict enforcement (`--enforce-quality`) ensures the pipeline stops immediately on failure in Production.
