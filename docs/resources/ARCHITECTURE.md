# Architecture Overview

## Data Flow Diagram

```mermaid
graph TD
    A[Bronze Layer<br/>Parquet in GCS] -->|Bronze Quality Check| B[Base Silver<br/>dbt + DuckDB]
    B -->|Silver Quality Check| C[Enriched Silver<br/>Polars Transforms]
    C -->|Enriched Quality Check| D[GCS Sync<br/>gcloud storage rsync (publish)]
    D --> E[BigQuery Load<br/>Parquet Import]
    E --> F[Gold Marts<br/>dbt + BigQuery]

    B -.->|Quarantine| G[Quarantine Tables]
```

## Text Flow

```
Bronze (Parquet in GCS or local)
    ↓  [Bronze Quality Validation]
Base Silver (dbt + DuckDB)
    ↓  [Silver Quality Validation + Optional Profile]
Enriched Silver (Polars transforms)
    ↓  [Enriched Quality Validation]
    ↓  [Sync to GCS with gcloud storage rsync (publish only)]
BigQuery Load (Parquet import)
    ↓
Gold Marts (dbt + BigQuery)
```

## Key Layers

- **Bronze**: Raw, partitioned parquet files with lineage metadata
- **Base Silver**: Cleaned/typed tables and quarantine outputs (dbt + DuckDB)
- **Enriched Silver**: Derived features/aggregations using **Polars Lazy API** for optimized query plans and memory efficiency
- **Gold**: Business-facing marts (dbt + BigQuery)

## Configuration & Gating

- **Centralized Config**: All SLAs, table partitions, and semantic checks are managed in `config/config.yml`
- **Quality Gates**:
    - **Bronze**: Pre-compute checks for manifests and empty partitions
    - **Silver**: Post-compute checks for pass rates and SLAs
    - **Enriched**: Schema validation, key uniqueness, semantic constraints
    - **Gating**: Strict enforcement (`--enforce-quality`) ensures the pipeline stops immediately on failure in Production

## Airflow DAG Architecture

The project uses Apache Airflow for orchestration with two primary DAGs:

### DAG 1: `ecom_dim_refresh_pipeline`

**Purpose**: Refresh dimension tables (customers, product_catalog) independently of fact table processing.

**Trigger**: Manual or on-demand (no schedule)

**Flow**:
```
setup_config
    ↓
validate_bronze_dims (customers, product_catalog)
    ↓
refresh_customers (dbt)
    ↓
refresh_product_catalog (dbt)
    ↓
validate_dim_quality
```

**Key Features**:
- Independent refresh for slowly-changing dimensions
- Parallel processing of customer/product refreshes (sequential shown for clarity)
- Quality gates before and after transformation
- Can be triggered independently or as part of main pipeline

### DAG 2: `ecom_silver_to_gold_pipeline`

**Purpose**: Full pipeline from Bronze validation through Gold marts.

**Trigger**: Manual or scheduled (configured via `schedule` parameter)

**Flow**:
```
setup_config
    ↓
trigger_dim_refresh (wait for completion)
    ↓
validate_bronze_quality
    ↓
base_silver (dbt - all fact tables)
    ↓
validate_silver_quality
    ↓
sync_silver_base_to_gcs
    ↓
enriched_silver (TaskGroup - 10 parallel tasks)
    ├─ int_attributed_purchases
    ├─ int_cart_attribution
    ├─ int_inventory_risk
    ├─ int_product_performance
    ├─ int_customer_retention_signals
    ├─ int_sales_velocity
    ├─ int_regional_financials
    ├─ int_customer_lifetime_value
    ├─ int_daily_business_metrics
    └─ int_shipping_economics
    ↓
validate_enriched_quality
    ↓
sync_silver_enriched_to_gcs
    ↓
should_load_bigquery (ShortCircuitOperator)
    ↓
load_to_bigquery (TaskGroup - 10 parallel loads)
    ↓
should_run_gold (ShortCircuitOperator)
    ↓
gold_marts_build (dbt + BigQuery)
```

**Key Features**:
- **Dependency Management**: Triggers `ecom_dim_refresh_pipeline` and waits for completion
- **Parallel Processing**: Enriched Silver tasks run concurrently (up to `max_active_tasks` limit)
- **Environment Gates**: BigQuery load and Gold marts conditionally execute based on environment
- **GCS Sync**: Ensures Parquet files are available in cloud storage for BigQuery import

### Configuration Management

DAGs use a lazy-loading configuration pattern via `SettingsConfig`:

```python
# airflow/dags/common.py
class SettingsConfig:
    """Lazy configuration loader for Airflow DAGs."""

    def __init__(self):
        self._settings = None
        self._config_path = os.getenv("ECOM_CONFIG_PATH", f"{AIRFLOW_HOME}/config/config.yml")

    @property
    def settings(self):
        if self._settings is None:
            self._settings = load_settings(self._config_path)
        return self._settings
```

**Benefits**:
- Config loaded only when needed (DAG parsing doesn't load heavy config)
- Environment-specific overrides via `ECOM_CONFIG_PATH`
- Shared config instance across tasks

### XCom Pattern

Config paths are pushed to XCom in `setup_pipeline_config` task:

```python
def load_config_to_xcom(**kwargs):
    config = SettingsConfig()
    return {
        "bronze": config.resolve_path(...),
        "silver": config.resolve_path(...),
        "enriched": config.resolve_path(...),
        "project_id": config.pipeline.project_id,
        "env": config.resolve_pipeline_env(),
    }
```

Downstream tasks pull these values:

```bash
--bronze-path {{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}
```

**Benefits**:
- Single source of truth for paths
- Environment-agnostic task definitions
- Easy to test locally vs. cloud

### Environment-Specific Behavior

**Local (`environment: local`)**:
- Writes to `./data/` directories
- Skips GCS sync tasks
- Skips BigQuery load (via `should_load_bigquery` gate)
- Skips Gold marts (via `should_run_gold` gate)
- Uses DuckDB for all dbt transformations

**Dev/Prod (`environment: dev|prod`)**:
- Reads/writes to GCS buckets
- Executes GCS sync with `gcloud storage rsync`
- Loads data to BigQuery
- Runs Gold marts in BigQuery
- Enforces quality gates (`--enforce-quality`)

### Task Groups

**Enriched Silver TaskGroup**:
```python
with TaskGroup("enriched_silver") as enriched_silver_group:
    PythonOperator(task_id="int_attributed_purchases", ...)
    PythonOperator(task_id="int_cart_attribution", ...)
    # ... 10 total tasks
```

**Benefits**:
- Logical grouping in Airflow UI
- Parallel execution (limited by `max_active_tasks`)
- Simplified dependency management

**BigQuery Load TaskGroup**:
```python
with TaskGroup("load_to_bigquery") as load_bigquery_group:
    for table in enriched_tables:
        BigQueryInsertJobOperator(task_id=f"load_{table}", ...)
```

**Benefits**:
- Dynamic task generation from config
- Parallel loads to BigQuery
- Partition-level WRITE_TRUNCATE for idempotency

## Error Recovery & Retry Strategy

### Config-Driven Retry Settings

Retry behavior is configured per environment in `config/config.yml`:

```yaml
retry_config:
  local:
    retries: 1
    retry_delay_minutes: 1
    retry_exponential_backoff: false
    max_retry_delay_minutes: 5
  dev:
    retries: 2
    retry_delay_minutes: 2
    retry_exponential_backoff: true
    max_retry_delay_minutes: 10
  prod:
    retries: 3
    retry_delay_minutes: 5
    retry_exponential_backoff: true
    max_retry_delay_minutes: 30
```

DAGs automatically load these settings:

```python
with DAG(
    dag_id="ecom_silver_to_gold_pipeline",
    default_args=get_retry_config(),  # Reads from config.yml
    ...
) as dag:
```

**Retry Strategy by Environment**:
- **Local**: Minimal retries (fast failure for development)
- **Dev**: Moderate retries with exponential backoff (balance speed + reliability)
- **Prod**: Aggressive retries with longer backoff (maximize success rate)

### Task-Level Recovery

- **dbt tasks**: Built-in idempotency via table materialization
- **Polars transforms**: Deterministic, safe to re-run
- **GCS sync**: `gcloud storage rsync` with `--delete-unmatched-destination-objects` is used for publish only (validation reads gs:// directly)
- **BigQuery loads**: `WRITE_TRUNCATE` disposition for partition-level idempotency

### Failure Handling

- **Validation failures**: Pipeline halts in strict mode (`--enforce-quality` in dev/prod)
- **Transform failures**: Logged to structured error logs with full traceback
- **Quarantined data**: Isolated to quarantine tables, does not block downstream processing
- **Transient failures**: Automatically retried with exponential backoff (dev/prod)
