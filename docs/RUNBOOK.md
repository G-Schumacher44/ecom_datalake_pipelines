# Operations Runbook

This runbook covers common failure scenarios and their resolution for the ecom-datalake-pipelines system.

---

## Table of Contents

1. [Bronze Ingestion Failures](#1-bronze-ingestion-failures)
2. [Base Silver dbt Failures](#2-base-silver-dbt-failures)
3. [Enriched Silver Validation Failures](#3-enriched-silver-validation-failures)
4. [Airflow DAG Failures](#4-airflow-dag-failures)
5. [GCS Connectivity Issues](#5-gcs-connectivity-issues)

---

## 1. Bronze Ingestion Failures

### Symptoms
- Missing `ingest_dt` partitions in Bronze layer
- Manifest file missing or row count mismatch
- Parquet files with invalid magic bytes

### Diagnosis

```bash
# Check Bronze partition exists
ls -la data/bronze/orders/ingest_dt=2025-01-15/

# Validate Parquet file integrity
python -c "
import pyarrow.parquet as pq
pf = pq.ParquetFile('data/bronze/orders/ingest_dt=2025-01-15/part-0.parquet')
print(f'Rows: {pf.metadata.num_rows}')
print(f'Schema: {pf.schema_arrow}')
"

# Check manifest
cat data/bronze/orders/ingest_dt=2025-01-15/_manifest.json
```

### Resolution

**Missing partition:**
1. Re-run the upstream ingestion job for the missing date
2. Verify source system data availability
3. Check ingestion logs for errors

**Corrupt Parquet file:**
1. Delete the corrupt file: `rm data/bronze/orders/ingest_dt=2025-01-15/part-0.parquet`
2. Re-run ingestion for that partition
3. If persists, check source data quality

**Manifest mismatch:**
1. Regenerate manifest: Run Bronze validation to create new manifest
2. If row counts don't match, investigate source system

### Prevention
- Enable Bronze quality checks in Airflow DAG
- Set up alerts for missing partitions
- Monitor ingestion job success rates

---

## 2. Base Silver dbt Failures

### Symptoms
- dbt run exits with non-zero code
- Quarantine tables have high row counts
- Foreign key validation failures
- Duplicate primary key errors

### Diagnosis

```bash
# Run dbt with verbose output
cd dbt_duckdb && dbt run --target local --full-refresh 2>&1 | tee dbt_run.log

# Check specific model
dbt run --select stg_orders --target local

# Inspect quarantine table
python -c "
import polars as pl
df = pl.read_parquet('data/silver/base/quarantine/orders/*.parquet')
print(df.head(10))
print(f'Quarantine rows: {df.height}')
"

# Check Silver validation report
python -m src.validation.silver --silver-path data/silver/base --env local
```

### Resolution

**High quarantine rate (>5%):**
1. Inspect quarantine records for patterns:
   ```python
   import polars as pl
   df = pl.read_parquet('data/silver/base/quarantine/orders/*.parquet')
   # Check failure reasons
   print(df.group_by('_quarantine_reason').len())
   ```
2. Common causes:
   - **Null primary keys**: Check Bronze data quality
   - **Invalid foreign keys**: Ensure reference tables loaded first
   - **Type casting failures**: Review Bronze schema changes
3. Fix upstream data or adjust validation rules in dbt models

**Duplicate primary keys:**
1. Check if source system has duplicates
2. Review deduplication logic in `stg_*` models
3. Ensure `ROW_NUMBER()` window function uses correct ordering

**Foreign key failures:**
1. Verify reference table exists and is populated
2. Check join conditions in `int_*` models
3. Consider adding missing reference data

### Prevention
- Run `dbt test` after each `dbt run`
- Set `max_quarantine_pct` threshold in config
- Monitor quarantine table growth over time

---

## 3. Enriched Silver Validation Failures

### Symptoms
- Validation status: FAIL or WARN
- Sanity check violations (negative values, rates > 1)
- Semantic check failures (business rule violations)
- Missing enriched tables

### Diagnosis

```bash
# Run enriched validation
python -m src.validation.enriched --enriched-path data/silver/enriched --env local

# Check specific table metrics
python -c "
import polars as pl
df = pl.read_parquet('data/silver/enriched/int_customer_lifetime_value/*.parquet')
print(df.describe())

# Check for negative CLV
negatives = df.filter(pl.col('net_clv') < 0)
print(f'Negative CLV records: {negatives.height}')
"

# Review validation report
cat data/metrics/enriched_quality_*.json | jq .
```

### Resolution

**Sanity check: negative values**
1. Identify affected records:
   ```python
   df.filter(pl.col('gross_revenue') < 0).select(['product_id', 'gross_revenue'])
   ```
2. Trace back to source data in Base Silver
3. Common causes:
   - Incorrect aggregation logic
   - Sign errors in refund calculations
   - Missing null handling

**Semantic check: `net_clv_matches_components`**
1. This means `total_spent - total_refunded != net_clv`
2. Check for floating-point precision issues
3. Review CLV calculation in `src/transforms/customer_lifetime_value.py`

**Semantic check: `return_rate_le_one`**
1. Returns exceed sales for a product
2. Check if returns are being double-counted
3. Verify join conditions in product_performance transform

**Missing enriched table:**
1. Verify Base Silver tables exist
2. Check runner logs for errors
3. Run specific runner: `python -m src.runners.enriched.customer`

### Prevention
- Set `enriched_ratio_epsilon` for floating-point tolerance
- Review semantic check expressions in `config/config.yml`
- Add unit tests for edge cases in transforms

---

## 4. Airflow DAG Failures

### Symptoms
- DAG tasks marked as failed in Airflow UI
- Sensor timeouts waiting for upstream data
- Task retries exhausted

### Diagnosis

```bash
# Check Airflow logs
docker-compose logs airflow-worker | grep ERROR

# Check specific task logs in Airflow UI
# Navigate to: DAG > Task Instance > Logs

# Verify DAG is parsed correctly
docker-compose exec airflow-scheduler airflow dags list-import-errors
```

### Resolution

**Sensor timeout (waiting for Bronze data):**
1. Check if upstream ingestion completed
2. Verify sensor is checking correct path/partition
3. Increase `timeout` or `poke_interval` if data arrives late

**Task failure with OOM:**
1. Increase worker memory in `docker-compose.yml`
2. For large tables, enable partitioned processing
3. Consider using `enriched_max_rows_per_file` setting

**dbt task failure:**
1. Check dbt logs in task output
2. Common issues: DuckDB lock contention, missing dependencies
3. Ensure `dbt deps` ran before `dbt run`

**Retry exhaustion:**
1. Review root cause of initial failure
2. Manually clear task state: `airflow tasks clear <dag_id> -t <task_id>`
3. Re-trigger DAG run

### Prevention
- Set appropriate `retries` and `retry_delay` in DAG
- Use `on_failure_callback` for alerting
- Monitor DAG SLAs in Airflow

---

## 5. GCS Connectivity Issues

### Symptoms
- `google.auth.exceptions.DefaultCredentialsError`
- `gcsfs` connection timeouts
- Permission denied errors on bucket operations

### Diagnosis

```bash
# Verify credentials are set
echo $GOOGLE_APPLICATION_CREDENTIALS
cat $GOOGLE_APPLICATION_CREDENTIALS | jq .client_email

# Test GCS access
gsutil ls gs://your-bucket-name/

# Check service account permissions
gcloud projects get-iam-policy your-project-id \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:your-sa@your-project.iam.gserviceaccount.com"
```

### Resolution

**Missing credentials:**
1. Set environment variable:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
   ```
2. For Airflow, add to `docker-compose.yml` environment
3. For local dev, use `gcloud auth application-default login`

**Permission denied:**
1. Required roles for service account:
   - `roles/storage.objectViewer` (read Bronze)
   - `roles/storage.objectCreator` (write Silver)
   - `roles/bigquery.dataEditor` (Gold layer)
2. Grant permissions:
   ```bash
   gcloud storage buckets add-iam-policy-binding gs://your-bucket \
     --member="serviceAccount:your-sa@project.iam.gserviceaccount.com" \
     --role="roles/storage.objectAdmin"
   ```

**Connection timeouts:**
1. Check network connectivity to GCS
2. Verify VPC/firewall rules if running in GCP
3. Increase timeout in `fsspec` configuration

### Prevention
- Use Workload Identity in GKE instead of key files
- Rotate service account keys regularly
- Set up IAM alerts for permission changes

---

## Quick Reference: Validation Exit Codes

| Exit Code | Meaning | Action Required |
|-----------|---------|-----------------|
| 0 | All checks passed | None |
| 1 | FAIL status in prod | Fix data quality issues before proceeding |
| 1 | FAIL with `--enforce` | Validation gate blocked pipeline |

## Quick Reference: Key Configuration

| Setting | Location | Purpose |
|---------|----------|---------|
| `max_quarantine_pct` | config/config.yml | Max allowed quarantine rate |
| `enriched_ratio_epsilon` | config/config.yml | Float comparison tolerance |
| `min_table_rows` | config/config.yml | Minimum rows per table |
| `sla_thresholds` | config/config.yml | Pass rate thresholds by table |

## Contact & Escalation

1. **L1 (Self-service)**: Follow this runbook
2. **L2 (Team)**: Check `#data-platform` Slack channel
3. **L3 (Escalation)**: Page on-call via PagerDuty

---

*Last updated: 2025-01-17*
