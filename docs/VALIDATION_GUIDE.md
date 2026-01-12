# Data Validation Guide

Complete guide to data quality validation in the e-commerce data pipeline.

## Overview

The pipeline has **3 validation gates**:

```
┌──────────────┐
│ Bronze Layer │  (GCS - Raw Data)
└──────┬───────┘
       │
       ▼
┌─────────────────────────────────────────┐
│ GATE 1: Bronze Validation               │
│ - Metadata checks (row counts, schema)  │
│ - Pydantic sample validation            │
│ Output: docs/validation_reports/        │
│         BRONZE_*.md                      │
└──────┬──────────────────────────────────┘
       │ PASS
       ▼
┌──────────────┐
│ Base Silver  │  (dbt transformations)
└──────┬───────┘
       │
       ▼
┌─────────────────────────────────────────┐
│ GATE 2: Silver Quality Validation       │
│ - Pass rate calculation                 │
│ - Quarantine analysis                   │
│ Output: docs/validation_reports/        │
│         SILVER_QUALITY.md                │
└──────┬──────────────────────────────────┘
       │ PASS
       ▼
┌──────────────────┐
│ Enriched Silver  │  (Polars transformations)
└──────┬───────────┘
       │
       ▼
┌─────────────────────────────────────────┐
│ GATE 3: Enriched Validation (Future)    │
│ - Business logic validation             │
│ - Attribution rate checks               │
└─────────────────────────────────────────┘
```

---

## Gate 2: Silver Quality Validation (✅ IMPLEMENTED)

### What It Does

After dbt processes Bronze → Silver, this validator:

1. **Counts rows** in Bronze, Silver, and Quarantine
2. **Calculates pass rates** (Silver / (Silver + Quarantine))
3. **Compares to SLA thresholds** (from `docs/planning/SLA_AND_QUALITY.md`)
4. **Analyzes quarantine reasons** (what failed, how often)
5. **Detects row loss** (Bronze vs. total processed)
6. **Generates dual output**:
   - JSON metrics → `data/metrics/silver_quality/`
   - Markdown report → `docs/validation_reports/SILVER_QUALITY.md`

### Usage

#### Basic Usage

```bash
# Run after dbt completes
python src/validation/silver_quality.py
```

#### With Custom Paths

```bash
python src/validation/silver_quality.py \
  --bronze-path samples/bronze \
  --silver-path data/silver/base \
  --quarantine-path data/silver/quarantine \
  --run-id "20260111_143022"
```

#### In Hard Fail Mode (Stops Pipeline on SLA Breach)

```bash
python src/validation/silver_quality.py \
  --fail-on-sla-breach
```

### Output Files

#### 1. JSON Metrics (Machine-Readable)

**Location:** `data/metrics/silver_quality/silver_quality_{run_id}.json`

```json
{
  "metadata": {
    "written_at": "2026-01-11T14:47:15Z",
    "environment": "local",
    "metric_type": "silver_quality"
  },
  "transformation_metadata": {
    "run_id": "20260111_143022",
    "timestamp": "2026-01-11T14:47:15Z",
    "dbt_project_version": "0.1.0"
  },
  "table_metrics": [
    {
      "table": "orders",
      "row_counts": {
        "bronze_input": 2456789,
        "silver_output": 2398456,
        "quarantine_output": 58333,
        "total_processed": 2456789,
        "row_loss": 0,
        "row_loss_pct": 0.0
      },
      "pass_rate": {
        "rate": 0.9762,
        "sla_threshold": 0.95,
        "status": "PASS"
      },
      "quarantine_breakdown": [...]
    }
  ],
  "overall_status": "PASS"
}
```

#### 2. Markdown Report (Human-Readable)

**Location:** `docs/validation_reports/SILVER_QUALITY.md`

See example in `docs/validation_reports/README.md`

### SLA Thresholds

Defined in the validator (from `docs/planning/SLA_AND_QUALITY.md`):

| Table | Pass Rate SLA |
|-------|--------------|
| orders | 95% |
| customers | 98% |
| product_catalog | 99% |
| shopping_carts | 95% |
| cart_items | 95% |
| order_items | 95% |
| returns | 95% |
| return_items | 95% |

### Status Logic

- **PASS**: Pass rate >= SLA threshold
- **WARN**: Pass rate >= 90% of SLA (within 10%)
- **FAIL**: Pass rate < 90% of SLA

### Integration with Airflow

Add to DAG as **Phase 1.5** (between Base Silver and Enriched Silver):

```python
# In airflow/dags/ecom_silver_to_gold.py

from airflow.operators.bash import BashOperator

validate_silver_quality = BashOperator(
    task_id="validate_silver_quality",
    bash_command=(
        "python src/validation/silver_quality.py "
        "--bronze-path samples/bronze "
        "--silver-path data/silver/base "
        "--quarantine-path data/silver/quarantine "
        "--run-id {{ run_id }} "
        # Optional: Fail pipeline on SLA breach
        # "--fail-on-sla-breach"
    ),
)

# Set dependencies
base_silver_group >> validate_silver_quality >> enriched_silver_group
```

---

## Gate 1: Bronze Validation (⏭️ NEXT TO BUILD)

### Planned Components

#### 1. Bronze Metadata Validation

**Script:** `src/validation/bronze_metadata.py`

**Purpose:** Fast pre-checks before expensive processing

**Checks:**
- Row counts vs. expected (from SLA doc)
- Partition completeness (all expected dates present)
- Schema validation (columns match data contract)
- Freshness (hours since latest partition)
- File health (detect small files, truncation)

**Output:**
- `data/metrics/data_quality/bronze_metadata_{run_id}.json`
- `docs/validation_reports/BRONZE_METADATA.md`

#### 2. Pydantic Sample Validation (Enhanced)

**Script:** `scripts/validate_bronze_samples.py` (existing - needs enhancement)

**Current Coverage:** 3/8 tables
**Needs:** Expand to all 8 tables + trending analysis

**Output:**
- `data/metrics/data_quality/pydantic_validation_{run_id}.json`
- `docs/validation_reports/BRONZE_QUALITY.md`

---

## Viewing Validation Results

### Latest Run

```bash
# View latest Silver quality report
cat docs/validation_reports/SILVER_QUALITY.md
```

### Historical Trends

```bash
# See how quality has changed over time
git log -p docs/validation_reports/SILVER_QUALITY.md

# Compare to previous run
git diff HEAD~1 docs/validation_reports/SILVER_QUALITY.md
```

### Query Metrics (Local)

```python
from src/observability import get_metrics_writer

# Get last 10 Silver quality runs
writer = get_metrics_writer("silver_quality")
recent_runs = writer.read_metrics(limit=10)

# Analyze trends
for run in recent_runs:
    for table in run["table_metrics"]:
        print(f"{table['table']}: {table['pass_rate']['rate']:.2%}")
```

### Query Metrics (Production - BigQuery)

```sql
-- Load metrics from GCS
CREATE EXTERNAL TABLE `my-project.observability.silver_quality`
OPTIONS (
  format = 'JSON',
  uris = ['gs://ecom-datalake-metrics/pipeline_metrics/silver_quality/*.json']
);

-- Track pass rate trends
SELECT
  DATE(metadata.written_at) as date,
  table_metric.table,
  table_metric.pass_rate.rate as pass_rate,
  table_metric.pass_rate.sla_threshold as sla
FROM `my-project.observability.silver_quality`,
  UNNEST(table_metrics) as table_metric
WHERE table_metric.table = 'orders'
ORDER BY date DESC
LIMIT 30;
```

---

## Troubleshooting

### Low Pass Rates

If a table shows low pass rate:

1. **Check quarantine reasons**
   ```bash
   # View top quarantine reasons in report
   grep -A 10 "Top Quarantine Reasons" docs/validation_reports/SILVER_QUALITY.md
   ```

2. **Inspect quarantine data**
   ```python
   import polars as pl
   df = pl.read_parquet("data/silver/quarantine/orders/**/*.parquet")
   print(df.select("invalid_reason").value_counts())
   ```

3. **Compare to Bronze**
   - High FK failures → Check upstream Bronze table quality
   - High duplicates → Check Bronze deduplication logic
   - Invalid dates → Check Bronze timestamp parsing

### Row Loss

If Bronze rows != (Silver + Quarantine):

1. **Check for filtering** in dbt models
2. **Verify all partitions processed**
3. **Check for dbt failures** (partial runs)

### Unexpected Quarantine Spikes

If quarantine rate suddenly increases:

1. **Check upstream data quality** (Bronze validation)
2. **Review recent Bronze schema changes**
3. **Look for new error patterns** in quarantine breakdown

---

## Best Practices

### 1. Always Review Reports After Changes

```bash
# After modifying dbt models
dbt run --select stg_ecommerce__orders
python src/validation/silver_quality.py
git diff docs/validation_reports/SILVER_QUALITY.md
```

### 2. Commit Validation Reports

```bash
# Reports are self-documenting - commit them!
git add docs/validation_reports/SILVER_QUALITY.md
git commit -m "chore: update Silver quality report"
```

### 3. Monitor Trends, Not Just Point-in-Time

```python
# Track pass rate trend over last 7 runs
writer = get_metrics_writer("silver_quality")
runs = writer.read_metrics(limit=7)

for run in runs:
    orders_metric = [m for m in run["table_metrics"] if m["table"] == "orders"][0]
    print(f"{run['transformation_metadata']['run_id']}: {orders_metric['pass_rate']['rate']:.2%}")
```

### 4. Set Appropriate SLA Thresholds

- **Critical tables** (orders, customers): 95-98%
- **Lookup tables** (product_catalog): 99%+
- **Denormalized tables** (order_items): 95%

### 5. Use Soft Fails in Development

```bash
# Development: Warn but don't fail
python src/validation/silver_quality.py
```

```bash
# Production: Fail pipeline on SLA breach
python src/validation/silver_quality.py --fail-on-sla-breach
```

---

## Roadmap

### Implemented ✅
- [x] Silver quality validation
- [x] Dual output (JSON + Markdown)
- [x] Observability framework integration
- [x] Quarantine analysis
- [x] SLA threshold validation

### Next Steps ⏭️
- [ ] Bronze metadata validation
- [ ] Enhanced Pydantic validation (all 8 tables)
- [ ] Historical baseline comparison
- [ ] Anomaly detection
- [ ] Alerting integration (Slack/PagerDuty)

---

## Files Reference

### Validation Scripts
- `src/validation/silver_quality.py` - Silver quality validator
- `scripts/validate_bronze_samples.py` - Bronze Pydantic validation
- `scripts/describe_parquet_samples.py` - Bronze schema profiling

### Validation Reports (Auto-Generated)
- `docs/validation_reports/SILVER_QUALITY.md` - Silver quality report
- `docs/validation_reports/README.md` - Reports documentation

### Metrics (JSON)
- `data/metrics/silver_quality/` - Silver quality metrics
- `data/metrics/data_quality/` - Bronze validation metrics

### Configuration
- `docs/planning/SLA_AND_QUALITY.md` - SLA thresholds and quality requirements
- `docs/planning/DATA_CONTRACT.md` - Schema definitions
