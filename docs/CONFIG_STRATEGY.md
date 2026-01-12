# Configuration Strategy

## Design Philosophy: Config-First with Environment Overrides

**Primary Source:** `config/config.yml` (version controlled, discoverable)  
**Override Mechanism:** Environment variables (deployment-specific)

This hybrid approach gives you:
- ✅ Single source of truth in Git
- ✅ Deployment flexibility via env vars
- ✅ Clear precedence rules
- ✅ Self-documenting defaults

---

## Configuration Hierarchy (Priority Order)

```
┌─────────────────────────────────────┐
│ 1. Environment Variables (HIGHEST) │  ← Deployment-specific overrides
├─────────────────────────────────────┤
│ 2. config/config.yml                │  ← Version-controlled defaults
├─────────────────────────────────────┤
│ 3. Code Defaults (LOWEST)           │  ← Fallback if config missing
└─────────────────────────────────────┘
```

### Example: `metrics_bucket`

**Lookup order:**
1. Check `METRICS_BUCKET` env var → Use if set
2. Check `config.yml` → `pipeline.metrics_bucket`
3. Fall back to hardcoded default: `"ecom-datalake-metrics"`

---

## Observability Configuration

### Local Development (Default)

**config/config.yml:**
```yaml
pipeline:
  environment: "local"
  metrics_bucket: "ecom-datalake-metrics"  # Unused in local mode
  logs_bucket: "ecom-datalake-logs"        # Unused in local mode
```

**Result:**
- Metrics → `./data/metrics/`
- Logs → `./data/logs/`
- No GCS access needed

**No .env file required!** ✅

---

### Development Environment

**Option 1: Update config.yml (Recommended)**

Create `config/config.dev.yml`:
```yaml
pipeline:
  environment: "dev"
  metrics_bucket: "ecom-datalake-dev-metrics"
  logs_bucket: "ecom-datalake-dev-logs"
  project_id: "my-dev-project"
  # ... other dev settings
```

Then run: `python src/validation/silver_quality.py --config config/config.dev.yml`

**Option 2: Environment Variables**

Keep `config.yml` unchanged, override via env:
```bash
export PIPELINE_ENV=dev
export METRICS_BUCKET=ecom-datalake-dev-metrics
python src/validation/silver_quality.py
```

---

### Production Environment

**Option 1: Update config.yml on deployment**

Production repo has different `config.yml`:
```yaml
pipeline:
  environment: "prod"
  project_id: "my-prod-project"
  bronze_bucket: "ecom-prod-bronze"
  silver_bucket: "ecom-prod-silver"
  metrics_bucket: "ecom-prod-metrics"
  logs_bucket: "ecom-prod-logs"
```

**Option 2: Environment Variables (Recommended for Airflow)**

Set in Airflow environment:
```bash
PIPELINE_ENV=prod
METRICS_BUCKET=ecom-prod-metrics
LOGS_BUCKET=ecom-prod-logs
```

This keeps `config.yml` generic across environments.

---

## All Configuration Options

### In config/config.yml

```yaml
pipeline:
  # GCP Configuration
  project_id: "your-gcp-project"

  # Data Lake Buckets (use "local" for local filesystem)
  bronze_bucket: "local"
  bronze_prefix: "samples/bronze"
  silver_bucket: "local"
  silver_base_prefix: "data/silver/base"
  silver_enriched_prefix: "data/silver/enriched"

  # BigQuery Datasets
  bigquery_dataset: "silver"
  gold_dataset: "gold_marts"

  # Observability & Metrics
  environment: "local"                      # local, dev, or prod
  metrics_bucket: "ecom-datalake-metrics"   # Used when environment=dev/prod
  logs_bucket: "ecom-datalake-logs"         # Used when environment=dev/prod

  # Business Logic Configuration
  default_ingest_dt: "2020-01-01"
  attribution_tolerance_hours: 48
  churn_danger_window_days: [30, 90]
  sales_velocity_window_days: 7
```

### Environment Variable Overrides

**See `.env.example` for all options.**

Key overrides:
- `PIPELINE_ENV` → Overrides `pipeline.environment`
- `METRICS_BUCKET` → Overrides `pipeline.metrics_bucket`
- `LOGS_BUCKET` → Overrides `pipeline.logs_bucket`
- `BRONZE_BASE_PATH` → Overrides dbt var (for dbt models)
- `SILVER_BASE_PATH` → Overrides dbt var (for dbt models)
- `SILVER_QUARANTINE_PATH` → Overrides silver quarantine output path
- `SILVER_ENRICHED_PATH` → Overrides silver enriched output path

---

## When to Use Each Approach

### Use config.yml When:
- ✅ Setting applies to all developers (default paths, business logic)
- ✅ Value should be version controlled
- ✅ You want changes tracked in Git
- ✅ Configuration is environment-independent

### Use Environment Variables When:
- ✅ Value differs per deployment (dev vs prod buckets)
- ✅ Value contains secrets (API keys, credentials)
- ✅ You need quick local overrides during debugging
- ✅ Running in CI/CD or Airflow

---

## Migration Guide: Local → Production

### Step 1: Update config.yml

**Before (local dev):**
```yaml
pipeline:
  environment: "local"
  bronze_bucket: "local"
  silver_bucket: "local"
```

**After (production-ready):**
```yaml
pipeline:
  environment: "prod"
  project_id: "my-prod-project"
  bronze_bucket: "ecom-prod-bronze"
  silver_bucket: "ecom-prod-silver"
  metrics_bucket: "ecom-prod-metrics"
  logs_bucket: "ecom-prod-logs"
```

### Step 2: Create GCS Buckets

```bash
gsutil mb -p my-prod-project gs://ecom-prod-metrics
gsutil mb -p my-prod-project gs://ecom-prod-logs
```

### Step 3: Grant Permissions

```bash
gsutil iam ch serviceAccount:airflow@my-prod-project.iam.gserviceaccount.com:roles/storage.objectAdmin \
  gs://ecom-prod-metrics

gsutil iam ch serviceAccount:airflow@my-prod-project.iam.gserviceaccount.com:roles/storage.objectAdmin \
  gs://ecom-prod-logs
```

### Step 4: Deploy

No environment variables needed - config.yml has everything!

---

## Best Practices

### ✅ DO

- **Commit config.yml** with sensible defaults
- **Use env vars** for deployment-specific overrides
- **Document** any required env vars in README
- **Version control** production config separately if needed
- **Use `config.prod.yml`** and pass `--config` flag in production

### ❌ DON'T

- Don't hardcode production bucket names in code
- Don't commit `.env` files (use `.env.example`)
- Don't duplicate config across files
- Don't use env vars for business logic (use config.yml)

---

## Troubleshooting

### "Metrics writing to wrong location"

Check config precedence:
```python
from src.observability import get_config
config = get_config()
print(f"Environment: {config.environment}")
print(f"Metrics path: {config.metrics_base_path}")
```

### "Can't find config.yml"

Pass explicit path:
```bash
python src/validation/silver_quality.py --config /path/to/config.yml
```

### "Env vars not taking effect"

Verify they're exported:
```bash
env | grep PIPELINE_ENV
env | grep METRICS_BUCKET
```

---

## Summary: Your Question Answered

> **Q:** Should `METRICS_BUCKET` and `LOGS_BUCKET` be env or config driven?

**A:** **Config-driven with env overrides** (hybrid approach)

**What we implemented:**
1. ✅ **Primary:** `config/config.yml` contains default values
2. ✅ **Override:** Environment variables can override config
3. ✅ **Fallback:** Code has hardcoded defaults if both missing

**Precedence:** `ENV VAR > config.yml > code default`

**Result:** 
- Local dev works with **zero configuration**
- Production can use **config.yml OR env vars** (your choice!)
- Maximum flexibility with clear, predictable behavior

---

## Files Modified

- ✅ `config/config.yml` - Added observability fields
- ✅ `src/settings.py` - Added environment, metrics_bucket, logs_bucket fields
- ✅ `src/observability/config.py` - Updated to read from config.yml with env overrides
- ✅ `.env.example` - Commented out observability vars (now optional)
- ✅ `docs/CONFIG_STRATEGY.md` - This file
