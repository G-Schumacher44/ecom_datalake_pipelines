# Environment Variable Strategy

## Three Categories of Configuration

### 1. Infrastructure Config (Config-First with Env Overrides)

**Pattern:** `config.yml` → env var → code default

**When to use:**
- Deployment-specific settings (buckets, datasets, regions)
- Values that differ between local/dev/prod
- Non-sensitive infrastructure settings

**Example:**
```yaml
# config/config.yml
pipeline:
  environment: "local"
  metrics_bucket: "ecom-datalake-metrics"
  bronze_bucket: "local"
```

**Override via env:**
```bash
export PIPELINE_ENV=prod
export METRICS_BUCKET=ecom-prod-metrics
```

**✅ Good for:** Buckets, datasets, regions, environment names  
**❌ Not for:** Secrets, per-developer settings, business logic

---

### 2. Secrets & Credentials (Env-Only)

**Pattern:** env var → error (no default)

**When to use:**
- API keys, passwords, tokens
- Service account credentials
- Any security-sensitive value

**Example:**
```bash
# .env (NEVER commit)
GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
DBT_BIGQUERY_KEYFILE=/path/to/service-account.json
OPENAI_API_KEY=sk-...
DATABASE_PASSWORD=secret
```

**In code:**
```python
import os

# No default - will raise error if not set
api_key = os.environ["OPENAI_API_KEY"]

# Or with explicit check
if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS must be set")
```

**✅ Good for:** Credentials, keys, passwords  
**❌ Not for:** Non-sensitive config, business logic

---

### 3. Business Logic (Config-Only)

**Pattern:** config.yml → code default (no env override)

**When to use:**
- Business rules and constants
- Algorithm parameters
- SLA thresholds
- Feature behavior

**Example:**
```yaml
# config/config.yml
pipeline:
  attribution_tolerance_hours: 48
  churn_danger_window_days: [30, 90]
  min_order_value: 10.00
  max_retry_attempts: 3
```

**In code:**
```python
from src.settings import load_settings

settings = load_settings()
# Use directly - no env var override
tolerance = settings.pipeline.attribution_tolerance_hours
```

**✅ Good for:** Business rules, algorithm params, thresholds  
**❌ Not for:** Deployment settings, secrets

---

## Quick Reference Table

| Category | Examples | Config.yml? | Env Var? | Default? | Commit? |
|----------|----------|-------------|----------|----------|---------|
| **Infrastructure** | Buckets, datasets, regions | ✅ Primary | ✅ Override | ✅ Yes | ✅ Yes |
| **Secrets** | API keys, passwords | ❌ Never | ✅ Required | ❌ No | ❌ Never |
| **Business Logic** | SLAs, tolerances, rules | ✅ Only | ❌ No override | ✅ Yes | ✅ Yes |
| **Per-Dev** | Debug flags, local paths | ❌ No | ✅ Optional | ✅ Yes | ❌ .env only |

---

## Examples from Your Pipeline

### Infrastructure (Config + Env)
```yaml
# config.yml
pipeline:
  environment: "local"          # Override: PIPELINE_ENV
  metrics_bucket: "..."         # Override: METRICS_BUCKET
  bronze_bucket: "local"        # Override: BRONZE_BUCKET
  project_id: "my-project"      # Override: GOOGLE_CLOUD_PROJECT
```

### Secrets (Env Only)
```bash
# .env (not in config.yml)
GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
DBT_BIGQUERY_KEYFILE=/path/to/sa.json
```

### Business Logic (Config Only)
```yaml
# config.yml (no env override)
pipeline:
  attribution_tolerance_hours: 48
  churn_danger_window_days: [30, 90]
  sales_velocity_window_days: 7
  default_ingest_dt: "2020-01-01"
```

---

## Anti-Patterns to Avoid

### ❌ DON'T: Put secrets in config.yml
```yaml
# BAD - Never do this
pipeline:
  api_key: "sk-secret123"
  database_password: "admin123"
```

### ❌ DON'T: Allow env override for business logic
```python
# BAD - Business rule shouldn't vary per deployment
attribution_hours = int(os.getenv("ATTRIBUTION_HOURS", "48"))
```

### ❌ DON'T: Hardcode deployment-specific values
```python
# BAD - Should be in config.yml
METRICS_BUCKET = "ecom-prod-metrics"  # Hardcoded!
```

---

## When to Make Exceptions

**Sometimes you need flexibility for debugging:**

```python
# OK for debug-only settings
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
```

**Emergency overrides (use sparingly):**
```python
# Allow env override for emergencies, but warn
sla_threshold = float(
    os.getenv("OVERRIDE_SLA_THRESHOLD") or 
    settings.pipeline.sla_threshold
)
if os.getenv("OVERRIDE_SLA_THRESHOLD"):
    logger.warning("SLA threshold overridden by environment variable!")
```

---

## Summary

**Not every env variable should work like observability settings.**

Use this decision tree:

```
Is it a secret/credential?
├─ YES → Env-only (no config.yml, no default)
└─ NO → Is it deployment-specific?
    ├─ YES → Config-first with env override
    └─ NO → Config-only (no env override)
```

**Your current setup:**
- ✅ Observability: Config-first (correct!)
- ⚠️ Secrets: Should be env-only
- ✅ Business logic: Config-only (correct!)
