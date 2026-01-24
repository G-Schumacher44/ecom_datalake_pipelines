# AI-Assisted dbt Model Generation Templates

Use these prompts with Claude Code, GitHub Copilot, or other AI coding assistants to rapidly generate the remaining Base Silver models.

## Setup Context

Before generating models, provide the AI with:
1. This file
2. `macros/cleaning_utils.sql` (our reusable macros)
3. `models/base_silver/stg_ecommerce__orders.sql` (transaction table pattern)
4. `models/base_silver/stg_ecommerce__customers.sql` (dimension table pattern)
5. `docs/planning/DATA_CONTRACT.md` (schema definitions)
6. `docs/planning/SLA_AND_QUALITY.md` (validation rules)

---

## Template 1: shopping_carts (Transaction Table with 1 FK)

**Prompt:**
```
Using the orders model as a template, create stg_ecommerce__shopping_carts.sql with:

FROM DATA_CONTRACT.md:
- cart_id (string, not null) - primary key
- customer_id (string, not null) - FK to customers
- created_at (timestamp, not null)
- updated_at (timestamp)
- cart_total (float64)
- status (string)
- batch_id, ingestion_ts, event_id, source_file (lineage columns)

Validation rules:
- cart_id must be non-null and non-empty
- customer_id must exist in dim_customers
- created_at must be valid timestamp
- updated_at >= created_at (if both not null)
- cart_total >= 0 (if not null)
- Deduplicate by cart_id, keep most recent ingestion_ts
- Partition output by created_dt (cast created_at to date)

Also create stg_ecommerce__shopping_carts_quarantine.sql
```

---

## Template 2: cart_items (Transaction Table with 2 FKs)

**Prompt:**
```
Using the orders model as a template, create stg_ecommerce__cart_items.sql with:

FROM DATA_CONTRACT.md:
- cart_item_id (int64, not null) - primary key
- cart_id (string, not null) - FK to shopping_carts
- product_id (int64, not null) - FK to product_catalog
- product_name (string)
- category (string)
- added_at (timestamp)
- quantity (int64, not null)
- unit_price (float64, not null)
- batch_id, ingestion_ts, event_id, source_file (lineage columns)

Validation rules:
- cart_item_id must be > 0
- cart_id must exist in shopping_carts (create dim_shopping_carts CTE)
- product_id must exist in product_catalog (create dim_products CTE)
- quantity must be > 0
- unit_price must be >= 0
- Deduplicate by cart_item_id, keep most recent ingestion_ts
- Partition output by added_dt (cast added_at to date)

Also create stg_ecommerce__cart_items_quarantine.sql
```

---

## Template 3: order_items (Transaction Table with 2 FKs)

**Prompt:**
```
Using the orders model as a template, create stg_ecommerce__order_items.sql with:

FROM DATA_CONTRACT.md:
- order_id (string, not null) - FK to orders
- product_id (int64, not null) - FK to product_catalog
- product_name (string)
- category (string)
- quantity (int64, not null)
- unit_price (float64, not null)
- discount_amount (float64)
- cost_price (float64)
- batch_id, ingestion_ts, event_id, source_file (lineage columns)

NOTE: order_items has NO unique ID - it's a fact table with composite key (order_id + product_id)

Validation rules:
- order_id must be non-null and exist in orders table (create dim_orders CTE)
- product_id must exist in product_catalog (create dim_products CTE)
- quantity must be > 0
- unit_price must be >= 0
- discount_amount >= 0 (if not null)
- cost_price >= 0 (if not null)
- Deduplicate by (order_id, product_id), keep most recent ingestion_ts
- DON'T partition output (no natural partition key - just write to /order_items/)

Also create stg_ecommerce__order_items_quarantine.sql
```

---

## Template 4: returns (Transaction Table with 2 FKs)

**Prompt:**
```
Using the orders model as a template, create stg_ecommerce__returns.sql with:

FROM DATA_CONTRACT.md:
- return_id (string, not null) - primary key
- order_id (string, not null) - FK to orders
- customer_id (string, not null) - FK to customers
- email (string)
- return_date (timestamp, not null)
- reason (string)
- return_type (string)
- refunded_amount (float64)
- return_channel (string)
- agent_id (string)
- refund_method (string)
- batch_id, ingestion_ts, event_id, source_file (lineage columns)

Validation rules:
- return_id must be non-null
- order_id must exist in orders (create dim_orders CTE)
- customer_id must exist in customers (create dim_customers CTE)
- return_date must be valid timestamp
- refunded_amount >= 0 (if not null)
- Deduplicate by return_id, keep most recent ingestion_ts
- Partition output by return_dt (cast return_date to date)

Also create stg_ecommerce__returns_quarantine.sql
```

---

## Template 5: return_items (Transaction Table with 3 FKs)

**Prompt:**
```
Using the orders model as a template, create stg_ecommerce__return_items.sql with:

FROM DATA_CONTRACT.md:
- return_item_id (int64, not null) - primary key
- return_id (string, not null) - FK to returns
- order_id (string, not null) - FK to orders
- product_id (int64, not null) - FK to product_catalog
- product_name (string)
- category (string)
- quantity_returned (int64, not null)
- unit_price (float64)
- cost_price (float64)
- refunded_amount (float64)
- batch_id, ingestion_ts, event_id, source_file (lineage columns)

Validation rules:
- return_item_id must be > 0
- return_id must exist in returns (create dim_returns CTE)
- order_id must exist in orders (create dim_orders CTE)
- product_id must exist in product_catalog (create dim_products CTE)
- quantity_returned must be > 0
- refunded_amount >= 0 (if not null)
- unit_price >= 0 (if not null)
- cost_price >= 0 (if not null)
- Deduplicate by return_item_id, keep most recent ingestion_ts
- DON'T partition output (no natural partition key)

Also create stg_ecommerce__return_items_quarantine.sql
```

---

## Post-Hook Pattern (Export to Parquet)

All models should include this config for exporting to GCS-compatible structure:

```sql
{{ config(
    materialized='table',
    post_hook=[
        "COPY (SELECT * FROM {{ this }}) TO '{{ var('silver_base_path') }}/TABLE_NAME' (FORMAT PARQUET, PARTITION_BY (partition_column), OVERWRITE_OR_IGNORE)"
    ]
) }}
```

For non-partitioned tables (order_items, return_items):
```sql
{{ config(
    materialized='table',
    post_hook=[
        "COPY (SELECT * FROM {{ this }}) TO '{{ var('silver_base_path') }}/TABLE_NAME' (FORMAT PARQUET, OVERWRITE_OR_IGNORE)"
    ]
) }}
```

---

## Macro Usage Checklist

When generating models, ALWAYS use these macros instead of manual logic:

- ✅ `{{ normalize_string('column') }}` - for IDs, addresses, names
- ✅ `{{ normalize_string_lower('column') }}` - for emails, channels, tiers
- ✅ `{{ safe_cast_timestamp('column') }}` - for all timestamp fields
- ✅ `{{ safe_cast_date('column') }}` - for date fields
- ✅ `{{ safe_cast_integer('column') }}` - for int64 fields
- ✅ `{{ safe_cast_decimal('column', 18, 2) }}` - for money/price fields
- ✅ `{{ safe_cast_boolean('column') }}` - for boolean fields
- ✅ `{{ is_valid_id('column') }}` - in validation scoring
- ✅ `{{ is_valid_timestamp('column') }}` - in validation scoring
- ✅ `{{ is_positive_number('column') }}` - for quantities (> 0)
- ✅ `{{ is_non_negative_number('column') }}` - for prices (>= 0)

---

## Testing After Generation

After generating each model pair (main + quarantine):

```bash
cd dbt_duckdb

# Test compilation
dbt compile --select stg_ecommerce__TABLE_NAME

# Test execution (if samples exist)
dbt run --select stg_ecommerce__TABLE_NAME

# Run tests
dbt test --select stg_ecommerce__TABLE_NAME

# Check quarantine output
dbt run --select stg_ecommerce__TABLE_NAME_quarantine
```

---

## Example: Full Workflow for One Table

1. Copy Template 1 prompt
2. Paste into Claude Code / Copilot
3. Review generated SQL
4. Save as `stg_ecommerce__shopping_carts.sql` and `stg_ecommerce__shopping_carts_quarantine.sql`
5. Run `dbt compile --select stg_ecommerce__shopping_carts`
6. Fix any compilation errors
7. Run `dbt run --select stg_ecommerce__shopping_carts` (if samples exist)
8. Repeat for next table

With AI assistance, you should complete all 5 remaining tables in **30-60 minutes**.

---

<p align="center">
  <a href="../README.md">🏠 <b>Home</b></a>
  &nbsp;·&nbsp;
  <a href="../RESOURCE_HUB.md">📚 <b>Resource Hub</b></a>
</p>

<p align="center">
  <sub>Last updated: 2026-01-24</sub><br>
  <sub>✨ Transform the data. Tell the story. Build the future. ✨</sub>
</p>
