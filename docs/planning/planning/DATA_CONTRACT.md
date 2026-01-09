# Data Contract (Bronze -> Base Silver, Enriched Silver)

## Purpose
Define the expected schema, required fields, and change policy for silver tables.
This is the single source of truth for required fields, data types, and nullability.

## Scope
Base Silver tables map 1:1 with bronze tables. No split/merge.
Enriched Silver introduces business-aligned tables derived from Base Silver.
See `docs/planning/planning/BRONZE_SCHEMA_SAMPLE.md` for current bronze schemas.

## Schema Versioning
- Current version: v1
- Additive changes (new nullable columns) -> minor version bump.
- Breaking changes (type changes, removed columns, renamed columns) -> major version bump.
- All schema changes must be documented in this file.

## Lineage Columns (Base Silver Only)
All Base Silver tables must include these columns (not null):
- batch_id (string)
- ingestion_ts (timestamp)
- event_id (string)
- source_file (string)

## Base Silver Tables (Required Fields)
Columns not listed below are optional and preserved as-is.

### orders
- order_id (string, not null)
- customer_id (string, not null)
- order_date (timestamp, not null)
- net_total (float64, not null)
- gross_total (float64, not null)

### order_items
- order_id (string, not null)
- product_id (int64, not null)
- quantity (int64, not null)
- unit_price (float64, not null)

### customers
- customer_id (string, not null)
- email (string, not null)
- signup_date (date, not null)

### product_catalog
- product_id (int64, not null)
- product_name (string, not null)
- unit_price (float64, not null)

### shopping_carts
- cart_id (string, not null)
- customer_id (string, not null)
- created_at (timestamp, not null)

### cart_items
- cart_item_id (int64, not null)
- cart_id (string, not null)
- product_id (int64, not null)
- quantity (int64, not null)
- unit_price (float64, not null)

### returns
- return_id (string, not null)
- order_id (string, not null)
- customer_id (string, not null)
- return_date (timestamp, not null)

### return_items
- return_item_id (int64, not null)
- return_id (string, not null)
- order_id (string, not null)
- product_id (int64, not null)
- quantity_returned (int64, not null)

## Enriched Silver Tables (Required Fields)
Enriched Silver tables must include stable business keys and event timestamps.

### int_attributed_purchases
- order_id (string, not null)
- customer_id (string, not null)
- order_date (timestamp, not null)
- cart_id (string, nullable)
- is_recovered (bool, not null)

### int_inventory_risk
- product_id (int64, not null)
- attention_score (float64, not null)
- risk_tier (string, not null)
- locked_capital (float64, not null)

### int_customer_retention_signals
- customer_id (string, not null)
- days_since_first_buy (int64, not null)
- days_since_last_buy (int64, not null)
- is_in_danger_zone (bool, not null)
- needs_bronze_nudge (bool, not null)

### int_sales_velocity
- product_id (int64, not null)
- order_date (timestamp, not null)
- velocity_avg (float64, not null)
- trend_signal (string, not null)

### int_regional_financials
- order_id (string, not null)
- region (string, not null)
- total_price (float64, not null)
- tax_amount (float64, not null)
- net_revenue (float64, not null)

## Change Policy
- Upstream changes must be communicated before deployment.
- Silver transforms will reject partitions with missing required columns.
- If a breaking change is detected, publish a new schema version and update downstream consumers.

## Contract Enforcement
- Base Silver validation uses Pydantic schemas and dbt tests.
- Enriched Silver validation uses unit tests for Polars transforms and dbt tests for output constraints.
- Contract violations block downstream publish steps.

## Ownership
- Drafted by: Data Engineering
- Reviewers: Business Intelligence, Data Science, Platform Engineering
