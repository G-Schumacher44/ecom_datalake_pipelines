# SLAs and Data Quality Checks

## Purpose
Define availability expectations and quality checks for the silver layer.
This is a lightweight SLA/quality spec for the simulated pipeline.

## SLAs (Draft)
- Daily silver availability by 07:00 local time for prior day partitions.
- Pipeline completes within 2 hours for a weekly backfill batch.
- Data quality checks must pass before publishing silver partitions.

## Quality Checks (General)
- Required columns present in each table.
- Primary keys are non-null and unique.
- Foreign keys reference existing parent records.
- Date/time columns parseable; invalid values quarantined or nullified.
- Numeric values that represent counts or prices are non-negative.

## Table-Specific Checks

### orders
- order_id unique and non-null.
- customer_id exists in customers.
- order_date parseable.
- net_total <= gross_total.

### order_items
- (order_id, product_id) unique per partition.
- order_id exists in orders.
- quantity > 0, unit_price >= 0.

### customers
- customer_id unique and non-null.
- email normalized to lowercase; invalid emails flagged.

### product_catalog
- product_id unique and non-null.
- unit_price >= 0, cost_price >= 0.

### shopping_carts
- cart_id unique and non-null.
- customer_id exists in customers.

### cart_items
- cart_item_id unique and non-null.
- cart_id exists in shopping_carts.
- product_id exists in product_catalog.

### returns
- return_id unique and non-null.
- order_id exists in orders.
- customer_id exists in customers.

### return_items
- return_item_id unique and non-null.
- return_id exists in returns.
- order_id exists in orders.
- product_id exists in product_catalog.

## Table-Specific SLAs (Draft)

### orders
- Freshness: silver available by 07:00 daily for prior day partition.
- Completeness: row count within +/- 10% of 7-day average.
- Integrity: `order_id` non-null >= 99.9%; `net_total <= gross_total`.
- FK: `customer_id` present in customers >= 99%.

### order_items
- Freshness: silver available by 07:00 daily for prior day partition.
- Completeness: items per order within +/- 15% of 7-day average.
- Integrity: `quantity > 0`; `unit_price >= 0`.

### customers
- Freshness: silver available by 09:00 daily for prior day partition.
- Completeness: `email` non-null >= 98%.
- Integrity: `customer_id` unique and non-null >= 99.9%.

### product_catalog
- Freshness: silver available by 09:00 daily (or weekly refresh if desired).
- Completeness: row count within +/- 20% of 30-day average.
- Integrity: `unit_price >= 0`, `inventory_quantity >= 0`.

### shopping_carts
- Freshness: silver available by 09:00 daily for prior day partition.
- Completeness: row count within +/- 15% of 7-day average.
- Integrity: `cart_id` unique and non-null >= 99.5%.

### cart_items
- Freshness: silver available by 09:00 daily for prior day partition.
- Completeness: row count within +/- 15% of 7-day average.
- Integrity: `quantity > 0`, `unit_price >= 0`.

### returns
- Freshness: silver available by 10:00 daily for prior day partition.
- Completeness: return rate within +/- 20% of 30-day average.
- Integrity: `return_id` non-null >= 99.5%; `refunded_amount >= 0`.

### return_items
- Freshness: silver available by 10:00 daily for prior day partition.
- Completeness: return items per return within +/- 20% of 30-day average.
- Integrity: `quantity_returned > 0`, `refunded_amount >= 0`.

## Failure Handling
- Fail fast for missing required columns.
- Quarantine rows that fail type parsing or FK checks.
- Emit a summary of rejected rows and sample IDs to audit logs.

## Processing Time SLAs (Table-Level)

### Base Silver (dbt-duckdb)

| Table | Size | Row Count | Target Time | Memory |
| --- | --- | --- | --- | --- |
| orders | ~4GB | 6M | 8-12 min | 4GB |
| order_items | ~3GB | 12M | 6-10 min | 3GB |
| customers | ~500MB | 500K | 2-3 min | 500MB |
| product_catalog | ~50MB | 50K | 30-60 sec | 100MB |
| shopping_carts | ~300MB | 300K | 2-3 min | 300MB |
| cart_items | ~2GB | 3M | 4-6 min | 2GB |
| returns | ~1GB | 1M | 3-5 min | 1GB |
| return_items | ~800MB | 1.5M | 3-4 min | 800MB |

**Total Base Silver**: 30-45 minutes (parallel execution)

**Peak memory**: ~4GB (orders table)

### Enriched Silver (Polars + dbt-bigquery Python)

| Table | Inputs | Target Time | Memory |
| --- | --- | --- | --- |
| int_attributed_purchases | carts, orders | 5-8 min | 4.5GB |
| int_inventory_risk | products, order_items, returns | 6-9 min | 4GB |
| int_customer_retention | customers, orders | 4-6 min | 4.5GB |
| int_sales_velocity | orders, order_items | 8-12 min | 5.5GB |
| int_regional_financials | orders, customers | 5-7 min | 4.5GB |

**Total Enriched Silver**: 30-50 minutes (parallel execution)

**Peak memory**: ~5.5GB (sales_velocity)

### Overall Pipeline SLA

- **Total processing time**: 60-95 minutes (Base + Enriched in sequence)
- **Peak memory usage**: ~6GB (Base Silver orders + Enriched Silver sales_velocity overhead)
- **Temp storage**: <10GB (ephemeral `/tmp/` files deleted after upload)
- **GCS egress cost**: ~$2-3/month for read/write operations

## Monitoring Expectations

- Audit logs are produced per table and partition.
- SLA dashboards consume the audit table in BigQuery.
- Alert when freshness or integrity thresholds are breached.
- Alert when processing time exceeds target by >25%.
