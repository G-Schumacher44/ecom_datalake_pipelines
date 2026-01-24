# Transformation Summary

## Overview

This document provides a comprehensive overview of all transformations in the medallion lakehouse pipeline, organized by layer (Bronze → Silver → Gold). Each transformation's purpose, inputs, outputs, and business logic are documented to support debugging, extension, and architectural decisions.

**Pipeline Architecture**:

```
Bronze (Raw Parquet)
    ↓
Base Silver (dbt-duckdb) - 8 tables
    ↓
Dimension Snapshots (dbt-duckdb) - 2 tables
    ↓
Enriched Silver (Polars) - 10 transforms
    ↓
Gold Marts (dbt-bigquery) - 8 fact tables
```

**Quick Navigation**:
- [Base Silver (dbt)](#base-silver-dbt-duckdb) - Clean and conform raw data
- [Dimension Snapshots](#dimension-snapshots) - Daily reference data snapshots
- [Enriched Silver (Polars)](#enriched-silver-polars) - Business logic and behavioral signals
- [Gold Marts (dbt)](#gold-marts-dbt-bigquery) - Aggregated BI-ready facts

---

## Base Silver (dbt-duckdb)

**Goal**: Clean and conform raw Bronze into trusted, typed tables with quarantine handling.

**Why**: Every downstream transform depends on consistent keys, timestamps, and numeric types. Base Silver enforces data contracts, validates integrity, and isolates bad data.

**Technology**: dbt-duckdb running on local DuckDB database

**Location**: [dbt_duckdb/models/staging/ecommerce/](../../dbt_duckdb/models/staging/ecommerce/)

### Transformation Pattern

Each Base Silver model follows this pattern:

1. **Source Bronze data** via [sources.yml](../../dbt_duckdb/models/staging/ecommerce/sources.yml)
2. **Type casting** - String timestamps → proper timestamps, numeric strings → decimals
3. **Deduplication** - Latest record wins based on `ingestion_dt` + `batch_id`
4. **Business date derivation** - Extract `order_dt`, `cart_dt`, `product_dt` from timestamps
5. **Primary key validation** - Flag null or duplicate PKs
6. **Foreign key validation** - Check FK existence in parent tables
7. **Quarantine split** - Invalid rows → quarantine table with `invalid_reason`
8. **Add metadata** - Append `ingestion_dt`, `pipeline_run_id`

### Base Silver Tables

#### 1. `stg_ecommerce__orders`

**Source**: [bronze.orders](../../samples/bronze/orders/)

**Primary Key**: `order_id`

**Foreign Keys**:
- `customer_id` → `stg_ecommerce__customers`

**Key Transformations**:
- Parse `order_date` (string) → `order_timestamp` (timestamp)
- Derive `order_dt` (date) for partitioning
- Cast `order_total`, `tax_amount`, `shipping_cost` to decimal
- Validate `order_total >= 0`, `tax_amount >= 0`
- Deduplicate by `order_id` (latest `ingestion_dt` wins)

**Quarantine Rules**:
- `order_id` is null
- `customer_id` is null
- `order_date` fails to parse
- `order_total < 0`

**Output Schema**:
```sql
order_id: string
customer_id: string
order_timestamp: timestamp
order_dt: date
order_total: decimal(10,2)
tax_amount: decimal(10,2)
shipping_cost: decimal(10,2)
order_status: string
ingestion_dt: date
```

---

#### 2. `stg_ecommerce__order_items`

**Source**: [bronze.order_items](../../samples/bronze/order_items/)

**Primary Key**: `item_id`

**Foreign Keys**:
- `order_id` → `stg_ecommerce__orders`
- `product_id` → `stg_ecommerce__product_catalog`

**Key Transformations**:
- Cast `quantity`, `unit_price`, `line_total` to numeric
- Validate `quantity > 0`, `unit_price >= 0`
- Check `line_total = quantity * unit_price` (within tolerance)
- Deduplicate by `item_id`

**Quarantine Rules**:
- `item_id` or `order_id` or `product_id` is null
- `quantity <= 0`
- `line_total` calculation mismatch > 1%

**Output Schema**:
```sql
item_id: string
order_id: string
product_id: string
quantity: int
unit_price: decimal(10,2)
line_total: decimal(10,2)
ingestion_dt: date
```

---

#### 3. `stg_ecommerce__customers`

**Source**: [bronze.customers](../../samples/bronze/customers/)

**Primary Key**: `customer_id`

**Key Transformations**:
- Parse `signup_date` → `signup_timestamp` → `signup_dt`
- Clean `email` (lowercase, trim)
- Cast `tier_id`, `region_id` to int
- Deduplicate by `customer_id` (latest signup wins)

**Quarantine Rules**:
- `customer_id` is null
- `email` is null or invalid format
- `signup_date` fails to parse

**Output Schema**:
```sql
customer_id: string
email: string
first_name: string
last_name: string
signup_timestamp: timestamp
signup_dt: date
tier_id: int
region_id: int
ingestion_dt: date
```

**Note**: Partitioned by `signup_dt` (not `ingestion_dt`)

---

#### 4. `stg_ecommerce__product_catalog`

**Source**: [bronze.product_catalog](../../samples/bronze/product_catalog/)

**Primary Key**: `product_id`

**Key Transformations**:
- Cast `price`, `cost`, `weight` to decimal
- Validate `price > 0`, `cost >= 0`
- Calculate `gross_margin = (price - cost) / price`
- Clean `category`, `subcategory` (trim, lowercase)

**Quarantine Rules**:
- `product_id` is null
- `price <= 0`
- `category` is null

**Output Schema**:
```sql
product_id: string
product_name: string
category: string
subcategory: string
price: decimal(10,2)
cost: decimal(10,2)
weight: decimal(8,2)
gross_margin: decimal(5,4)
ingestion_dt: date
```

**Note**: Reference data - partitioned by `category` in Bronze, by `ingestion_dt` in Silver

---

#### 5. `stg_ecommerce__shopping_carts`

**Source**: [bronze.shopping_carts](../../samples/bronze/shopping_carts/)

**Primary Key**: `cart_id`

**Foreign Keys**:
- `customer_id` → `stg_ecommerce__customers`

**Key Transformations**:
- Parse `cart_created_at` → `cart_timestamp` → `cart_dt`
- Parse `last_updated_at` → `last_updated_timestamp`
- Cast `cart_value` to decimal
- Validate `cart_value >= 0`

**Quarantine Rules**:
- `cart_id` or `customer_id` is null
- `cart_created_at` fails to parse
- `cart_value < 0`

**Output Schema**:
```sql
cart_id: string
customer_id: string
cart_timestamp: timestamp
cart_dt: date
last_updated_timestamp: timestamp
cart_value: decimal(10,2)
cart_status: string
ingestion_dt: date
```

---

#### 6. `stg_ecommerce__cart_items`

**Source**: [bronze.cart_items](../../samples/bronze/cart_items/)

**Primary Key**: `cart_item_id`

**Foreign Keys**:
- `cart_id` → `stg_ecommerce__shopping_carts`
- `product_id` → `stg_ecommerce__product_catalog`

**Key Transformations**:
- Cast `quantity`, `unit_price` to numeric
- Validate `quantity > 0`, `unit_price >= 0`

**Quarantine Rules**:
- `cart_item_id`, `cart_id`, or `product_id` is null
- `quantity <= 0`

**Output Schema**:
```sql
cart_item_id: string
cart_id: string
product_id: string
quantity: int
unit_price: decimal(10,2)
ingestion_dt: date
```

---

#### 7. `stg_ecommerce__returns`

**Source**: [bronze.returns](../../samples/bronze/returns/)

**Primary Key**: `return_id`

**Foreign Keys**:
- `order_id` → `stg_ecommerce__orders`

**Key Transformations**:
- Parse `return_date` → `return_timestamp` → `return_dt`
- Cast `refund_amount` to decimal
- Validate `refund_amount >= 0`

**Quarantine Rules**:
- `return_id` or `order_id` is null
- `return_date` fails to parse
- `refund_amount < 0`

**Output Schema**:
```sql
return_id: string
order_id: string
return_timestamp: timestamp
return_dt: date
refund_amount: decimal(10,2)
return_reason: string
ingestion_dt: date
```

**Note**: Can have zero rows per partition (returns are optional)

---

#### 8. `stg_ecommerce__return_items`

**Source**: [bronze.return_items](../../samples/bronze/return_items/)

**Primary Key**: `return_item_id`

**Foreign Keys**:
- `return_id` → `stg_ecommerce__returns`
- `product_id` → `stg_ecommerce__product_catalog`

**Key Transformations**:
- Cast `quantity_returned`, `refund_per_item` to numeric
- Validate `quantity_returned > 0`, `refund_per_item >= 0`

**Quarantine Rules**:
- `return_item_id`, `return_id`, or `product_id` is null
- `quantity_returned <= 0`

**Output Schema**:
```sql
return_item_id: string
return_id: string
product_id: string
quantity_returned: int
refund_per_item: decimal(10,2)
ingestion_dt: date
```

---

### Base Silver Summary

| Table | Bronze Source | PK | FKs | Partition Key | Allow Empty |
|-------|---------------|----|----|---------------|-------------|
| `stg_ecommerce__orders` | `orders` | `order_id` | `customer_id` | `ingestion_dt` | No |
| `stg_ecommerce__order_items` | `order_items` | `item_id` | `order_id`, `product_id` | `ingestion_dt` | No |
| `stg_ecommerce__customers` | `customers` | `customer_id` | None | `signup_dt` | No |
| `stg_ecommerce__product_catalog` | `product_catalog` | `product_id` | None | `ingestion_dt` | No |
| `stg_ecommerce__shopping_carts` | `shopping_carts` | `cart_id` | `customer_id` | `ingestion_dt` | No |
| `stg_ecommerce__cart_items` | `cart_items` | `cart_item_id` | `cart_id`, `product_id` | `ingestion_dt` | No |
| `stg_ecommerce__returns` | `returns` | `return_id` | `order_id` | `ingestion_dt` | Yes |
| `stg_ecommerce__return_items` | `return_items` | `return_item_id` | `return_id`, `product_id` | `ingestion_dt` | Yes |

---

## Dimension Snapshots

**Goal**: Create daily snapshots of slowly changing dimensions to avoid re-reading Bronze for every enriched transform.

**Why**: 60% reduction in Bronze reads, faster DAG execution, prevents stale dimension joins.

**Technology**: dbt-duckdb with custom snapshot runner

**Location**: [src/runners/dims_snapshot.py](../../src/runners/dims_snapshot.py)

### Snapshot Pattern

1. **Freshness gate** - Check `_latest.json` pointer
2. **Read Base Silver** - Load today's partition from `stg_ecommerce__customers` or `stg_ecommerce__product_catalog`
3. **Write snapshot** - Create `snapshot_dt=YYYY-MM-DD/snapshot.parquet`
4. **Update pointer** - Write `_latest.json` with current snapshot date
5. **Validate** - Run PK integrity checks

### Dimension Tables

#### 1. `dims/customers`

**Source**: [data/silver/base/customers](../../data/silver/base/customers/)

**Snapshot Frequency**: Daily

**Structure**:
```
data/silver/dims/customers/
  snapshot_dt=2025-10-15/
    snapshot.parquet
    _MANIFEST.json
  snapshot_dt=2025-10-16/
    snapshot.parquet
    _MANIFEST.json
  _latest.json  # {"customers": "2025-10-16"}
```

**Schema**: Same as `stg_ecommerce__customers` plus `snapshot_dt`

---

#### 2. `dims/product_catalog`

**Source**: [data/silver/base/product_catalog](../../data/silver/base/product_catalog/)

**Snapshot Frequency**: Daily

**Structure**:
```
data/silver/dims/product_catalog/
  snapshot_dt=2025-10-15/
    snapshot.parquet
    _MANIFEST.json
  _latest.json  # {"product_catalog": "2025-10-15"}
```

**Schema**: Same as `stg_ecommerce__product_catalog` plus `snapshot_dt`

---

## Enriched Silver (Polars)

**Goal**: Join Base Silver entities and compute behavioral/business signals using Polars for fast, memory-efficient processing.

**Why**: Provide domain-ready, interpretable metrics while keeping grain near source. Polars enables complex transformations with lazy evaluation and columnar optimizations.

**Technology**: Polars (lazy evaluation) with Parquet I/O

**Location**:
- **Transforms**: [src/transforms/](../../src/transforms/)
- **Runners**: [src/runners/enriched/](../../src/runners/enriched/)

### Transformation Pattern

Each enriched transform follows this pattern:

1. **Lazy load** Base Silver tables via `pl.scan_parquet()`
2. **Apply business logic** using Polars expressions
3. **Compute metrics** (aggregations, window functions, joins)
4. **Validate results** (sanity checks, semantic checks)
5. **Write output** partitioned Parquet with manifest

### Enriched Transforms

#### 1. `int_attributed_purchases`

**Transform**: [src/transforms/attributed_purchases.py](../../src/transforms/attributed_purchases.py)

**Runner**: [src/runners/enriched/attributed_purchases.py](../../src/runners/enriched/attributed_purchases.py)

**Inputs**:
- `shopping_carts` (Base Silver)
- `orders` (Base Silver)

**Business Logic**:

Links each order to its most recent cart session within a configurable attribution window (default: 48 hours). Enables analysis of cart recovery patterns and channel attribution.

**Key Calculations**:
- `time_since_cart_created` - Hours between cart creation and order placement
- `attributed_cart_id` - Most recent cart within attribution window
- `attribution_confidence` - High/Medium/Low based on time gap

**Partition Key**: `order_dt`

**Output Schema**:
```
order_id: string
customer_id: string
order_dt: date
attributed_cart_id: string (nullable)
time_since_cart_created: float (nullable)
attribution_confidence: string
ingestion_dt: date
```

**Validation**:
- All orders have non-null `order_id`, `customer_id`
- `time_since_cart_created` <= 48 hours (if attributed)
- `attribution_confidence` in {High, Medium, Low, None}

---

#### 2. `int_cart_attribution`

**Transform**: [src/transforms/cart_attribution.py](../../src/transforms/cart_attribution.py)

**Runner**: [src/runners/enriched/cart_attribution.py](../../src/runners/enriched/cart_attribution.py)

**Inputs**:
- `shopping_carts` (Base Silver)
- `cart_items` (Base Silver)
- `orders` (Base Silver)

**Business Logic**:

Cart-level conversion/abandonment analysis. Flags abandoned carts, calculates lost value, and measures time-to-purchase for converted carts.

**Key Calculations**:
- `cart_status` - 'converted', 'abandoned', 'active'
- `time_to_purchase_hours` - Hours from cart creation to order (if converted)
- `abandoned_value` - Cart value if abandoned
- `item_count` - Number of items in cart

**Partition Key**: `cart_dt`

**Output Schema**:
```
cart_id: string
customer_id: string
cart_dt: date
cart_status: string
order_id: string (nullable)
time_to_purchase_hours: float (nullable)
cart_value: decimal(10,2)
abandoned_value: decimal(10,2) (nullable)
item_count: int
ingestion_dt: date
```

**Semantic Checks**:
- `cart_status = 'converted'` → `order_id` must be non-null
- `cart_status = 'abandoned'` → `order_id` must be null
- `cart_status = 'abandoned'` → `abandoned_value` must equal `cart_value`
- `time_to_purchase_hours >= 0`

---

#### 3. `int_product_performance`

**Transform**: [src/transforms/product_performance.py](../../src/transforms/product_performance.py)

**Runner**: [src/runners/enriched/product_performance.py](../../src/runners/enriched/product_performance.py)

**Inputs**:
- `product_catalog` (Dimension snapshot)
- `order_items` (Base Silver)
- `return_items` (Base Silver)
- `cart_items` (Base Silver)

**Business Logic**:

Product-level profitability, return rate, and cart intent signals per business date.

**Key Calculations**:
- `units_sold` - Total quantity ordered
- `units_returned` - Total quantity returned
- `return_rate` - `units_returned / units_sold`
- `gross_revenue` - Total revenue before returns
- `net_revenue` - Revenue after returns
- `gross_margin` - `(price - cost) / price`
- `net_margin` - Margin after returns
- `cart_to_order_rate` - Conversion rate from cart adds to purchases
- `units_in_carts` - Total quantity in active carts

**Partition Key**: `product_dt`

**Output Schema**:
```
product_id: string
product_dt: date
units_sold: int
units_returned: int
return_rate: decimal(5,4)
gross_revenue: decimal(12,2)
net_revenue: decimal(12,2)
gross_margin: decimal(5,4)
net_margin: decimal(5,4)
cart_to_order_rate: decimal(5,4)
units_in_carts: int
ingestion_dt: date
```

**Semantic Checks**:
- `return_rate <= 1.0`
- `cart_to_order_rate <= 1.0`
- `net_margin <= gross_margin`
- `units_returned <= units_sold * 2.0` (tolerance for data errors)

---

#### 4. `int_sales_velocity`

**Transform**: [src/transforms/sales_velocity.py](../../src/transforms/sales_velocity.py)

**Runner**: [src/runners/enriched/sales_velocity.py](../../src/runners/enriched/sales_velocity.py)

**Inputs**:
- `orders` (Base Silver)
- `order_items` (Base Silver)

**Business Logic**:

Rolling 7-day demand velocity and trend signals for inventory planning.

**Key Calculations**:
- `units_sold_7d` - Units sold in trailing 7 days
- `avg_daily_velocity` - Average units per day over 7-day window
- `velocity_trend` - 'Accelerating', 'Stable', 'Declining'
- `revenue_7d` - Revenue in trailing 7 days

**Partition Key**: `order_dt`

**Lookback**: Requires 7 days of historical data (controlled by `enriched_lookback_days`)

**Output Schema**:
```
product_id: string
order_dt: date
units_sold_7d: int
avg_daily_velocity: decimal(8,2)
velocity_trend: string
revenue_7d: decimal(12,2)
ingestion_dt: date
```

**Validation**:
- `units_sold_7d >= 0`
- `velocity_trend` in {'Accelerating', 'Stable', 'Declining'}

---

#### 5. `int_customer_retention_signals`

**Transform**: [src/transforms/customer_retention.py](../../src/transforms/customer_retention.py)

**Runner**: [src/runners/enriched/customer_retention.py](../../src/runners/enriched/customer_retention.py)

**Inputs**:
- `customers` (Dimension snapshot)
- `orders` (Base Silver)

**Business Logic**:

Churn risk flags and engagement scores based on recency of last order.

**Key Calculations**:
- `days_since_last_order` - Days since most recent order
- `churn_risk_30d` - Boolean flag if > 30 days since last order
- `churn_risk_90d` - Boolean flag if > 90 days since last order
- `order_count_lifetime` - Total orders to date
- `engagement_score` - 1-5 score based on recency and frequency

**Partition Key**: `ingest_dt`

**Output Schema**:
```
customer_id: string
ingest_dt: date
days_since_last_order: int (nullable)
churn_risk_30d: boolean
churn_risk_90d: boolean
order_count_lifetime: int
engagement_score: int
ingestion_dt: date
```

**Validation**:
- `engagement_score` in {1, 2, 3, 4, 5}
- `churn_risk_90d = true` → `churn_risk_30d = true`

---

#### 6. `int_customer_lifetime_value`

**Transform**: [src/transforms/customer_ltv.py](../../src/transforms/customer_ltv.py)

**Runner**: [src/runners/enriched/customer_ltv.py](../../src/runners/enriched/customer_ltv.py)

**Inputs**:
- `customers` (Dimension snapshot)
- `orders` (Base Silver)
- `returns` (Base Silver)

**Business Logic**:

CLV calculation with segment bucketing for marketing and finance.

**Key Calculations**:
- `total_spent` - Lifetime gross revenue
- `total_refunded` - Lifetime refund amount
- `net_clv` - `total_spent - total_refunded`
- `clv_bucket` - 'High', 'Medium', 'Low' based on thresholds
- `order_count` - Lifetime order count
- `avg_order_value` - `total_spent / order_count`

**Partition Key**: `ingest_dt`

**Output Schema**:
```
customer_id: string
ingest_dt: date
total_spent: decimal(12,2)
total_refunded: decimal(12,2)
net_clv: decimal(12,2)
clv_bucket: string
order_count: int
avg_order_value: decimal(10,2)
ingestion_dt: date
```

**Semantic Checks**:
- `net_clv = total_spent - total_refunded` (within $0.01 tolerance)
- `clv_bucket` in {'High', 'Medium', 'Low'}

---

#### 7. `int_regional_financials`

**Transform**: [src/transforms/regional_financials.py](../../src/transforms/regional_financials.py)

**Runner**: [src/runners/enriched/regional_financials.py](../../src/runners/enriched/regional_financials.py)

**Inputs**:
- `orders` (Base Silver)
- `customers` (Dimension snapshot)

**Business Logic**:

Regional revenue rollups for finance and ops reporting.

**Key Calculations**:
- `gross_revenue` - Total order revenue by region
- `order_count` - Number of orders by region
- `avg_order_value` - Average order size by region

**Partition Key**: `order_dt`

**Output Schema**:
```
region_id: int
order_dt: date
gross_revenue: decimal(12,2)
order_count: int
avg_order_value: decimal(10,2)
ingestion_dt: date
```

**Validation**:
- `gross_revenue >= 0`
- `order_count > 0`

---

#### 8. `int_shipping_economics`

**Transform**: [src/transforms/shipping_economics.py](../../src/transforms/shipping_economics.py)

**Runner**: [src/runners/enriched/shipping_economics.py](../../src/runners/enriched/shipping_economics.py)

**Inputs**:
- `orders` (Base Silver)

**Business Logic**:

Shipping margin and cost efficiency per order.

**Key Calculations**:
- `shipping_cost` - Charged to customer
- `actual_shipping_cost` - Incurred by business
- `shipping_margin` - `shipping_cost - actual_shipping_cost`
- `shipping_margin_pct` - `shipping_margin / shipping_cost`

**Partition Key**: `order_dt`

**Output Schema**:
```
order_id: string
order_dt: date
shipping_cost: decimal(10,2)
actual_shipping_cost: decimal(10,2)
shipping_margin: decimal(10,2)
shipping_margin_pct: decimal(5,4)
ingestion_dt: date
```

**Semantic Checks**:
- `shipping_margin = shipping_cost - actual_shipping_cost` (within $0.01)
- `shipping_margin_pct` is null when `shipping_cost = 0`

---

#### 9. `int_inventory_risk`

**Transform**: [src/transforms/inventory_risk.py](../../src/transforms/inventory_risk.py)

**Runner**: [src/runners/enriched/inventory_risk.py](../../src/runners/enriched/inventory_risk.py)

**Inputs**:
- `product_catalog` (Dimension snapshot)
- `order_items` (Base Silver)
- `return_items` (Base Silver)

**Business Logic**:

Risk tiers and locked capital from inventory utilization.

**Key Calculations**:
- `units_available` - On-hand inventory
- `units_sold_30d` - Sales velocity
- `inventory_turnover_ratio` - `units_sold_30d / units_available`
- `risk_tier` - 'High', 'Medium', 'Low' based on turnover
- `locked_capital` - `units_available * cost`

**Partition Key**: `ingest_dt`

**Output Schema**:
```
product_id: string
ingest_dt: date
units_available: int
units_sold_30d: int
inventory_turnover_ratio: decimal(5,2)
risk_tier: string
locked_capital: decimal(12,2)
ingestion_dt: date
```

**Validation**:
- `risk_tier` in {'High', 'Medium', 'Low'}
- `locked_capital >= 0`

---

#### 10. `int_daily_business_metrics`

**Transform**: [src/transforms/daily_metrics.py](../../src/transforms/daily_metrics.py)

**Runner**: [src/runners/enriched/daily_metrics.py](../../src/runners/enriched/daily_metrics.py)

**Inputs**:
- `orders` (Base Silver)
- `returns` (Base Silver)
- `shopping_carts` (Base Silver)

**Business Logic**:

Daily KPI rollup for executive dashboards.

**Key Calculations**:
- `orders_count` - Total orders
- `gross_revenue` - Total order revenue
- `returns_count` - Total returns
- `refund_total` - Total refund amount
- `return_rate` - `returns_count / orders_count`
- `carts_created` - Total carts created
- `cart_conversion_rate` - `orders_count / carts_created`

**Partition Key**: `date`

**Output Schema**:
```
date: date
orders_count: int
gross_revenue: decimal(12,2)
returns_count: int
refund_total: decimal(12,2)
return_rate: decimal(5,4)
carts_created: int
cart_conversion_rate: decimal(5,4)
ingestion_dt: date
```

**Semantic Checks**:
- `return_rate = returns_count / orders_count` (within epsilon)
- `cart_conversion_rate = orders_count / carts_created` (within epsilon)

---

### Enriched Silver Summary

| Transform | Partition Key | Inputs | Business Domain |
|-----------|---------------|--------|-----------------|
| `int_attributed_purchases` | `order_dt` | carts, orders | Commerce |
| `int_cart_attribution` | `cart_dt` | carts, cart_items, orders | Commerce |
| `int_product_performance` | `product_dt` | products, order_items, return_items, cart_items | Commerce |
| `int_sales_velocity` | `order_dt` | orders, order_items | Commerce |
| `int_customer_retention_signals` | `ingest_dt` | customers, orders | Customer |
| `int_customer_lifetime_value` | `ingest_dt` | customers, orders, returns | Customer |
| `int_regional_financials` | `order_dt` | orders, customers | Finance & Ops |
| `int_shipping_economics` | `order_dt` | orders | Finance & Ops |
| `int_inventory_risk` | `ingest_dt` | products, order_items, return_items | Finance & Ops |
| `int_daily_business_metrics` | `date` | orders, returns, carts | Executive |

---

## Gold Marts (dbt-bigquery)

**Goal**: Aggregated, domain-friendly marts for BI and analytics.

**Why**: BI tools need stable, summarized facts instead of granular transforms. Gold marts provide pre-aggregated, denormalized tables optimized for query performance.

**Technology**: dbt-bigquery running on BigQuery warehouse

**Location**: [dbt_bigquery/models/marts/](../../dbt_bigquery/models/marts/)

### Mart Pattern

Each Gold mart follows this pattern:

1. **Source Enriched Silver** - Read from BigQuery Silver dataset
2. **Aggregate** - Group by time/dimension keys
3. **Join dimensions** - Enrich with dimension attributes
4. **Calculate KPIs** - Compute business metrics
5. **Materialize** - Write to Gold dataset with partitioning/clustering

### Gold Mart Tables

#### 1. `fct_finance_revenue`

**Sources**:
- `int_regional_financials` (Enriched Silver)
- `int_shipping_economics` (Enriched Silver)

**Grain**: Daily revenue by region

**Key Metrics**:
- Gross revenue
- Net revenue (after returns)
- Shipping P&L
- Average order value

**Partition**: `order_dt`

**Cluster**: `region_id`

**Business Use**: Finance dashboards, regional performance tracking

---

#### 2. `fct_marketing_attribution`

**Sources**:
- `int_attributed_purchases` (Enriched Silver)
- `int_cart_attribution` (Enriched Silver)
- `int_customer_retention_signals` (Enriched Silver)

**Grain**: Daily attribution metrics

**Key Metrics**:
- Cart recovery rate
- Abandonment value
- Average time-to-purchase
- At-risk customer count

**Partition**: `date`

**Business Use**: Marketing campaign analysis, funnel optimization

---

#### 3. `fct_sales_operations`

**Sources**:
- `int_sales_velocity` (Enriched Silver)
- `int_inventory_risk` (Enriched Silver)
- `int_product_performance` (Enriched Silver)

**Grain**: Daily product operations metrics

**Key Metrics**:
- Velocity trends
- Inventory risk tiers
- Margin and return KPIs
- Stock-out predictions

**Partition**: `date`

**Cluster**: `product_id`

**Business Use**: Inventory planning, pricing strategy

---

#### 4. `fct_cart_abandonment`

**Sources**:
- `int_cart_attribution` (Enriched Silver)

**Grain**: Daily cart abandonment vs conversion trends

**Key Metrics**:
- Abandonment rate by channel
- Lost cart value
- Average items per abandoned cart

**Partition**: `cart_dt`

**Business Use**: Remarketing campaigns, checkout optimization

---

#### 5. `fct_product_profitability`

**Sources**:
- `int_product_performance` (Enriched Silver)

**Grain**: Daily profitability by product

**Key Metrics**:
- Gross margin %
- Net margin %
- Return rate
- Units sold vs returned

**Partition**: `product_dt`

**Cluster**: `product_id`

**Business Use**: Product line optimization, pricing decisions

---

#### 6. `fct_customer_segments`

**Sources**:
- `int_customer_lifetime_value` (Enriched Silver)

**Grain**: Daily customer segments by CLV bucket

**Key Metrics**:
- Average CLV by segment
- Customer count by bucket
- Average order value by segment

**Partition**: `ingest_dt`

**Business Use**: Customer segmentation, loyalty programs

---

#### 7. `fct_daily_dashboard`

**Sources**:
- `int_daily_business_metrics` (Enriched Silver)

**Grain**: Daily executive KPI rollup

**Key Metrics**:
- Orders, revenue, returns
- Cart conversion rate
- Return rate

**Partition**: `date`

**Business Use**: Executive dashboards, daily reporting

---

#### 8. `fct_shipping_analysis`

**Sources**:
- `int_shipping_economics` (Enriched Silver)

**Grain**: Daily shipping metrics by speed/channel

**Key Metrics**:
- Shipping margin %
- Cost per shipment
- Margin by shipping method

**Partition**: `order_dt`

**Business Use**: Logistics optimization, carrier negotiations

---

### Gold Marts Summary

| Mart | Enriched Sources | Partition | Business Domain |
|------|------------------|-----------|-----------------|
| `fct_finance_revenue` | regional_financials, shipping_economics | `order_dt` | Finance |
| `fct_marketing_attribution` | attributed_purchases, cart_attribution, retention_signals | `date` | Marketing |
| `fct_sales_operations` | sales_velocity, inventory_risk, product_performance | `date` | Sales Ops |
| `fct_cart_abandonment` | cart_attribution | `cart_dt` | Commerce |
| `fct_product_profitability` | product_performance | `product_dt` | Product |
| `fct_customer_segments` | customer_lifetime_value | `ingest_dt` | Customer |
| `fct_daily_dashboard` | daily_business_metrics | `date` | Executive |
| `fct_shipping_analysis` | shipping_economics | `order_dt` | Logistics |

---

## How to Use This Document

### For Debugging

**Trace data lineage**: Follow transform chain for the table in question

```
Bronze → Base Silver → Enriched → Gold
```

**Example**: Debug cart conversion metrics

1. Check Bronze `shopping_carts` partition exists
2. Validate Base Silver `stg_ecommerce__shopping_carts` row counts
3. Review Enriched `int_cart_attribution` logic
4. Verify Gold `fct_cart_abandonment` aggregations

### For Adding New Transforms

**Enriched Silver**:

1. Identify inputs from Base Silver or Dims
2. Define business logic in [src/transforms/](../../src/transforms/)
3. Create runner in [src/runners/enriched/](../../src/runners/enriched/)
4. Add table spec to [config/specs/enriched.yml](../../config/specs/enriched.yml)
5. Update this document with transform details

**Gold Marts**:

1. Identify Enriched Silver sources
2. Define aggregation grain and KPIs
3. Create dbt model in [dbt_bigquery/models/marts/](../../dbt_bigquery/models/marts/)
4. Add partitioning/clustering strategy
5. Update this document with mart details

### For QA Focus

**Layer-specific validation**:

- **Base Silver**: Validates data integrity (PK/FK, types, ranges)
- **Enriched Silver**: Validates business logic (semantic checks, sanity checks)
- **Gold Marts**: Validates aggregation logic (totals, averages, counts)

**Use validation modules**:

```bash
# Base Silver validation
python -m src.validation.silver --partition-date 2025-10-15

# Enriched Silver validation
python -m src.validation.enriched --ingest-dt 2025-10-15
```

---

## Related Documentation

- **[Architecture Overview](ARCHITECTURE.md)** - Complete system architecture with data flow
- **[Spec Overview](SPEC_OVERVIEW.md)** - Spec-driven orchestration and table metadata
- **[Data Contract](DATA_CONTRACT.md)** - Bronze → Silver type mappings
- **[Validation Guide](VALIDATION_GUIDE.md)** - Three-layer validation framework
- **[CLI Usage Guide](CLI_USAGE_GUIDE.md)** - Running transforms and validation

---

**Last Updated**: 2026-01-23
**Base Silver Tables**: 8 (dbt-duckdb)
**Dimension Snapshots**: 2 (customers, products)
**Enriched Transforms**: 10 (Polars)
**Gold Marts**: 8 (dbt-bigquery)

---

<p align="center">
  <a href="../../README.md">🏠 <b>Home</b></a>
  &nbsp;·&nbsp;
  <a href="../../RESOURCE_HUB.md">📚 <b>Resource Hub</b></a>
</p>

<p align="center">
  <sub>Last updated: 2026-01-24</sub><br>
  <sub>✨ Transform the data. Tell the story. Build the future. ✨</sub>
</p>
