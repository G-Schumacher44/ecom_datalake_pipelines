# Transformation Summary

Purpose-built overview of what each layer transforms and why.

---

## Base Silver (dbt_duckdb)

**Goal:** Clean and conform raw Bronze into trusted, typed tables with quarantine handling.  
**Why:** Every downstream transform depends on consistent keys, timestamps, and numeric types.

Each Base Silver model is 1:1 with a Bronze entity:

- `orders` → clean order facts, enforce valid IDs, timestamps, numeric ranges
- `order_items` → line-level facts, deduped by business keys
- `customers` → customer attributes, typed demographics + signup date
- `product_catalog` → product reference data, typed prices + inventory
- `shopping_carts` → cart sessions, cleaned timestamps and totals
- `cart_items` → cart line items, quantity/unit price checks
- `returns` → return facts, timestamp + ID checks
- `return_items` → return line items, quantity/refund checks

**Key transformations:**
- Normalize IDs and strings
- Parse timestamps, cast numerics, derive business dates (e.g., `order_dt`)
- Deduplicate by business keys (latest record wins)
- Split invalid rows into quarantine with `invalid_reason`

---

## Enriched Silver (Polars)

**Goal:** Join Base Silver entities and compute behavioral/business signals.  
**Why:** Provide domain-ready, interpretable metrics while keeping grain near source.

### Commerce
- `int_attributed_purchases`  
  **Input:** carts + orders  
  **Why:** Link orders to most recent cart to measure recovery and attribution.

- `int_cart_attribution`  
  **Input:** carts + cart_items + orders  
  **Why:** Cart-level conversion/abandonment, time-to-purchase, and lost value.

- `int_product_performance`  
  **Input:** products + order_items + returns + carts  
  **Why:** Profitability, return rate, and cart intent per product per date.

- `int_sales_velocity`  
  **Input:** orders + order_items  
  **Why:** Rolling demand velocity and trend signals for planning.

### Customer
- `int_customer_retention_signals`  
  **Input:** customers + orders  
  **Why:** Churn windows and risk flags for retention actions.

- `int_customer_lifetime_value`  
  **Input:** customers + orders + returns  
  **Why:** CLV buckets and segment snapshots for marketing/finance.

### Finance & Ops
- `int_regional_financials`  
  **Input:** orders + customers  
  **Why:** Regional rollups for finance and ops reporting.

- `int_shipping_economics`  
  **Input:** orders  
  **Why:** Shipping margin and cost efficiency per order.

- `int_inventory_risk`  
  **Input:** products + order_items + returns  
  **Why:** Risk tiers and locked capital from inventory utilization.

- `int_daily_business_metrics`  
  **Input:** orders + carts + returns  
  **Why:** Daily KPI rollup for executive dashboards.

---

## Gold Marts (dbt_bigquery)

**Goal:** Aggregated, domain-friendly marts for BI and analytics.  
**Why:** BI tools need stable, summarized facts instead of granular transforms.

Current marts:

- `fct_finance_revenue`  
  **Sources:** `int_regional_financials`, `int_shipping_economics`  
  **Outputs:** daily gross/net revenue + shipping P&L

- `fct_marketing_attribution`  
  **Sources:** `int_attributed_purchases`, `int_cart_attribution`, `int_customer_retention_signals`  
  **Outputs:** channel recovery, abandonment, time-to-purchase, at-risk counts

- `fct_sales_operations`  
  **Sources:** `int_sales_velocity`, `int_inventory_risk`, `int_product_performance`  
  **Outputs:** velocity, trend, inventory risk, margin/return KPIs

- `fct_cart_abandonment`  
  **Sources:** `int_cart_attribution`  
  **Outputs:** abandonment vs conversion trends by channel

- `fct_product_profitability`  
  **Sources:** `int_product_performance`  
  **Outputs:** profit, margins, return rates per product per date

- `fct_customer_segments`  
  **Sources:** `int_customer_lifetime_value`  
  **Outputs:** CLV segments and averages by bucket

- `fct_daily_dashboard`  
  **Sources:** `int_daily_business_metrics`  
  **Outputs:** daily KPI rollup for exec reporting

- `fct_shipping_analysis`  
  **Sources:** `int_shipping_economics`  
  **Outputs:** shipping margin trends by speed/channel

---

## How to Use This

- **Debugging a transform:** trace Base → Enriched → Gold for the table in question.
- **Adding a new mart:** identify the enriched tables needed and add a dbt model.
- **QA focus:** Base Silver validates data integrity; Enriched validates business logic; Gold validates aggregation logic.
