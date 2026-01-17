# Enriched Silver Quality Report

**Last Updated:** 2026-01-17 05:11:10 UTC
**Run ID:** `manual__2025-10-03T00:00:00+00:00`
**Overall Status:** ⚠️ WARN

## Summary

| Table | Partition | Value | Rows | Min Rows | Δ vs prior | Status | Notes |
|-------|-----------|-------|------|----------|-----------|--------|-------|
| int_attributed_purchases | order_dt | 2025-10-03 | 727 | 1 | +14.49% | ✅ PASS | - |
| int_cart_attribution | cart_dt | 2025-10-03 | 5,241 | 1 | +3.97% | ⚠️ WARN | semantic_checks_failed |
| int_inventory_risk | ingest_dt | 2025-10-03 | 6,000 | 1 | +100.00% | ✅ PASS | - |
| int_customer_retention_signals | ingest_dt | 2025-10-03 | 6,412 | 1 | -19.37% | ✅ PASS | - |
| int_customer_lifetime_value | ingest_dt | 2025-10-03 | 6,412 | 1 | -19.37% | ✅ PASS | - |
| int_daily_business_metrics | date | 2025-10-03 | 1 | 1 | +0.00% | ⚠️ WARN | semantic_checks_failed |
| int_product_performance | product_dt | 2025-10-03 | 5,990 | 1 | +99.80% | ⚠️ WARN | sanity_checks_failed, semantic_checks_failed |
| int_sales_velocity | order_dt | 2025-10-03 | 2,032 | 1 | +4.21% | ✅ PASS | - |
| int_regional_financials | order_dt | 2025-10-03 | 727 | 1 | +14.49% | ✅ PASS | - |
| int_shipping_economics | order_dt | 2025-10-03 | 727 | 1 | +14.49% | ✅ PASS | - |

---

## Issues Detected

### ⚠️ int_cart_attribution
- **Partition:** cart_dt=2025-10-03
- **Rows:** 5,241
- **Min Rows:** 1
- **Row Delta vs prior:** +3.97%
- **Notes:** semantic_checks_failed

### ⚠️ int_daily_business_metrics
- **Partition:** date=2025-10-03
- **Rows:** 1
- **Min Rows:** 1
- **Row Delta vs prior:** +0.00%
- **Notes:** semantic_checks_failed

### ⚠️ int_product_performance
- **Partition:** product_dt=2025-10-03
- **Rows:** 5,990
- **Min Rows:** 1
- **Row Delta vs prior:** +99.80%
- **Notes:** sanity_checks_failed, semantic_checks_failed

---

## Schema & Null Rates

### int_attributed_purchases

**Schema (column → dtype):**

| Column | Dtype |
|--------|-------|
| order_id | large_string |
| total_items | int64 |
| order_date | timestamp[us] |
| customer_id | large_string |
| email | large_string |
| order_channel | large_string |
| is_expedited | bool |
| customer_tier | large_string |
| gross_total | decimal128(18, 2) |
| net_total | decimal128(18, 2) |
| total_discount_amount | decimal128(18, 2) |
| payment_method | large_string |
| shipping_speed | large_string |
| shipping_cost | decimal128(18, 2) |
| agent_id | large_string |
| actual_shipping_cost | decimal128(18, 2) |
| payment_processing_fee | decimal128(18, 2) |
| shipping_address | large_string |
| billing_address | large_string |
| clv_bucket | large_string |
| is_reactivated | bool |
| batch_id | large_string |
| ingestion_ts | timestamp[us] |
| ingestion_dt | date32[day] |
| event_id | large_string |
| source_file | large_string |
| order_dt | large_string |
| event_ts | timestamp[us] |
| cart_id | large_string |
| created_at | timestamp[us] |
| updated_at | timestamp[us] |
| cart_total | decimal128(18, 2) |
| status | large_string |
| batch_id_right | large_string |
| ingestion_ts_right | timestamp[us] |
| ingestion_dt_right | date32[day] |
| event_id_right | large_string |
| source_file_right | large_string |
| created_dt | date32[day] |
| is_recovered | bool |
| ingest_dt | large_string |

**Key Field Null Rates:**

| Column | Null Rate |
|--------|----------|
| order_id | 0.00% |
| customer_id | 0.00% |
| cart_id | 0.00% |

### int_cart_attribution

**Schema (column → dtype):**

| Column | Dtype |
|--------|-------|
| cart_id | large_string |
| customer_id | large_string |
| created_at | timestamp[us] |
| updated_at | timestamp[us] |
| cart_value | decimal128(38, 2) |
| item_count | uint32 |
| category_count | uint32 |
| cart_status | large_string |
| time_to_purchase_hours | int64 |
| order_id | large_string |
| order_date | timestamp[us] |
| order_channel | large_string |
| abandoned_value | double |
| cart_dt | large_string |
| ingest_dt | large_string |

**Key Field Null Rates:**

| Column | Null Rate |
|--------|----------|
| cart_id | 0.00% |
| customer_id | 0.00% |

**Semantic Issues:** abandoned_value_positive_for_abandoned: 652 rows

### int_inventory_risk

**Schema (column → dtype):**

| Column | Dtype |
|--------|-------|
| product_id | int64 |
| product_name | large_string |
| unit_price | decimal128(18, 2) |
| cost_price | decimal128(38, 2) |
| inventory_quantity | int64 |
| batch_id | large_string |
| ingestion_ts | timestamp[us] |
| ingestion_dt | date32[day] |
| event_id | large_string |
| source_file | large_string |
| category | large_string |
| sales_volume | int64 |
| return_volume | int64 |
| utilization_ratio | double |
| return_signal | double |
| locked_capital | decimal128(38, 2) |
| attention_score | double |
| risk_tier | large_string |
| ingest_dt | large_string |

**Key Field Null Rates:**

| Column | Null Rate |
|--------|----------|
| product_id | 0.00% |

### int_customer_retention_signals

**Schema (column → dtype):**

| Column | Dtype |
|--------|-------|
| customer_id | large_string |
| email | large_string |
| signup_date | date32[day] |
| first_name | large_string |
| last_name | large_string |
| phone_number | large_string |
| gender | large_string |
| age | decimal128(10, 2) |
| is_guest | bool |
| customer_status | large_string |
| signup_channel | large_string |
| loyalty_tier | large_string |
| initial_loyalty_tier | large_string |
| email_verified | bool |
| marketing_opt_in | bool |
| mailing_address | large_string |
| billing_address | large_string |
| loyalty_enrollment_date | date32[day] |
| clv_bucket | large_string |
| batch_id | large_string |
| ingestion_ts | timestamp[us] |
| ingestion_dt | date32[day] |
| event_id | large_string |
| source_file | large_string |
| signup_dt | date32[day] |
| first_purchase_date | date32[day] |
| last_purchase_date | date32[day] |
| total_orders | uint32 |
| days_since_first_buy | int64 |
| days_since_last_buy | int64 |
| is_in_danger_zone | bool |
| needs_bronze_nudge | bool |
| ingest_dt | large_string |

**Key Field Null Rates:**

| Column | Null Rate |
|--------|----------|
| customer_id | 0.00% |

### int_customer_lifetime_value

**Schema (column → dtype):**

| Column | Dtype |
|--------|-------|
| customer_id | large_string |
| total_spent | double |
| total_refunded | double |
| net_clv | double |
| order_count | uint32 |
| return_count | uint32 |
| avg_order_value | double |
| first_order_date | date32[day] |
| last_order_date | date32[day] |
| days_since_last_order | int64 |
| customer_segment | large_string |
| predicted_clv_bucket | large_string |
| actual_clv_bucket | large_string |
| ingest_dt | large_string |

**Key Field Null Rates:**

| Column | Null Rate |
|--------|----------|
| customer_id | 0.00% |

### int_daily_business_metrics

**Schema (column → dtype):**

| Column | Dtype |
|--------|-------|
| date | large_string |
| orders_count | uint32 |
| gross_revenue | double |
| net_revenue | double |
| avg_order_value | double |
| carts_created | uint32 |
| cart_conversion_rate | double |
| returns_count | uint32 |
| return_rate | double |
| refund_total | double |
| orders_7d_avg | double |
| revenue_7d_avg | double |
| revenue_30d_avg | double |
| revenue_30d_std | double |
| revenue_anomaly_flag | large_string |
| ingest_dt | large_string |

**Key Field Null Rates:**

| Column | Null Rate |
|--------|----------|
| date | 0.00% |

**Semantic Issues:** return_rate_matches_ratio: 1 rows, cart_conversion_rate_matches_ratio: 1 rows

### int_product_performance

**Schema (column → dtype):**

| Column | Dtype |
|--------|-------|
| product_id | int64 |
| product_name | large_string |
| category | large_string |
| product_dt | large_string |
| units_sold | int64 |
| units_returned | int64 |
| units_in_carts | int64 |
| gross_revenue | double |
| net_revenue | double |
| gross_margin | double |
| gross_profit | double |
| net_margin | double |
| refunded_amount | double |
| return_rate | double |
| cart_to_order_rate | double |
| margin_pct | double |
| catalog_unit_price | decimal128(18, 2) |
| catalog_cost_price | decimal128(18, 2) |
| inventory_quantity | int64 |
| ingest_dt | large_string |

**Key Field Null Rates:**

| Column | Null Rate |
|--------|----------|
| product_id | 0.00% |
| product_dt | 0.00% |

**Sanity Issues:** return_rate: 10 outside_0_1, cart_to_order_rate: 228 outside_0_1

**Semantic Issues:** units_returned_le_units_sold: 26 rows, return_rate_le_one: 10 rows, cart_to_order_rate_le_one: 228 rows

### int_sales_velocity

**Schema (column → dtype):**

| Column | Dtype |
|--------|-------|
| product_id | int64 |
| order_dt | large_string |
| daily_quantity | int64 |
| velocity_avg | double |
| trend_signal | large_string |
| ingest_dt | large_string |

**Key Field Null Rates:**

| Column | Null Rate |
|--------|----------|
| product_id | 0.00% |
| order_dt | 0.00% |

### int_regional_financials

**Schema (column → dtype):**

| Column | Dtype |
|--------|-------|
| order_id | large_string |
| total_items | int64 |
| order_date | timestamp[us] |
| customer_id | large_string |
| email | large_string |
| order_channel | large_string |
| is_expedited | bool |
| customer_tier | large_string |
| gross_total | decimal128(18, 2) |
| net_total | decimal128(18, 2) |
| total_discount_amount | decimal128(18, 2) |
| payment_method | large_string |
| shipping_speed | large_string |
| shipping_cost | decimal128(18, 2) |
| agent_id | large_string |
| actual_shipping_cost | decimal128(18, 2) |
| payment_processing_fee | decimal128(18, 2) |
| shipping_address | large_string |
| billing_address | large_string |
| clv_bucket | large_string |
| is_reactivated | bool |
| batch_id | large_string |
| ingestion_ts | timestamp[us] |
| ingestion_dt | date32[day] |
| event_id | large_string |
| source_file | large_string |
| order_dt | large_string |
| region | large_string |
| tax_rate | double |
| tax_amount | double |
| net_revenue | double |
| ingest_dt | large_string |

**Key Field Null Rates:**

| Column | Null Rate |
|--------|----------|
| order_id | 0.00% |
| customer_id | 0.00% |

### int_shipping_economics

**Schema (column → dtype):**

| Column | Dtype |
|--------|-------|
| order_id | large_string |
| order_dt | large_string |
| shipping_speed | large_string |
| shipping_cost | decimal128(18, 2) |
| actual_shipping_cost | decimal128(18, 2) |
| shipping_margin | decimal128(38, 2) |
| shipping_margin_pct | decimal128(38, 2) |
| is_expedited | bool |
| order_channel | large_string |
| ingest_dt | large_string |

**Key Field Null Rates:**

| Column | Null Rate |
|--------|----------|
| order_id | 0.00% |
| order_dt | 0.00% |

---

## Metadata

- **Generated by:** `src/validation/enriched_quality.py`
- **Report Format Version:** 1.0

<!-- AUTO-GENERATED - DO NOT EDIT MANUALLY -->