# Silver Data Profile

**Generated:** 2026-01-11 20:17:11 UTC

## Table Profiles

### orders

- **Row Count:** 34,786
- **Column Count:** 26

**Schema (column → dtype):**

| Column | Dtype | Nulls |
|--------|-------|-------|
| order_id | String | 0 |
| total_items | Int64 | 0 |
| order_date | Datetime(time_unit='us', time_zone=None) | 0 |
| customer_id | String | 0 |
| email | String | 0 |
| order_channel | String | 0 |
| is_expedited | Boolean | 0 |
| customer_tier | String | 0 |
| gross_total | Decimal(precision=18, scale=2) | 0 |
| net_total | Decimal(precision=18, scale=2) | 0 |
| total_discount_amount | Decimal(precision=18, scale=2) | 0 |
| payment_method | String | 0 |
| shipping_speed | String | 0 |
| shipping_cost | Decimal(precision=18, scale=2) | 0 |
| agent_id | String | 1,462 |
| actual_shipping_cost | Decimal(precision=18, scale=2) | 0 |
| payment_processing_fee | Decimal(precision=18, scale=2) | 0 |
| shipping_address | String | 0 |
| billing_address | String | 0 |
| clv_bucket | String | 0 |
| is_reactivated | Boolean | 0 |
| batch_id | String | 0 |
| ingestion_ts | Datetime(time_unit='us', time_zone=None) | 0 |
| event_id | String | 0 |
| source_file | String | 0 |
| order_dt | Categorical | 0 |

### customers

- **Row Count:** 6,412
- **Column Count:** 24

**Schema (column → dtype):**

| Column | Dtype | Nulls |
|--------|-------|-------|
| customer_id | String | 0 |
| email | String | 0 |
| signup_date | Date | 0 |
| first_name | String | 0 |
| last_name | String | 0 |
| phone_number | String | 0 |
| gender | String | 0 |
| age | Decimal(precision=10, scale=2) | 0 |
| is_guest | Boolean | 0 |
| customer_status | String | 0 |
| signup_channel | String | 0 |
| loyalty_tier | String | 644 |
| initial_loyalty_tier | String | 644 |
| email_verified | Boolean | 0 |
| marketing_opt_in | Boolean | 0 |
| mailing_address | String | 0 |
| billing_address | String | 0 |
| loyalty_enrollment_date | Date | 644 |
| clv_bucket | String | 0 |
| batch_id | String | 0 |
| ingestion_ts | Datetime(time_unit='us', time_zone=None) | 0 |
| event_id | String | 0 |
| source_file | String | 0 |
| signup_dt | Categorical | 0 |

### product_catalog

- **Row Count:** 3,000
- **Column Count:** 10

**Schema (column → dtype):**

| Column | Dtype | Nulls |
|--------|-------|-------|
| product_id | Int64 | 0 |
| product_name | String | 0 |
| unit_price | Decimal(precision=18, scale=2) | 0 |
| cost_price | Decimal(precision=18, scale=2) | 0 |
| inventory_quantity | Int64 | 0 |
| batch_id | String | 0 |
| ingestion_ts | Datetime(time_unit='us', time_zone=None) | 0 |
| event_id | String | 0 |
| source_file | String | 0 |
| category | Categorical | 0 |

### shopping_carts

- **Row Count:** 262,024
- **Column Count:** 11

**Schema (column → dtype):**

| Column | Dtype | Nulls |
|--------|-------|-------|
| cart_id | String | 0 |
| customer_id | String | 0 |
| created_at | Datetime(time_unit='us', time_zone=None) | 0 |
| updated_at | Datetime(time_unit='us', time_zone=None) | 10,341 |
| cart_total | Decimal(precision=18, scale=2) | 0 |
| status | String | 0 |
| batch_id | String | 0 |
| ingestion_ts | Datetime(time_unit='us', time_zone=None) | 0 |
| event_id | String | 0 |
| source_file | String | 0 |
| created_dt | Categorical | 0 |

### cart_items

- **Row Count:** 1,096,189
- **Column Count:** 13

**Schema (column → dtype):**

| Column | Dtype | Nulls |
|--------|-------|-------|
| cart_item_id | Int64 | 0 |
| cart_id | String | 0 |
| product_id | Int64 | 0 |
| product_name | String | 0 |
| category | String | 0 |
| added_at | Datetime(time_unit='us', time_zone=None) | 0 |
| quantity | Int64 | 0 |
| unit_price | Decimal(precision=18, scale=2) | 0 |
| batch_id | String | 0 |
| ingestion_ts | Datetime(time_unit='us', time_zone=None) | 0 |
| event_id | String | 0 |
| source_file | String | 0 |
| added_dt | Categorical | 0 |

### order_items

- **Row Count:** 174,657
- **Column Count:** 13

**Schema (column → dtype):**

| Column | Dtype | Nulls |
|--------|-------|-------|
| order_id | String | 0 |
| product_id | Int64 | 0 |
| product_name | String | 0 |
| category | String | 0 |
| quantity | Int64 | 0 |
| unit_price | Decimal(precision=18, scale=2) | 0 |
| discount_amount | Decimal(precision=18, scale=2) | 0 |
| cost_price | Decimal(precision=18, scale=2) | 0 |
| batch_id | String | 0 |
| ingestion_ts | Datetime(time_unit='us', time_zone=None) | 0 |
| event_id | String | 0 |
| source_file | String | 0 |
| order_dt | Categorical | 0 |

### returns

- **Row Count:** 2,248
- **Column Count:** 16

**Schema (column → dtype):**

| Column | Dtype | Nulls |
|--------|-------|-------|
| return_id | String | 0 |
| order_id | String | 0 |
| customer_id | String | 0 |
| email | String | 0 |
| return_date | Datetime(time_unit='us', time_zone=None) | 0 |
| reason | String | 0 |
| return_type | String | 0 |
| refunded_amount | Decimal(precision=18, scale=2) | 0 |
| return_channel | String | 0 |
| agent_id | String | 85 |
| refund_method | String | 0 |
| batch_id | String | 0 |
| ingestion_ts | Datetime(time_unit='us', time_zone=None) | 0 |
| event_id | String | 0 |
| source_file | String | 0 |
| return_dt | Categorical | 0 |

### return_items

- **Row Count:** 8,756
- **Column Count:** 15

**Schema (column → dtype):**

| Column | Dtype | Nulls |
|--------|-------|-------|
| return_item_id | Int64 | 0 |
| return_id | String | 0 |
| order_id | String | 0 |
| product_id | Int64 | 0 |
| product_name | String | 0 |
| category | String | 0 |
| quantity_returned | Int64 | 0 |
| unit_price | Decimal(precision=18, scale=2) | 0 |
| cost_price | Decimal(precision=18, scale=2) | 0 |
| refunded_amount | Decimal(precision=18, scale=2) | 0 |
| batch_id | String | 0 |
| ingestion_ts | Datetime(time_unit='us', time_zone=None) | 0 |
| event_id | String | 0 |
| source_file | String | 0 |
| return_dt | Categorical | 0 |
