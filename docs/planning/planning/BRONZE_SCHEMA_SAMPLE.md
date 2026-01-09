# Bronze Sample Schema Summary

Generated from local parquet samples in `samples/bronze/`.

## cart_items

Sample files:
- `samples/bronze/cart_items/ingest_dt=2020-01-01/part-0000.parquet`

Schema:

| Column | Type |
| --- | --- |
| cart_item_id | int64 |
| cart_id | string |
| product_id | int64 |
| product_name | string |
| category | string |
| added_at | string |
| quantity | int64 |
| unit_price | double |
| batch_id | string |
| ingestion_ts | string |
| event_id | string |
| source_file | string |

Sample row counts:
- [61218]

## customers

Sample files:
- `samples/bronze/customers/ingest_dt=2020-01-01/part-0000.parquet`

Schema:

| Column | Type |
| --- | --- |
| customer_id | string |
| first_name | string |
| last_name | string |
| email | string |
| phone_number | string |
| signup_date | string |
| gender | string |
| age | double |
| is_guest | bool |
| customer_status | string |
| signup_channel | string |
| loyalty_tier | string |
| initial_loyalty_tier | string |
| email_verified | bool |
| marketing_opt_in | bool |
| mailing_address | string |
| billing_address | string |
| loyalty_enrollment_date | string |
| clv_bucket | string |
| batch_id | string |
| ingestion_ts | string |
| event_id | string |
| source_file | string |

Sample row counts:
- [6000]

## order_items

Sample files:
- `samples/bronze/order_items/ingest_dt=2020-01-01/part-0000.parquet`

Schema:

| Column | Type |
| --- | --- |
| order_id | string |
| product_id | int64 |
| product_name | string |
| category | string |
| quantity | int64 |
| unit_price | double |
| discount_amount | double |
| cost_price | double |
| batch_id | string |
| ingestion_ts | string |
| event_id | string |
| source_file | string |

Sample row counts:
- [9214]

## orders

Sample files:
- `samples/bronze/orders/ingest_dt=2020-01-01/part-0000.parquet`

Schema:

| Column | Type |
| --- | --- |
| order_id | string |
| total_items | int64 |
| order_date | string |
| customer_id | string |
| email | string |
| order_channel | string |
| is_expedited | bool |
| customer_tier | string |
| gross_total | double |
| net_total | double |
| total_discount_amount | double |
| payment_method | string |
| shipping_speed | string |
| shipping_cost | double |
| agent_id | string |
| actual_shipping_cost | double |
| payment_processing_fee | double |
| shipping_address | string |
| billing_address | string |
| clv_bucket | string |
| is_reactivated | bool |
| batch_id | string |
| ingestion_ts | string |
| event_id | string |
| source_file | string |

Sample row counts:
- [1849]

## product_catalog

Sample files:
- `samples/bronze/product_catalog/ingest_dt=2020-01-01/part-0000.parquet`

Schema:

| Column | Type |
| --- | --- |
| product_id | int64 |
| product_name | string |
| category | string |
| unit_price | double |
| cost_price | double |
| inventory_quantity | int64 |
| batch_id | string |
| ingestion_ts | string |
| event_id | string |
| source_file | string |

Sample row counts:
- [300]

## return_items

Sample files:
- `samples/bronze/return_items/ingest_dt=2020-01-01/part-0000.parquet`

Schema:

| Column | Type |
| --- | --- |
| return_item_id | int64 |
| return_id | string |
| order_id | string |
| product_id | int64 |
| product_name | string |
| category | string |
| quantity_returned | int64 |
| unit_price | double |
| cost_price | double |
| refunded_amount | double |
| batch_id | string |
| ingestion_ts | string |
| event_id | string |
| source_file | string |

Sample row counts:
- [730]

## returns

Sample files:
- `samples/bronze/returns/ingest_dt=2020-01-01/part-0000.parquet`

Schema:

| Column | Type |
| --- | --- |
| return_id | string |
| order_id | string |
| customer_id | string |
| email | string |
| return_date | string |
| reason | string |
| return_type | string |
| refunded_amount | double |
| return_channel | string |
| agent_id | string |
| refund_method | string |
| batch_id | string |
| ingestion_ts | string |
| event_id | string |
| source_file | string |

Sample row counts:
- [187]

## shopping_carts

Sample files:
- `samples/bronze/shopping_carts/ingest_dt=2020-01-01/part-0000.parquet`

Schema:

| Column | Type |
| --- | --- |
| cart_id | string |
| customer_id | string |
| created_at | string |
| updated_at | string |
| cart_total | double |
| status | string |
| batch_id | string |
| ingestion_ts | string |
| event_id | string |
| source_file | string |

Sample row counts:
- [13520]
