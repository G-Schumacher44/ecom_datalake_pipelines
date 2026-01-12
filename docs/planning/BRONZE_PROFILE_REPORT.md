# Bronze Sample Profile Report

Generated from local parquet samples in `samples/bronze/`.

## Overview

- **Tables sampled**: 8
- **Partitions sampled**: 98
- **Total sample rows**: 1,588,142

### Per-Table Summary

| Table | Partitions | Sample Rows |
| --- | --- | --- |
| cart_items | 93 | 1,096,259 |
| customers | 93 | 6,412 |
| order_items | 93 | 174,657 |
| orders | 93 | 34,786 |
| product_catalog | 5 | 3,000 |
| return_items | 87 | 8,756 |
| returns | 87 | 2,248 |
| shopping_carts | 93 | 262,024 |

### Data Quality Flags

- ⚠️ **return_items.return_id**: Only 1 distinct values (expected high cardinality for primary entity ID) (count=22, samples=return_items:2020-03-03, return_items:2020-03-04, return_items:2020-03-05, return_items:2020-03-06, return_items:2020-03-07)
- ⚠️ **return_items.order_id**: Only 1 distinct values (expected high cardinality for primary entity ID) (count=22, samples=return_items:2020-03-03, return_items:2020-03-04, return_items:2020-03-05, return_items:2020-03-06, return_items:2020-03-07)
- ⚠️ **returns.return_id**: Only 1 distinct values (expected high cardinality for primary entity ID) (count=22, samples=returns:2020-03-03, returns:2020-03-04, returns:2020-03-05, returns:2020-03-06, returns:2020-03-07)
- ⚠️ **returns.order_id**: Only 1 distinct values (expected high cardinality for primary entity ID) (count=22, samples=returns:2020-03-03, returns:2020-03-04, returns:2020-03-05, returns:2020-03-06, returns:2020-03-07)
- ⚠️ **returns.customer_id**: Only 1 distinct values (expected high cardinality for primary entity ID) (count=22, samples=returns:2020-03-03, returns:2020-03-04, returns:2020-03-05, returns:2020-03-06, returns:2020-03-07)
- ⚠️ **return_items.return_item_id**: Only 7 distinct values (expected high cardinality for primary entity ID) (count=10, samples=return_items:2020-03-03, return_items:2020-03-04, return_items:2020-03-05, return_items:2020-03-07, return_items:2023-01-16)
- ⚠️ **return_items.product_id**: Only 7 distinct values (expected high cardinality for primary entity ID) (count=10, samples=return_items:2020-03-03, return_items:2020-03-04, return_items:2020-03-05, return_items:2020-03-07, return_items:2023-01-16)
- 📊 **cart_items** partition `2020-03-26`: 18,087 rows (+53% above average) (count=1, samples=cart_items:2020-03-26)
- 📊 **cart_items** partition `2020-03-27`: 21,304 rows (+80% above average) (count=1, samples=cart_items:2020-03-27)
- 📊 **cart_items** partition `2020-03-28`: 25,864 rows (+119% above average) (count=1, samples=cart_items:2020-03-28)

## Schema Drift

- ✅ No schema drift detected across sampled partitions.

_Note: Partitions with all-null values for a column may show `Null` type instead of the actual type. These are treated as compatible schemas, not drift._

## Partition Coverage

Shows which partitions were sampled per table (for temporal schema drift detection).

**cart_items** (93 partitions):
- `2020-03`: 2020-03-01 ... 2020-03-31 (31 days)
- `2023-01`: 2023-01-01 ... 2023-01-31 (31 days)
- `2025-10`: 2025-10-01 ... 2025-10-31 (31 days)

**customers** (93 partitions):
- `2020-03`: 2020-03-01 ... 2020-03-31 (31 days)
- `2023-01`: 2023-01-01 ... 2023-01-31 (31 days)
- `2025-10`: 2025-10-01 ... 2025-10-31 (31 days)

**order_items** (93 partitions):
- `2020-03`: 2020-03-01 ... 2020-03-31 (31 days)
- `2023-01`: 2023-01-01 ... 2023-01-31 (31 days)
- `2025-10`: 2025-10-01 ... 2025-10-31 (31 days)

**orders** (93 partitions):
- `2020-03`: 2020-03-01 ... 2020-03-31 (31 days)
- `2023-01`: 2023-01-01 ... 2023-01-31 (31 days)
- `2025-10`: 2025-10-01 ... 2025-10-31 (31 days)

**product_catalog** (5 partitions):
- `non-date`: category=Books, category=Clothing, category=Electronics, category=Home, category=Toys

**return_items** (87 partitions):
- `2020-03`: 2020-03-03 ... 2020-03-30 (28 days)
- `2023-01`: 2023-01-01 ... 2023-01-31 (30 days)
- `2025-10`: 2025-10-02 ... 2025-10-30 (29 days)

**returns** (87 partitions):
- `2020-03`: 2020-03-03 ... 2020-03-30 (28 days)
- `2023-01`: 2023-01-01 ... 2023-01-31 (30 days)
- `2025-10`: 2025-10-02 ... 2025-10-30 (29 days)

**shopping_carts** (93 partitions):
- `2020-03`: 2020-03-01 ... 2020-03-31 (31 days)
- `2023-01`: 2023-01-01 ... 2023-01-31 (31 days)
- `2025-10`: 2025-10-01 ... 2025-10-31 (31 days)

## Canonical Schema Keys

| Table | Canonical Schema Key | Sample Partitions |
| --- | --- | --- |
| cart_items | `cart_item_id:Int64|cart_id:String|product_id:Int64|product_name:String|category:String|added_at:String|quantity:Int64|unit_price:Float64|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-03-01, 2020-03-02, 2020-03-03, 2020-03-04, 2020-03-05 |
| customers | `customer_id:String|first_name:String|last_name:String|email:String|phone_number:String|signup_date:String|gender:String|age:Float64|is_guest:Boolean|customer_status:String|signup_channel:String|loyalty_tier:String|initial_loyalty_tier:String|email_verified:Boolean|marketing_opt_in:Boolean|mailing_address:String|billing_address:String|loyalty_enrollment_date:String|clv_bucket:String|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-03-01, 2020-03-02, 2020-03-03, 2020-03-04, 2020-03-05 |
| order_items | `order_id:String|product_id:Int64|product_name:String|category:String|quantity:Int64|unit_price:Float64|discount_amount:Float64|cost_price:Float64|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-03-01, 2020-03-02, 2020-03-03, 2020-03-04, 2020-03-05 |
| orders | `order_id:String|total_items:Int64|order_date:String|customer_id:String|email:String|order_channel:String|is_expedited:Boolean|customer_tier:String|gross_total:Float64|net_total:Float64|total_discount_amount:Float64|payment_method:String|shipping_speed:String|shipping_cost:Float64|agent_id:String|actual_shipping_cost:Float64|payment_processing_fee:Float64|shipping_address:String|billing_address:String|clv_bucket:String|is_reactivated:Boolean|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-03-01, 2020-03-02, 2020-03-03, 2020-03-04, 2020-03-05 |
| product_catalog | `product_id:Int64|product_name:String|category:String|unit_price:Float64|cost_price:Float64|inventory_quantity:Int64|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | Books, Clothing, Electronics, Home, Toys |
| return_items | `return_item_id:Int64|return_id:String|order_id:String|product_id:Int64|product_name:String|category:String|quantity_returned:Int64|unit_price:Float64|cost_price:Float64|refunded_amount:Float64|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-03-03, 2020-03-04, 2020-03-05, 2020-03-06, 2020-03-07 |
| returns | `return_id:String|order_id:String|customer_id:String|email:String|return_date:String|reason:String|return_type:String|refunded_amount:Float64|return_channel:String|agent_id:String|refund_method:String|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-03-03, 2020-03-04, 2020-03-05, 2020-03-06, 2020-03-07 |
| shopping_carts | `cart_id:String|customer_id:String|created_at:String|updated_at:String|cart_total:Float64|status:String|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-03-01, 2020-03-02, 2020-03-03, 2020-03-04, 2020-03-05 |

## Column Statistics (Sample Partition)

Showing detailed stats for one representative partition per table.

### cart_items

**Sample**: `ingest_dt=2020-03-01` (5,310 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| cart_item_id | Int64 | 0.0% | 5,310 | Range: `48` to `189601`<br>p25=44216.0, p50=95884.0, p75=144697.0, p95=180815.0 |
| cart_id | String | 0.0% | 1,195 | Top: `CART-03395281` (15), `CART-03403768` (15), `CART-03397245` (14) |
| product_id | Int64 | 0.0% | 2,496 | Range: `1` to `3000`<br>p25=766.0, p50=1512.0, p75=2267.0, p95=2858.0 |
| product_name | String | 0.0% | 442 | Top: `Elegant Table` (99), `Cozy Lamp` (94), `Compact Monitor` (81) |
| category | String | 0.0% | 5 | Top: `Home` (1222), `Electronics` (1036), `Clothing` (1025) |
| added_at | String | 0.0% | 5,310 | Top: `2020-03-01T03:15:43.907659` (1), `2020-03-01T01:24:50.808738` (1), `2020-03-01T01:27:49.808738` (1) |
| quantity | Int64 | 0.0% | 6 | Range: `1` to `6`<br>p25=1.0, p50=2.0, p75=3.0, p95=5.0 |
| unit_price | Float64 | 0.0% | 2,371 | Range: `5.02` to `249.86`<br>p25=64.03, p50=127.26, p75=188.2, p95=237.65 |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260110T193518` (5310) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T01:47:57+00:00` (5310) |
| event_id | String | 0.0% | 5,310 | Top: `evt_d42693ef508d4d51a94469a90a5da90b58075d288ffc4540bc9ba4b2d5734796` (1), `evt_11e0a256a3e93a1358475df0da29683c5369270b2ab195cf0e30649da0361fff` (1), `evt_2e56407d9c3fa10e073de74a6c28f4d7c75a145a1b98a0097edeb95d039a8bd5` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://gcs-automation-project-raw/ecom/raw/cart_items/ingest_dt=2020-03-01/part-0000.parquet` (5310) |

### customers

**Sample**: `signup_date=2020-03-01` (65 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| customer_id | String | 0.0% | 65 | Top: `CUST-7702` (1), `CUST-19825` (1), `CUST-22240` (1) |
| first_name | String | 0.0% | 56 | Top: `Dawn` (3), `Dennis` (2), `David` (2) |
| last_name | String | 0.0% | 55 | Top: `Williams` (4), `Smith` (4), `Gray` (2) |
| email | String | 0.0% | 65 | Top: `dennis.vincent@gmail.com` (1), `susan.gray@yahoo.com` (1), `thomas.morales@yahoo.com` (1) |
| phone_number | String | 0.0% | 65 | Top: `844-271-8939x530` (1), `+1-748-549-9615x410` (1), `001-470-638-4758x31120` (1) |
| signup_date | String | 0.0% | 1 | Top: `2020-03-01` (65) |
| gender | String | 0.0% | 3 | Top: `Female` (24), `Unknown` (22), `Male` (19) |
| age | Float64 | 0.0% | 38 | Range: `18.0` to `70.0`<br>p25=35.0, p50=45.0, p75=60.0, p95=69.0 |
| is_guest | Boolean | 0.0% | 1 | — |
| customer_status | String | 0.0% | 3 | Top: `Active` (50), `Inactive` (9), `Dormant` (6) |
| signup_channel | String | 0.0% | 4 | Top: `Website` (39), `email` (15), `Social Media` (7) |
| loyalty_tier | String | 10.77% | 5 | Top: `Bronze` (26), `Silver` (16), `Gold` (8) |
| initial_loyalty_tier | String | 10.77% | 5 | Top: `Bronze` (26), `Silver` (16), `Gold` (8) |
| email_verified | Boolean | 0.0% | 2 | — |
| marketing_opt_in | Boolean | 0.0% | 2 | — |
| mailing_address | String | 0.0% | 65 | Top: `1038 Derek Forest, West Douglaschester, IL 55842` (1), `1544 James Well Suite 568, Lukeshire, AS 13600` (1), `121 Watkins Roads Suite 021, Taraport, AR 05450` (1) |
| billing_address | String | 0.0% | 65 | Top: `1038 Derek Forest, West Douglaschester, IL 55842` (1), `1544 James Well Suite 568, Lukeshire, AS 13600` (1), `121 Watkins Roads Suite 021, Taraport, AR 05450` (1) |
| loyalty_enrollment_date | String | 10.77% | 59 | Top: `None` (7), `2021-04-07` (1), `2020-06-29` (1) |
| clv_bucket | String | 0.0% | 3 | Top: `Low` (33), `Medium` (16), `High` (16) |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260110T193518` (65) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T01:35:58+00:00` (65) |
| event_id | String | 0.0% | 65 | Top: `evt_f4a1e0e3303eefd2feb5396178bf83fc393b63d8bfe9044d4426577b401677ca` (1), `evt_74287e37d699583b80a3fb75f2f19d08d83f45678124f8303d2682039a727320` (1), `evt_1dc3b4f64ee0eff88d6e3bc0f607ead802efd3e514adf07eb8642cc475b091c2` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://gcs-automation-project-raw/ecom/raw/customers/signup_date=2020-03-01/part-0000.parquet` (65) |

### order_items

**Sample**: `ingest_dt=2020-03-01` (824 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| order_id | String | 0.0% | 191 | Top: `ORD-00437071` (13), `ORD-00437089` (13), `ORD-00437138` (12) |
| product_id | Int64 | 0.0% | 708 | Range: `1` to `2997`<br>p25=726.0, p50=1517.0, p75=2269.0, p95=2847.0 |
| product_name | String | 0.0% | 188 | Top: `Elegant Table` (15), `Modern Guide` (14), `Portable Monitor` (14) |
| category | String | 0.0% | 5 | Top: `Home` (170), `Clothing` (165), `Electronics` (165) |
| quantity | Int64 | 0.0% | 7 | Range: `1` to `9`<br>p25=1.0, p50=2.0, p75=3.0, p95=5.0 |
| unit_price | Float64 | 0.0% | 698 | Range: `5.14` to `249.86`<br>p25=69.14, p50=129.2, p75=187.87, p95=236.07 |
| discount_amount | Float64 | 0.0% | 155 | Range: `0.0` to `167.94`<br>p25=0.0, p50=0.0, p75=0.0, p95=48.28 |
| cost_price | Float64 | 0.0% | 693 | Range: `2.16` to `167.35`<br>p25=34.8, p50=69.5, p75=99.55, p95=139.58 |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260110T193518` (824) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T01:48:01+00:00` (824) |
| event_id | String | 0.0% | 824 | Top: `evt_2206cd25eb064ee76dbc9e21fe7632446ab9900fb828b05c4ad3d199d0d48664` (1), `evt_2980b5eff5cac3690eb79bf9eb3c471c319eef93bfc4dcbc992967bad97af9f7` (1), `evt_5adcbfb077f0f7cdd804fd34f307f7e15db69a434d598baa1014f5450a57e453` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://gcs-automation-project-raw/ecom/raw/order_items/ingest_dt=2020-03-01/part-0000.parquet` (824) |

### orders

**Sample**: `ingest_dt=2020-03-01` (191 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| order_id | String | 0.0% | 191 | Top: `ORD-00437023` (1), `ORD-00437024` (1), `ORD-00437025` (1) |
| total_items | Int64 | 0.0% | 13 | Range: `1` to `21`<br>p25=3.0, p50=6.0, p75=9.0, p95=12.0 |
| order_date | String | 0.0% | 191 | Top: `2020-03-01 00:02:01.499000` (1), `2020-03-01 00:04:11.752835` (1), `2020-03-01 00:11:46.320004` (1) |
| customer_id | String | 0.0% | 191 | Top: `CUST-15854` (1), `CUST-46461` (1), `GUEST-208412` (1) |
| email | String | 0.0% | 191 | Top: `gregory.peterson@hotmail.com` (1), `chris.alexander@hotmail.com` (1), `joseph47@example.com` (1) |
| order_channel | String | 0.0% | 5 | Top: `Web` (80), `Phone` (42), `Social Media` (35) |
| is_expedited | Boolean | 0.0% | 2 | — |
| customer_tier | String | 0.0% | 4 | Top: `Platinum` (97), `Gold` (45), `Silver` (41) |
| gross_total | Float64 | 0.0% | 191 | Range: `29.72` to `6738.72`<br>p25=495.58, p50=1004.05, p75=1661.82, p95=3019.89 |
| net_total | Float64 | 0.0% | 191 | Range: `29.72` to `6225.36`<br>p25=486.47, p50=985.08, p75=1609.9, p95=2971.89 |
| total_discount_amount | Float64 | 0.0% | 103 | Range: `0.0` to `513.36`<br>p25=0.0, p50=7.56, p75=41.33, p95=117.43 |
| payment_method | String | 0.0% | 20 | Top: `Credit Card` (101), `PayPal` (42), `Apple Pay` (14) |
| shipping_speed | String | 0.0% | 17 | Top: `Standard` (111), `Two-Day` (35), `Overnight` (19) |
| shipping_cost | Float64 | 0.0% | 3 | Range: `5.0` to `80.0`<br>p25=5.0, p50=5.0, p75=45.0, p95=80.0 |
| agent_id | String | 4.19% | 20 | Top: `ONLINE` (142), `None` (8), `CSR-0003` (4) |
| actual_shipping_cost | Float64 | 0.0% | 141 | Range: `3.84` to `75.96`<br>p25=4.17, p50=4.59, p75=36.11, p95=71.24 |
| payment_processing_fee | Float64 | 0.0% | 187 | Range: `0.59` to `181.45`<br>p25=12.06, p50=23.89, p75=40.25, p95=75.68 |
| shipping_address | String | 0.0% | 191 | Top: `35328 Jay Overpass, Dukeport, VA 31150` (1), `122 Jesse Prairie, East Aaron, ME 62666` (1), `8041 Day Meadows, West Maria, TN 55940` (1) |
| billing_address | String | 0.0% | 191 | Top: `35328 Jay Overpass, Dukeport, VA 31150` (1), `122 Jesse Prairie, East Aaron, ME 62666` (1), `8041 Day Meadows, West Maria, TN 55940` (1) |
| clv_bucket | String | 0.0% | 13 | Top: `High` (106), `Medium` (44), `Low` (24) |
| is_reactivated | Boolean | 0.0% | 1 | — |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260110T193518` (191) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T01:48:01+00:00` (191) |
| event_id | String | 0.0% | 191 | Top: `evt_c670d0543fc2a7069857da22657b7c9d9e38214b454a99f9f485e8c8810c4e7d` (1), `evt_5404a6c2f9c39b322ba800a80724a084316f5fc5b9506d74fc4a90572fe59fa8` (1), `evt_09eda64ea38f74c0fa8878f248ca0be5740980e313e023db79321e27d1957927` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://gcs-automation-project-raw/ecom/raw/orders/ingest_dt=2020-03-01/part-0000.parquet` (191) |

### product_catalog

**Sample**: `category=Books` (598 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| product_id | Int64 | 0.0% | 598 | Range: `8` to `2998`<br>p25=661.0, p50=1402.0, p75=2156.0, p95=2868.0 |
| product_name | String | 0.0% | 16 | Top: `Modern Guide` (45), `Modern Memoir` (45), `Illustrated Memoir` (45) |
| category | String | 0.0% | 1 | Top: `Books` (598) |
| unit_price | Float64 | 0.0% | 591 | Range: `5.36` to `249.61`<br>p25=65.0, p50=127.78, p75=190.36, p95=238.1 |
| cost_price | Float64 | 0.0% | 583 | Range: `2.46` to `164.64`<br>p25=34.59, p50=68.15, p75=101.18, p95=143.34 |
| inventory_quantity | Int64 | 0.0% | 147 | Range: `100` to `250`<br>p25=135.0, p50=175.0, p75=212.0, p95=243.0 |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260110T193518` (598) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T01:36:04+00:00` (598) |
| event_id | String | 0.0% | 598 | Top: `evt_38bd8d51c5f4ad7c74f65ab8dfcf4d49ec54765ec7c05ecef3dae6b3b212a590` (1), `evt_62220d5155bb967bebe9dcbf358d96fee0fdc9ac7efc3429ab0e17506bdaf048` (1), `evt_e237c63d597aa9e870d2b749432e384ad8e93526f2e1d53aaa0053792a87e9ce` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://gcs-automation-project-raw/ecom/raw/product_catalog/category=Books/part-0000.parquet` (598) |

### return_items

**Sample**: `ingest_dt=2020-03-03` (7 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| return_item_id | Int64 | 0.0% | 7 | Range: `165` to `171`<br>p25=167.0, p50=168.0, p75=170.0, p95=171.0 |
| return_id | String | 0.0% | 1 | Top: `RTN-00119851` (7) |
| order_id | String | 0.0% | 1 | Top: `ORD-00437195` (7) |
| product_id | Int64 | 0.0% | 7 | Range: `154` to `2767`<br>p25=1070.0, p50=1215.0, p75=2034.0, p95=2767.0 |
| product_name | String | 0.0% | 7 | Top: `Classic Anthology` (1), `Interactive Puzzle` (1), `Educational Doll` (1) |
| category | String | 0.0% | 3 | Top: `Toys` (3), `Home` (3), `Books` (1) |
| quantity_returned | Int64 | 0.0% | 3 | Range: `1` to `3`<br>p25=1.0, p50=1.0, p75=3.0, p95=3.0 |
| unit_price | Float64 | 0.0% | 7 | Range: `6.8` to `201.43`<br>p25=50.76, p50=91.63, p75=169.3, p95=201.43 |
| cost_price | Float64 | 0.0% | 7 | Range: `4.76` to `105.55`<br>p25=22.96, p50=52.3, p75=69.21, p95=105.55 |
| refunded_amount | Float64 | 0.0% | 7 | Range: `13.6` to `604.29`<br>p25=50.76, p50=91.63, p75=507.9, p95=604.29 |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260110T193518` (7) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T01:48:01+00:00` (7) |
| event_id | String | 0.0% | 7 | Top: `evt_2c8ae67aba643d6784bd8609f78e87d5018f392b9e624fc1e126742e0e0ef714` (1), `evt_3717a7e5cfbdc4b95918332876e2e2d9070087e1a7b4085d55e39a3a2f676941` (1), `evt_d73948f946a729955c38a6fa6351fff309b0d03f173cbd902b897c82eaae3aa7` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://gcs-automation-project-raw/ecom/raw/return_items/ingest_dt=2020-03-03/part-0000.parquet` (7) |

### returns

**Sample**: `ingest_dt=2020-03-03` (1 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| return_id | String | 0.0% | 1 | Top: `RTN-00119851` (1) |
| order_id | String | 0.0% | 1 | Top: `ORD-00437195` (1) |
| customer_id | String | 0.0% | 1 | Top: `GUEST-173093` (1) |
| email | String | 0.0% | 1 | Top: `egay@example.net` (1) |
| return_date | String | 0.0% | 1 | Top: `2020-03-03` (1) |
| reason | String | 0.0% | 1 | Top: `Product did not match description` (1) |
| return_type | String | 0.0% | 1 | Top: `Refund` (1) |
| refunded_amount | Float64 | 0.0% | 1 | Range: `1406.79` to `1406.79`<br>p25=1406.79, p50=1406.79, p75=1406.79, p95=1406.79 |
| return_channel | String | 0.0% | 1 | Top: `Phone` (1) |
| agent_id | String | 0.0% | 1 | Top: `CSR-0006` (1) |
| refund_method | String | 0.0% | 1 | Top: `Credit Card` (1) |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260110T193518` (1) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T01:48:01+00:00` (1) |
| event_id | String | 0.0% | 1 | Top: `evt_db62e6f9d46922e9246ca8c683395644f509d69a569a402b0a326fa01d0290d2` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://gcs-automation-project-raw/ecom/raw/returns/ingest_dt=2020-03-03/part-0000.parquet` (1) |

### shopping_carts

**Sample**: `ingest_dt=2020-03-01` (1,376 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| cart_id | String | 0.0% | 1,376 | Top: `CART-03379183` (1), `CART-03379191` (1), `CART-03379221` (1) |
| customer_id | String | 0.0% | 1,376 | Top: `CUST-52579` (1), `CUST-20911` (1), `CUST-160708` (1) |
| created_at | String | 0.0% | 1,376 | Top: `2020-03-01T03:13:05.907659` (1), `2020-03-01T01:23:49.808738` (1), `2020-03-01T21:00:45.944809` (1) |
| updated_at | String | 4.58% | 1,314 | Top: `None` (63), `2020-03-01T03:15:43.907659` (1), `2020-03-01T01:36:13.808738` (1) |
| cart_total | Float64 | 0.0% | 1,196 | Range: `0.0` to `9192.92`<br>p25=298.87, p50=914.68, p75=1628.86, p95=3140.21 |
| status | String | 0.0% | 26 | Top: `abandoned` (865), `converted` (171), `emptied` (164) |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260110T193518` (1376) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T01:47:57+00:00` (1376) |
| event_id | String | 0.0% | 1,376 | Top: `evt_65f4c556f6f567fe22b1f2976df89d5d9b56617612b599586b9143394e333e1d` (1), `evt_f47c2e977add4b53bdbc6afeb9658b744b333aad4769f99f0e160c9b639610e8` (1), `evt_b262fa44e94edd560b1e422bfaf5ac10b198c6eed85c69cc530022ac466bd0a5` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://gcs-automation-project-raw/ecom/raw/shopping_carts/ingest_dt=2020-03-01/part-0000.parquet` (1376) |
<!-- GENERATED META -->
Last updated (UTC): 2026-01-11T17:17:15Z
Content hash (SHA-256): d3f5b16e97cb29af3d0bdb09396ae84be4bb0d494038d24425e90beec89d7b7c
<!-- END GENERATED META -->
