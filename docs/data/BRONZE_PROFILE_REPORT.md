# Bronze Sample Profile Report

Generated from local parquet samples in `samples/bronze/`.

## Sample Scope

- **Months**: 2020-03, 2023-01, 2025-10

## Overview

- **Tables sampled**: 7
- **Partitions sampled**: 93
- **Total sample rows**: 5,504,825

### Per-Table Summary

| Table | Partitions | Sample Rows |
| --- | --- | --- |
| cart_items | 93 | 3,688,292 |
| customers | 93 | 6,412 |
| order_items | 93 | 654,480 |
| orders | 93 | 130,166 |
| return_items | 88 | 34,249 |
| returns | 88 | 8,808 |
| shopping_carts | 93 | 982,418 |

### Data Quality Flags

- ⚠️ **return_items.return_id**: Only 4 distinct values (expected high cardinality for primary entity ID) (count=6, samples=return_items:2020-03-02, return_items:2023-01-16, return_items:2023-01-17, return_items:2025-10-02, return_items:2025-10-03)
- ⚠️ **return_items.order_id**: Only 4 distinct values (expected high cardinality for primary entity ID) (count=6, samples=return_items:2020-03-02, return_items:2023-01-16, return_items:2023-01-17, return_items:2025-10-02, return_items:2025-10-03)
- ⚠️ **returns.return_id**: Only 4 distinct values (expected high cardinality for primary entity ID) (count=6, samples=returns:2020-03-02, returns:2023-01-16, returns:2023-01-17, returns:2025-10-02, returns:2025-10-03)
- ⚠️ **returns.order_id**: Only 4 distinct values (expected high cardinality for primary entity ID) (count=6, samples=returns:2020-03-02, returns:2023-01-16, returns:2023-01-17, returns:2025-10-02, returns:2025-10-03)
- ⚠️ **returns.customer_id**: Only 4 distinct values (expected high cardinality for primary entity ID) (count=6, samples=returns:2020-03-02, returns:2023-01-16, returns:2023-01-17, returns:2025-10-02, returns:2025-10-03)
- 📊 **cart_items** partition `2020-03-26`: 67,704 rows (+70% above average) (count=1, samples=cart_items:2020-03-26)
- 📊 **cart_items** partition `2020-03-27`: 77,978 rows (+96% above average) (count=1, samples=cart_items:2020-03-27)
- 📊 **cart_items** partition `2020-03-28`: 95,661 rows (+141% above average) (count=1, samples=cart_items:2020-03-28)
- 📊 **cart_items** partition `2020-03-29`: 100,000 rows (+152% above average) (count=1, samples=cart_items:2020-03-29)
- 📊 **cart_items** partition `2020-03-30`: 100,000 rows (+152% above average) (count=1, samples=cart_items:2020-03-30)

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

**return_items** (88 partitions):
- `2020-03`: 2020-03-02 ... 2020-03-30 (29 days)
- `2023-01`: 2023-01-01 ... 2023-01-31 (30 days)
- `2025-10`: 2025-10-02 ... 2025-10-30 (29 days)

**returns** (88 partitions):
- `2020-03`: 2020-03-02 ... 2020-03-30 (29 days)
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
| return_items | `return_item_id:Int64|return_id:String|order_id:String|product_id:Int64|product_name:String|category:String|quantity_returned:Int64|unit_price:Float64|cost_price:Float64|refunded_amount:Float64|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-03-02, 2020-03-03, 2020-03-04, 2020-03-05, 2020-03-06 |
| returns | `return_id:String|order_id:String|customer_id:String|email:String|return_date:String|reason:String|return_type:String|refunded_amount:Float64|return_channel:String|agent_id:String|refund_method:String|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-03-02, 2020-03-03, 2020-03-04, 2020-03-05, 2020-03-06 |
| shopping_carts | `cart_id:String|customer_id:String|created_at:String|updated_at:String|cart_total:Float64|status:String|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-03-01, 2020-03-02, 2020-03-03, 2020-03-04, 2020-03-05 |

## Column Statistics (Sample Partition)

Showing detailed stats for one representative partition per table.

### cart_items

**Sample**: `ingest_dt=2020-03-01` (20,023 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| cart_item_id | Int64 | 0.0% | 20,023 | Range: `211` to `709765`<br>p25=180859.0, p50=354799.0, p75=538992.0, p95=679379.0 |
| cart_id | String | 0.0% | 4,399 | Top: `CART-11037596` (15), `CART-11044607` (15), `CART-11097168` (15) |
| product_id | Int64 | 0.0% | 2,996 | Range: `1` to `3000`<br>p25=737.0, p50=1488.0, p75=2256.0, p95=2851.0 |
| product_name | String | 0.0% | 638 | Top: `Cozy Lamp` (371), `Elegant Table` (331), `Rustic Table` (298) |
| category | String | 0.0% | 5 | Top: `Home` (4847), `Electronics` (3958), `Books` (3944) |
| added_at | String | 0.0% | 20,023 | Top: `2020-03-01T13:59:50.717691` (1), `2020-03-01T14:00:38.717691` (1), `2020-03-01T14:04:56.717691` (1) |
| quantity | Int64 | 0.0% | 6 | Range: `1` to `6`<br>p25=1.0, p50=2.0, p75=3.0, p95=5.0 |
| unit_price | Float64 | 0.0% | 2,827 | Range: `5.02` to `249.86`<br>p25=63.56, p50=125.26, p75=188.96, p95=238.2 |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T104546` (20023) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T17:07:45+00:00` (20023) |
| event_id | String | 0.0% | 20,023 | Top: `evt_ee1f47614e81aff0a9c9554bcc96f41419e6729988e35a299b8959a7f7d392b3` (1), `evt_d1aeca8e33677b189763608c34df7e13bdadaf9621f58481c8870be1dc651128` (1), `evt_015661fd07fd85888c04f036a1a926c9656485fb618e9e7cac0b0628e269f281` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/cart_items/ingest_dt=2020-03-01/part-0000.parquet` (20023) |

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
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T104546` (65) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T16:48:28+00:00` (65) |
| event_id | String | 0.0% | 65 | Top: `evt_f4a1e0e3303eefd2feb5396178bf83fc393b63d8bfe9044d4426577b401677ca` (1), `evt_74287e37d699583b80a3fb75f2f19d08d83f45678124f8303d2682039a727320` (1), `evt_1dc3b4f64ee0eff88d6e3bc0f607ead802efd3e514adf07eb8642cc475b091c2` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/customers/signup_date=2020-03-01/part-0000.parquet` (65) |

### order_items

**Sample**: `ingest_dt=2020-03-01` (3,035 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| order_id | String | 0.0% | 621 | Top: `ORD-01422581` (15), `ORD-01422759` (15), `ORD-01422915` (15) |
| product_id | Int64 | 0.0% | 1,925 | Range: `1` to `3000`<br>p25=754.0, p50=1491.0, p75=2274.0, p95=2860.0 |
| product_name | String | 0.0% | 341 | Top: `Cozy Lamp` (58), `Elegant Rug` (55), `Elegant Table` (51) |
| category | String | 0.0% | 5 | Top: `Home` (738), `Electronics` (611), `Books` (593) |
| quantity | Int64 | 0.0% | 7 | Range: `1` to `9`<br>p25=1.0, p50=2.0, p75=3.0, p95=5.0 |
| unit_price | Float64 | 0.0% | 1,864 | Range: `5.02` to `249.86`<br>p25=62.08, p50=125.96, p75=188.57, p95=237.5 |
| discount_amount | Float64 | 0.0% | 605 | Range: `0.0` to `283.26`<br>p25=0.0, p50=0.0, p75=0.0, p95=59.93 |
| cost_price | Float64 | 0.0% | 1,806 | Range: `2.22` to `170.41`<br>p25=33.53, p50=68.48, p75=100.23, p95=141.73 |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T104546` (3035) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T17:07:57+00:00` (3035) |
| event_id | String | 0.0% | 3,035 | Top: `evt_b15e82d3857e1dde2db9428fef82ca947f118ba09085ef01b89b185a5a9150c5` (1), `evt_75c8a41a7655ea1399e50c85600595a7ed72a7b47fc5cf667d44646bac7b6968` (1), `evt_2c73f0a0a2fde7b6b63987b3fdc1be21b9436b8ba4bb8d9faef0669d4c1fefcf` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/order_items/ingest_dt=2020-03-01/part-0000.parquet` (3035) |

### orders

**Sample**: `ingest_dt=2020-03-01` (621 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| order_id | String | 0.0% | 621 | Top: `ORD-01422430` (1), `ORD-01422431` (1), `ORD-01422432` (1) |
| total_items | Int64 | 0.0% | 14 | Range: `1` to `22`<br>p25=4.0, p50=7.0, p75=10.0, p95=13.0 |
| order_date | String | 0.0% | 621 | Top: `2020-03-01 00:01:23.094983` (1), `2020-03-01 00:09:10.575202` (1), `2020-03-01 00:11:22.849149` (1) |
| customer_id | String | 0.0% | 621 | Top: `GUEST-179840` (1), `CUST-57274` (1), `CUST-105882` (1) |
| email | String | 0.0% | 619 | Top: `susan.harrison@gmail.com` (2), `amy.smith@yahoo.com` (2), `randy92@example.org` (1) |
| order_channel | String | 0.0% | 5 | Top: `Web` (283), `Phone` (137), `Social Media` (84) |
| is_expedited | Boolean | 0.0% | 2 | — |
| customer_tier | String | 0.0% | 4 | Top: `Platinum` (343), `Gold` (140), `Silver` (122) |
| gross_total | Float64 | 0.0% | 620 | Range: `11.46` to `9571.19`<br>p25=549.33, p50=1113.79, p75=1771.28, p95=4108.23 |
| net_total | Float64 | 0.0% | 621 | Range: `9.17` to `8943.47`<br>p25=537.68, p50=1085.31, p75=1747.35, p95=4024.49 |
| total_discount_amount | Float64 | 0.0% | 371 | Range: `0.0` to `776.61`<br>p25=0.0, p50=14.38, p75=60.79, p95=176.38 |
| payment_method | String | 0.0% | 33 | Top: `Credit Card` (343), `PayPal` (115), `Apple Pay` (39) |
| shipping_speed | String | 0.0% | 22 | Top: `Standard` (331), `Two-Day` (136), `Overnight` (75) |
| shipping_cost | Float64 | 0.0% | 3 | Range: `5.0` to `80.0`<br>p25=5.0, p50=5.0, p75=45.0, p95=80.0 |
| agent_id | String | 3.86% | 22 | Top: `ONLINE` (464), `None` (24), `CSR-0014` (11) |
| actual_shipping_cost | Float64 | 0.0% | 344 | Range: `3.83` to `76.98`<br>p25=4.27, p50=4.72, p75=38.05, p95=71.91 |
| payment_processing_fee | Float64 | 0.0% | 593 | Range: `0.23` to `268.3`<br>p25=12.86, p50=25.76, p75=44.38, p95=94.96 |
| shipping_address | String | 0.0% | 621 | Top: `6106 Brown Roads, Douglasville, MH 82321` (1), `13471 Charles Plains Suite 128, Moorefort, WI 47283` (1), `054 Allen Ramp, Joshuaview, PA 06763` (1) |
| billing_address | String | 0.0% | 621 | Top: `6106 Brown Roads, Douglasville, MH 82321` (1), `13471 Charles Plains Suite 128, Moorefort, WI 47283` (1), `054 Allen Ramp, Joshuaview, PA 06763` (1) |
| clv_bucket | String | 0.0% | 22 | Top: `High` (358), `Medium` (131), `Low` (54) |
| is_reactivated | Boolean | 0.0% | 1 | — |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T104546` (621) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T17:07:57+00:00` (621) |
| event_id | String | 0.0% | 621 | Top: `evt_1c68f045d72215bd32907a79f803e9d3fbb585ab97277e24a21746915ea67023` (1), `evt_aef33d2865b4e97e4e5ed1c1ece133e731d87c77419e061bda3c5d4d0643add5` (1), `evt_b983f2244bba0b2f28f6c39c2ee6d099b4cec05afd53bb0eaa6cc7013e53c6fe` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/orders/ingest_dt=2020-03-01/part-0000.parquet` (621) |

### return_items

**Sample**: `ingest_dt=2020-03-02` (17 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| return_item_id | Int64 | 0.0% | 17 | Range: `4` to `333`<br>p25=279.0, p50=293.0, p75=297.0, p95=332.0 |
| return_id | String | 0.0% | 4 | Top: `RTN-00388674` (8), `RTN-00388597` (4), `RTN-00388671` (3) |
| order_id | String | 0.0% | 4 | Top: `ORD-01422691` (8), `ORD-01422440` (4), `ORD-01422678` (3) |
| product_id | Int64 | 0.0% | 17 | Range: `233` to `2663`<br>p25=939.0, p50=1452.0, p75=2036.0, p95=2586.0 |
| product_name | String | 0.0% | 15 | Top: `Illustrated Anthology` (2), `Wireless Headphones` (2), `Compact Speaker` (1) |
| category | String | 0.0% | 4 | Top: `Electronics` (6), `Books` (6), `Clothing` (3) |
| quantity_returned | Int64 | 0.0% | 3 | Range: `1` to `3`<br>p25=1.0, p50=2.0, p75=2.0, p95=3.0 |
| unit_price | Float64 | 0.0% | 17 | Range: `34.9` to `242.17`<br>p25=109.74, p50=142.94, p75=167.17, p95=227.98 |
| cost_price | Float64 | 0.0% | 17 | Range: `19.29` to `141.73`<br>p25=56.49, p50=66.67, p75=98.83, p95=137.52 |
| refunded_amount | Float64 | 0.0% | 17 | Range: `34.9` to `726.51`<br>p25=134.76, p50=167.17, p75=301.0, p95=683.94 |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T104546` (17) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T17:07:59+00:00` (17) |
| event_id | String | 0.0% | 17 | Top: `evt_17e28442a177effc114a19741f55aef451887cec2070bf0ef9d3cfb7318bf576` (1), `evt_ef3f0e7ff27adfda6e266f7a34b996cee90f0c3f2ccf6b6b0c6a47a54a1ed87f` (1), `evt_6834ef08b98cf84c0e429b43fd921aad6766d07ad99a24bdd90e38503bd023ad` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/return_items/ingest_dt=2020-03-02/part-0000.parquet` (17) |

### returns

**Sample**: `ingest_dt=2020-03-02` (4 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| return_id | String | 0.0% | 4 | Top: `RTN-00388597` (1), `RTN-00388671` (1), `RTN-00388674` (1) |
| order_id | String | 0.0% | 4 | Top: `ORD-01422440` (1), `ORD-01422678` (1), `ORD-01422691` (1) |
| customer_id | String | 0.0% | 4 | Top: `GUEST-121883` (1), `GUEST-117584` (1), `GUEST-126332` (1) |
| email | String | 0.0% | 4 | Top: `noah53@example.com` (1), `rnichols@example.net` (1), `david60@example.org` (1) |
| return_date | String | 0.0% | 1 | Top: `2020-03-02` (4) |
| reason | String | 0.0% | 4 | Top: `Damaged in transit` (1), `Item arrived late` (1), `Arrived damaged` (1) |
| return_type | String | 0.0% | 1 | Top: `Refund` (4) |
| refunded_amount | Float64 | 0.0% | 4 | Range: `397.71` to `1736.21`<br>p25=604.38, p50=1577.62, p75=1577.62, p95=1736.21 |
| return_channel | String | 0.0% | 3 | Top: `Phone` (2), `Web` (1), `Ebay` (1) |
| agent_id | String | 0.0% | 3 | Top: `ONLINE` (2), `CSR-0015` (1), `CSR-0014` (1) |
| refund_method | String | 0.0% | 2 | Top: `Credit Card` (3), `ACH` (1) |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T104546` (4) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T17:07:59+00:00` (4) |
| event_id | String | 0.0% | 4 | Top: `evt_ea567ed9068e8ecaeb1366129dd49548b7a5ba0ce67ed5a6b81d3d2311419613` (1), `evt_0fb86a7ff76238791cfd1114066de72db5160f4295b1acadc1907f8bed663532` (1), `evt_1bc64aa10f4bd627f95502eaeec9bf5bce1e9c20802edaceee1a96f4a2a40e17` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/returns/ingest_dt=2020-03-02/part-0000.parquet` (4) |

### shopping_carts

**Sample**: `ingest_dt=2020-03-01` (5,089 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| cart_id | String | 0.0% | 5,089 | Top: `CART-11017447` (1), `CART-11017476` (1), `CART-11017515` (1) |
| customer_id | String | 0.0% | 5,089 | Top: `CUST-111247` (1), `GUEST-109600` (1), `CUST-118476` (1) |
| created_at | String | 0.0% | 5,089 | Top: `2020-03-01T13:24:45.648577` (1), `2020-03-01T13:55:33.717691` (1), `2020-03-01T01:41:30.351686` (1) |
| updated_at | String | 3.83% | 4,895 | Top: `None` (195), `2020-03-01T13:37:28.648577` (1), `2020-03-01T14:13:28.717691` (1) |
| cart_total | Float64 | 0.0% | 4,379 | Range: `0.0` to `10782.04`<br>p25=346.74, p50=900.81, p75=1621.7, p95=3321.27 |
| status | String | 0.0% | 33 | Top: `abandoned` (3323), `emptied` (584), `converted` (532) |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T104546` (5089) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T17:07:42+00:00` (5089) |
| event_id | String | 0.0% | 5,089 | Top: `evt_6fe812c5617c8dd0f98430c40b4e9613678f1eb8173baf837eeccf5da41e59d6` (1), `evt_9fde26223b1d1845a033744594586c167fc44e921508a0c3a90acaf6ecced20f` (1), `evt_62719919106dace9f47ae6a27b5d6a743bde5edc0cb27b7a07692d8f55daad45` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/shopping_carts/ingest_dt=2020-03-01/part-0000.parquet` (5089) |
<!-- GENERATED META -->
Last updated (UTC): 2026-01-12T19:47:22Z
Content hash (SHA-256): 7a45617d90ee8cdec879b2874db9896853bd5f3eaadf06fc0fb9490ffc019c56
<!-- END GENERATED META -->
