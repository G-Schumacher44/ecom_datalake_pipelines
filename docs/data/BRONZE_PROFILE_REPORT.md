# Bronze Sample Profile Report

Generated from local parquet samples in `samples/bronze/`.

## Sample Scope

- **Ingest dates**: 2020-03-01, 2023-01-01, 2024-01-01, 2024-01-02, 2024-01-03, 2025-10-01

## Overview

- **Tables sampled**: 8
- **Partitions sampled**: 11
- **Total sample rows**: 194,325

### Per-Table Summary

| Table | Partitions | Sample Rows |
| --- | --- | --- |
| cart_items | 6 | 132,693 |
| customers | 3 | 211 |
| order_items | 6 | 20,371 |
| orders | 6 | 4,215 |
| product_catalog | 5 | 3,000 |
| return_items | 1 | 615 |
| returns | 1 | 153 |
| shopping_carts | 6 | 33,067 |

### Data Quality Flags

- 📊 **order_items** partition `2023-01-01`: 5,180 rows (+52% above average) (count=1, samples=order_items:2023-01-01)
- 📊 **orders** partition `2023-01-01`: 1,070 rows (+52% above average) (count=1, samples=orders:2023-01-01)

## Schema Drift

- ✅ No schema drift detected across sampled partitions.

_Note: Partitions with all-null values for a column may show `Null` type instead of the actual type. These are treated as compatible schemas, not drift._

## Partition Coverage

Shows which partitions were sampled per table (for temporal schema drift detection).

**cart_items** (6 partitions):
- `2020-03`: 2020-03-01
- `2023-01`: 2023-01-01
- `2024-01`: 2024-01-01, 2024-01-02, 2024-01-03
- `2025-10`: 2025-10-01

**customers** (3 partitions):
- `2020-03`: 2020-03-01
- `2023-01`: 2023-01-01
- `2025-10`: 2025-10-01

**order_items** (6 partitions):
- `2020-03`: 2020-03-01
- `2023-01`: 2023-01-01
- `2024-01`: 2024-01-01, 2024-01-02, 2024-01-03
- `2025-10`: 2025-10-01

**orders** (6 partitions):
- `2020-03`: 2020-03-01
- `2023-01`: 2023-01-01
- `2024-01`: 2024-01-01, 2024-01-02, 2024-01-03
- `2025-10`: 2025-10-01

**product_catalog** (5 partitions):
- `non-date`: category=Books, category=Clothing, category=Electronics, category=Home, category=Toys

**return_items** (1 partitions):
- `2023-01`: 2023-01-01

**returns** (1 partitions):
- `2023-01`: 2023-01-01

**shopping_carts** (6 partitions):
- `2020-03`: 2020-03-01
- `2023-01`: 2023-01-01
- `2024-01`: 2024-01-01, 2024-01-02, 2024-01-03
- `2025-10`: 2025-10-01

## Canonical Schema Keys

| Table | Canonical Schema Key | Sample Partitions |
| --- | --- | --- |
| cart_items | `cart_item_id:Int64|cart_id:String|product_id:Int64|product_name:String|category:String|added_at:String|quantity:Int64|unit_price:Float64|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-03-01, 2023-01-01, 2024-01-01, 2024-01-02, 2024-01-03 |
| customers | `customer_id:String|first_name:String|last_name:String|email:String|phone_number:String|signup_date:String|gender:String|age:Float64|is_guest:Boolean|customer_status:String|signup_channel:String|loyalty_tier:String|initial_loyalty_tier:String|email_verified:Boolean|marketing_opt_in:Boolean|mailing_address:String|billing_address:String|loyalty_enrollment_date:String|clv_bucket:String|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-03-01, 2023-01-01, 2025-10-01 |
| order_items | `order_id:String|product_id:Int64|product_name:String|category:String|quantity:Int64|unit_price:Float64|discount_amount:Float64|cost_price:Float64|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-03-01, 2023-01-01, 2024-01-01, 2024-01-02, 2024-01-03 |
| orders | `order_id:String|total_items:Int64|order_date:String|customer_id:String|email:String|order_channel:String|is_expedited:Boolean|customer_tier:String|gross_total:Float64|net_total:Float64|total_discount_amount:Float64|payment_method:String|shipping_speed:String|shipping_cost:Float64|agent_id:String|actual_shipping_cost:Float64|payment_processing_fee:Float64|shipping_address:String|billing_address:String|clv_bucket:String|is_reactivated:Boolean|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-03-01, 2023-01-01, 2024-01-01, 2024-01-02, 2024-01-03 |
| product_catalog | `product_id:Int64|product_name:String|category:String|unit_price:Float64|cost_price:Float64|inventory_quantity:Int64|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | Books, Clothing, Electronics, Home, Toys |
| return_items | `return_item_id:Int64|return_id:String|order_id:String|product_id:Int64|product_name:String|category:String|quantity_returned:Int64|unit_price:Float64|cost_price:Float64|refunded_amount:Float64|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2023-01-01 |
| returns | `return_id:String|order_id:String|customer_id:String|email:String|return_date:String|reason:String|return_type:String|refunded_amount:Float64|return_channel:String|agent_id:String|refund_method:String|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2023-01-01 |
| shopping_carts | `cart_id:String|customer_id:String|created_at:String|updated_at:String|cart_total:Float64|status:String|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-03-01, 2023-01-01, 2024-01-01, 2024-01-02, 2024-01-03 |

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
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T104546` (598) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T16:48:34+00:00` (598) |
| event_id | String | 0.0% | 598 | Top: `evt_38bd8d51c5f4ad7c74f65ab8dfcf4d49ec54765ec7c05ecef3dae6b3b212a590` (1), `evt_62220d5155bb967bebe9dcbf358d96fee0fdc9ac7efc3429ab0e17506bdaf048` (1), `evt_e237c63d597aa9e870d2b749432e384ad8e93526f2e1d53aaa0053792a87e9ce` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/product_catalog/category=Books/part-0000.parquet` (598) |

### return_items

**Sample**: `ingest_dt=2023-01-01` (615 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| return_item_id | Int64 | 0.0% | 615 | Range: `262` to `20739`<br>p25=4782.0, p50=10644.0, p75=15721.0, p95=19527.0 |
| return_id | String | 0.0% | 151 | Top: `RTN-00828082` (14), `RTN-00826048` (12), `RTN-00827804` (11) |
| order_id | String | 0.0% | 151 | Top: `ORD-03027133` (14), `ORD-03019942` (12), `ORD-03026232` (11) |
| product_id | Int64 | 0.0% | 567 | Range: `7` to `2999`<br>p25=825.0, p50=1554.0, p75=2340.0, p95=2886.0 |
| product_name | String | 0.0% | 147 | Top: `Rustic Rug` (14), `Compact Headphones` (14), `Rustic Table` (14) |
| category | String | 0.0% | 5 | Top: `Home` (149), `Electronics` (144), `Books` (113) |
| quantity_returned | Int64 | 0.0% | 6 | Range: `1` to `6`<br>p25=1.0, p50=2.0, p75=3.0, p95=5.0 |
| unit_price | Float64 | 0.0% | 561 | Range: `5.25` to `249.65`<br>p25=59.03, p50=130.07, p75=195.53, p95=239.25 |
| cost_price | Float64 | 0.0% | 560 | Range: `2.69` to `165.25`<br>p25=33.76, p50=70.17, p75=103.52, p95=140.73 |
| refunded_amount | Float64 | 0.0% | 596 | Range: `6.26` to `1491.48`<br>p25=93.7, p50=197.41, p75=385.5, p95=758.52 |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T140649` (615) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T22:54:41+00:00` (615) |
| event_id | String | 0.0% | 615 | Top: `evt_4b43f57680e2d59841752f7dc70152fa6073b596f6495555dea15d551d6b7eff` (1), `evt_093420196d7415768953d8e57e2cfe63465a273280be60605d63ee71467b347d` (1), `evt_51c763a2c4cf16c921033740990a6777d11b5f41564027bed0917efd20fd41cf` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/return_items/ingest_dt=2023-01-01/part-0000.parquet` (615) |

### returns

**Sample**: `ingest_dt=2023-01-01` (153 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| return_id | String | 0.0% | 153 | Top: `RTN-00825054` (1), `RTN-00825110` (1), `RTN-00825244` (1) |
| order_id | String | 0.0% | 153 | Top: `ORD-03016384` (1), `ORD-03016574` (1), `ORD-03017004` (1) |
| customer_id | String | 0.0% | 153 | Top: `CUST-106936` (1), `GUEST-114301` (1), `CUST-85405` (1) |
| email | String | 0.0% | 153 | Top: `ryan.khan@gmail.com` (1), `lesliemartinez@example.org` (1), `martin.walker@yahoo.com` (1) |
| return_date | String | 0.0% | 1 | Top: `2023-01-01` (153) |
| reason | String | 0.0% | 27 | Top: `No longer needed` (20), `Found a better price` (19), `Defective` (17) |
| return_type | String | 0.0% | 6 | Top: `Refund` (137), `refund` (5), `  Refund ` (4) |
| refunded_amount | Float64 | 0.0% | 152 | Range: `0.0` to `7937.01`<br>p25=301.06, p50=812.15, p75=1476.85, p95=2710.68 |
| return_channel | String | 0.0% | 18 | Top: `Web` (58), `Phone` (30), `Social Media` (23) |
| agent_id | String | 4.58% | 18 | Top: `ONLINE` (113), `None` (7), `CSR-0009` (5) |
| refund_method | String | 0.0% | 5 | Top: `Credit Card` (95), `PayPal` (27), `Apple Pay` (20) |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260111T140649` (153) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-11T22:54:41+00:00` (153) |
| event_id | String | 0.0% | 153 | Top: `evt_c32ba1378ea83d2f12b37130f04982422f9e320a2f937ff3255de649f18accf2` (1), `evt_637cb0e4c7a1f6b82ebb1d75535d6e94b142e963f5a6f562a63133d15aa9998d` (1), `evt_210b9e5f99319344e0f9329705a49fdf33d7e02c3d3411eaee4a9acfd8a43649` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/returns/ingest_dt=2023-01-01/part-0000.parquet` (153) |

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
Last updated (UTC): 2026-01-24T20:59:59Z
Content hash (SHA-256): c0c61d5e5f60158fc17417d4a4657276ec9f9166569678fb11d96b3ace99cda1
<!-- END GENERATED META -->
