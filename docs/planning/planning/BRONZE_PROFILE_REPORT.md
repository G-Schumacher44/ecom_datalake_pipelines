# Bronze Sample Profile Report

Generated from local parquet samples in `samples/bronze/`.

## Overview

- **Tables sampled**: 8
- **Partitions sampled**: 92
- **Total sample rows**: 7,690,289

### Per-Table Summary

| Table | Partitions | Sample Rows |
| --- | --- | --- |
| cart_items | 92 | 5,023,478 |
| customers | 92 | 552,000 |
| order_items | 92 | 750,234 |
| orders | 92 | 153,648 |
| product_catalog | 92 | 27,600 |
| return_items | 92 | 8,712 |
| returns | 92 | 2,248 |
| shopping_carts | 92 | 1,172,369 |

### Data Quality Flags

- ⚠️ **cart_items**: More product names than product IDs (possible duplicates/variations) (count=92, samples=cart_items:2020-06-01, cart_items:2020-06-02, cart_items:2020-06-03, cart_items:2020-06-04, cart_items:2020-06-05)
- ⚠️ **order_items**: More product names than product IDs (possible duplicates/variations) (count=85, samples=order_items:2020-06-01, order_items:2020-06-02, order_items:2020-06-03, order_items:2020-06-04, order_items:2020-06-05)
- 📊 **return_items** partition `2025-12-10`: 169 rows (+78% above average) (count=1, samples=return_items:2025-12-10)
- 📊 **return_items** partition `2025-12-11`: 169 rows (+78% above average) (count=1, samples=return_items:2025-12-11)
- 📊 **return_items** partition `2025-12-12`: 169 rows (+78% above average) (count=1, samples=return_items:2025-12-12)
- 📊 **return_items** partition `2025-12-13`: 169 rows (+78% above average) (count=1, samples=return_items:2025-12-13)
- 📊 **return_items** partition `2025-12-14`: 169 rows (+78% above average) (count=1, samples=return_items:2025-12-14)
- 📊 **return_items** partition `2025-12-15`: 169 rows (+78% above average) (count=1, samples=return_items:2025-12-15)
- 📊 **return_items** partition `2025-12-16`: 169 rows (+78% above average) (count=1, samples=return_items:2025-12-16)

## Schema Drift

- No schema drift detected across sampled partitions.

## Partition Coverage

Shows which partitions were sampled per table (for temporal schema drift detection).

**cart_items** (92 partitions):
- `2020-06`: 2020-06-01 ... 2020-06-30 (30 days)
- `2023-01`: 2023-01-01 ... 2023-01-31 (31 days)
- `2025-12`: 2025-12-01 ... 2025-12-31 (31 days)

**customers** (92 partitions):
- `2020-06`: 2020-06-01 ... 2020-06-30 (30 days)
- `2023-01`: 2023-01-01 ... 2023-01-31 (31 days)
- `2025-12`: 2025-12-01 ... 2025-12-31 (31 days)

**order_items** (92 partitions):
- `2020-06`: 2020-06-01 ... 2020-06-30 (30 days)
- `2023-01`: 2023-01-01 ... 2023-01-31 (31 days)
- `2025-12`: 2025-12-01 ... 2025-12-31 (31 days)

**orders** (92 partitions):
- `2020-06`: 2020-06-01 ... 2020-06-30 (30 days)
- `2023-01`: 2023-01-01 ... 2023-01-31 (31 days)
- `2025-12`: 2025-12-01 ... 2025-12-31 (31 days)

**product_catalog** (92 partitions):
- `2020-06`: 2020-06-01 ... 2020-06-30 (30 days)
- `2023-01`: 2023-01-01 ... 2023-01-31 (31 days)
- `2025-12`: 2025-12-01 ... 2025-12-31 (31 days)

**return_items** (92 partitions):
- `2020-06`: 2020-06-01 ... 2020-06-30 (30 days)
- `2023-01`: 2023-01-01 ... 2023-01-31 (31 days)
- `2025-12`: 2025-12-01 ... 2025-12-31 (31 days)

**returns** (92 partitions):
- `2020-06`: 2020-06-01 ... 2020-06-30 (30 days)
- `2023-01`: 2023-01-01 ... 2023-01-31 (31 days)
- `2025-12`: 2025-12-01 ... 2025-12-31 (31 days)

**shopping_carts** (92 partitions):
- `2020-06`: 2020-06-01 ... 2020-06-30 (30 days)
- `2023-01`: 2023-01-01 ... 2023-01-31 (31 days)
- `2025-12`: 2025-12-01 ... 2025-12-31 (31 days)

## Canonical Schema Keys

| Table | Canonical Schema Key | Sample Partitions |
| --- | --- | --- |
| cart_items | `cart_item_id:Int64|cart_id:String|product_id:Int64|product_name:String|category:String|added_at:String|quantity:Int64|unit_price:Float64|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-06-01, 2020-06-02, 2020-06-03, 2020-06-04, 2020-06-05 |
| customers | `customer_id:String|first_name:String|last_name:String|email:String|phone_number:String|signup_date:String|gender:String|age:Float64|is_guest:Boolean|customer_status:String|signup_channel:String|loyalty_tier:String|initial_loyalty_tier:String|email_verified:Boolean|marketing_opt_in:Boolean|mailing_address:String|billing_address:String|loyalty_enrollment_date:String|clv_bucket:String|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-06-01, 2020-06-02, 2020-06-03, 2020-06-04, 2020-06-05 |
| order_items | `order_id:String|product_id:Int64|product_name:String|category:String|quantity:Int64|unit_price:Float64|discount_amount:Float64|cost_price:Float64|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-06-01, 2020-06-02, 2020-06-03, 2020-06-04, 2020-06-05 |
| orders | `order_id:String|total_items:Int64|order_date:String|customer_id:String|email:String|order_channel:String|is_expedited:Boolean|customer_tier:String|gross_total:Float64|net_total:Float64|total_discount_amount:Float64|payment_method:String|shipping_speed:String|shipping_cost:Float64|agent_id:String|actual_shipping_cost:Float64|payment_processing_fee:Float64|shipping_address:String|billing_address:String|clv_bucket:String|is_reactivated:Boolean|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-06-01, 2020-06-02, 2020-06-03, 2020-06-04, 2020-06-05 |
| product_catalog | `product_id:Int64|product_name:String|category:String|unit_price:Float64|cost_price:Float64|inventory_quantity:Int64|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-06-01, 2020-06-02, 2020-06-03, 2020-06-04, 2020-06-05 |
| return_items | `return_item_id:Int64|return_id:String|order_id:String|product_id:Int64|product_name:String|category:String|quantity_returned:Int64|unit_price:Float64|cost_price:Float64|refunded_amount:Float64|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-06-01, 2020-06-02, 2020-06-03, 2020-06-04, 2020-06-05 |
| returns | `return_id:String|order_id:String|customer_id:String|email:String|return_date:String|reason:String|return_type:String|refunded_amount:Float64|return_channel:String|agent_id:String|refund_method:String|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-06-01, 2020-06-02, 2020-06-03, 2020-06-04, 2020-06-05 |
| shopping_carts | `cart_id:String|customer_id:String|created_at:String|updated_at:String|cart_total:Float64|status:String|batch_id:String|ingestion_ts:String|event_id:String|source_file:String` | 2020-06-01, 2020-06-02, 2020-06-03, 2020-06-04, 2020-06-05 |

## Column Statistics (Sample Partition)

Showing detailed stats for one representative partition per table.

### cart_items

**Sample**: `ingest_dt=2020-06-01` (47,020 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| cart_item_id | Int64 | 0.0% | 47,020 | Range: `1` to `53712`<br>p25=13639.0, p50=26910.0, p75=40110.0, p95=50896.0 |
| cart_id | String | 0.0% | 9,628 | Top: `CART-98913805` (15), `CART-07619411` (15), `CART-61439122` (15) |
| product_id | Int64 | 0.0% | 300 | Range: `1` to `300`<br>p25=75.0, p50=151.0, p75=227.0, p95=286.0 |
| product_name | String | 0.0% | 705 | Top: `Fun Car` (1098), `Modern Guide` (1082), `Compact Camera` (1078) |
| category | String | 0.0% | 5 | Top: `Home` (11696), `Electronics` (9387), `Books` (9305) |
| added_at | String | 4.0% | 45,063 | Top: `None` (1881), `2020-06-02T23:59:59.999999` (78), `2020-06-01T02:42:26.082846` (1) |
| quantity | Int64 | 0.0% | 6 | Range: `1` to `6`<br>p25=1.0, p50=2.0, p75=3.0, p95=5.0 |
| unit_price | Float64 | 0.0% | 299 | Range: `5.15` to `249.46`<br>p25=62.66, p50=129.56, p75=183.83, p95=229.51 |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260108T205055` (47020) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-09T03:32:50+00:00` (47020) |
| event_id | String | 0.0% | 47,020 | Top: `evt_c63b3145ff8b88de89d03797eb0e40eb341320edeaa660d887ed706c8e870236` (1), `evt_96653397f4b6848484ada84a5eecc43316e80b9f5f07b2a5914cfbda8dc5beab` (1), `evt_30b01b49796da4381377378700572270a952c1deb2ff71c3fad51259d428fcbd` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/cart_items/ingest_dt=2020-06-01/part-0000.parquet` (47020) |

### customers

**Sample**: `ingest_dt=2020-06-01` (6,000 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| customer_id | String | 0.0% | 6,000 | Top: `CUST-2118` (1), `CUST-2119` (1), `CUST-2120` (1) |
| first_name | String | 40.0% | 547 | Top: `None` (2400), `Michael` (101), `James` (56) |
| last_name | String | 40.0% | 838 | Top: `None` (2400), `Smith` (91), `Johnson` (64) |
| email | String | 0.0% | 5,471 | Top: `james73@example.net` (4), `hannah68@example.net` (4), `davidruiz@example.net` (4) |
| phone_number | String | 40.0% | 3,601 | Top: `None` (2400), `+1-366-913-9176x0788` (1), `336.978.7965x627` (1) |
| signup_date | String | 40.0% | 367 | Top: `None` (2400), `2019-07-24` (20), `2020-02-13` (18) |
| gender | String | 40.0% | 4 | Top: `None` (2400), `Unknown` (1306), `Male` (1163) |
| age | Float64 | 40.0% | 54 | Range: `18.0` to `70.0`<br>p25=31.0, p50=43.0, p75=57.0, p95=68.0 |
| is_guest | Boolean | 0.0% | 2 | — |
| customer_status | String | 0.0% | 42 | Top: `Active` (2218), `Guest` (2097), `Inactive` (639) |
| signup_channel | String | 40.0% | 5 | Top: `None` (2400), `Website` (1925), `email` (726) |
| loyalty_tier | String | 40.0% | 5 | Top: `Bronze` (2682), `None` (2400), `Platinum` (562) |
| initial_loyalty_tier | String | 46.08% | 5 | Top: `None` (2765), `Bronze` (1221), `Silver` (1013) |
| email_verified | Boolean | 4.42% | 3 | — |
| marketing_opt_in | Boolean | 3.88% | 3 | — |
| mailing_address | String | 0.0% | 5,510 | Top: `4213 Stone Brook Apt. 947, Williamsfurt, IA 88167` (4), `109 Austin Union, Perezport, IA 12354` (4), `7361 Taylor Trail Suite 690, Brianbury, ID 93859` (4) |
| billing_address | String | 0.0% | 5,510 | Top: `4213 Stone Brook Apt. 947, Williamsfurt, IA 88167` (4), `109 Austin Union, Perezport, IA 12354` (4), `7361 Taylor Trail Suite 690, Brianbury, ID 93859` (4) |
| loyalty_enrollment_date | String | 46.08% | 330 | Top: `None` (2765), `2020-06-02` (61), `2020-06-01` (54) |
| clv_bucket | String | 40.0% | 4 | Top: `Low` (2746), `None` (2400), `High` (646) |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260108T205055` (6000) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-09T03:32:51+00:00` (6000) |
| event_id | String | 0.0% | 6,000 | Top: `evt_886bf3ab8ae340bb8c7efc04400dad42b7a70fb9568c310961947a1508b3b275` (1), `evt_1884eac4028e923dd3933884cbd06bacf90d6f5d861220e282553fff1eba29c6` (1), `evt_1ea13304373391ee17fd3e901df7935ea58ee2fd98faa12628545d6f0984d175` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/customers/ingest_dt=2020-06-01/part-0000.parquet` (6000) |

### order_items

**Sample**: `ingest_dt=2020-06-01` (7,114 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| order_id | String | 0.0% | 1,468 | Top: `ORD-18013024` (15), `ORD-18813765` (15), `ORD-44968229` (15) |
| product_id | Int64 | 0.0% | 300 | Range: `1` to `300`<br>p25=74.0, p50=151.0, p75=229.0, p95=287.0 |
| product_name | String | 0.0% | 474 | Top: `Fun Car` (183), `Modern Guide` (163), `Elegant Lamp` (159) |
| category | String | 0.0% | 5 | Top: `Home` (1797), `Electronics` (1383), `Books` (1379) |
| quantity | Int64 | 0.0% | 12 | Range: `1` to `12`<br>p25=1.0, p50=2.0, p75=3.0, p95=5.0 |
| unit_price | Float64 | 0.0% | 299 | Range: `5.15` to `249.46`<br>p25=62.98, p50=130.71, p75=185.27, p95=226.84 |
| discount_amount | Float64 | 0.0% | 1,348 | Range: `0.0` to `316.63`<br>p25=0.0, p50=0.0, p75=0.0, p95=59.36 |
| cost_price | Float64 | 0.0% | 298 | Range: `2.9` to `161.95`<br>p25=33.93, p50=69.48, p75=98.82, p95=137.74 |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260108T205055` (7114) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-09T03:32:51+00:00` (7114) |
| event_id | String | 0.0% | 7,114 | Top: `evt_f654e1e93e70b0999300a4a650fe7ac5a6663885c00494576fb2212323e1a2a7` (1), `evt_25c69a597c754d2922584f3401ff039a9680485c0a6f9302dac4b123e3df990a` (1), `evt_e3d0a15a60ff2667127ee7b947590b7b016f29a6c671e7f437849a575037008d` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/order_items/ingest_dt=2020-06-01/part-0000.parquet` (7114) |

### orders

**Sample**: `ingest_dt=2020-06-01` (1,468 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| order_id | String | 0.0% | 1,468 | Top: `ORD-88123198` (1), `ORD-07060831` (1), `ORD-40027248` (1) |
| total_items | Int64 | 0.0% | 20 | Range: `1` to `22`<br>p25=3.0, p50=6.0, p75=8.0, p95=12.0 |
| order_date | String | 0.0% | 1,468 | Top: `2020-05-27 00:18:33.392705` (1), `2020-05-27 00:25:47.224025` (1), `2020-05-27 00:33:44.248833` (1) |
| customer_id | String | 0.0% | 1,360 | Top: `CUST-2208` (3), `CUST-5136` (2), `CUST-3598` (2) |
| email | String | 0.0% | 1,342 | Top: `kevin.wood@hotmail.com` (3), `tammy11@example.org` (3), `kara.flores@hotmail.com` (2) |
| order_channel | String | 0.0% | 5 | Top: `Web` (661), `Phone` (274), `Social Media` (238) |
| is_expedited | Boolean | 0.0% | 2 | — |
| customer_tier | String | 0.0% | 4 | Top: `Platinum` (836), `Gold` (314), `Silver` (271) |
| gross_total | Float64 | 0.0% | 1,441 | Range: `7.18` to `8425.07`<br>p25=551.62, p50=1124.59, p75=1909.94, p95=4135.7 |
| net_total | Float64 | 0.0% | 1,452 | Range: `7.18` to `8348.75`<br>p25=531.63, p50=1090.97, p75=1840.78, p95=3943.45 |
| total_discount_amount | Float64 | 0.0% | 859 | Range: `0.0` to `520.44`<br>p25=0.0, p50=16.72, p75=59.76, p95=189.06 |
| payment_method | String | 0.0% | 40 | Top: `Credit Card` (750), `PayPal` (261), `Apple Pay` (138) |
| shipping_speed | String | 0.0% | 23 | Top: `Standard` (822), `Two-Day` (269), `Overnight` (189) |
| shipping_cost | Float64 | 0.0% | 3 | Range: `5.0` to `80.0`<br>p25=5.0, p50=5.0, p75=45.0, p95=80.0 |
| agent_id | String | 3.34% | 22 | Top: `ONLINE` (1153), `None` (49), `CSR-0018` (20) |
| actual_shipping_cost | Float64 | 0.0% | 594 | Range: `3.83` to `76.96`<br>p25=4.26, p50=4.68, p75=37.45, p95=71.38 |
| payment_processing_fee | Float64 | 0.0% | 1,331 | Range: `0.18` to `240.38`<br>p25=12.41, p50=26.06, p75=45.0, p95=103.6 |
| shipping_address | String | 0.0% | 1,342 | Top: `37763 Ryan Pines Apt. 852, South Keith, SC 55463` (3), `0843 Thomas Wall, Davishaven, WI 70004` (3), `PSC 9814, Box 9672, APO AA 02689` (2) |
| billing_address | String | 0.0% | 1,342 | Top: `37763 Ryan Pines Apt. 852, South Keith, SC 55463` (3), `0843 Thomas Wall, Davishaven, WI 70004` (3), `PSC 9814, Box 9672, APO AA 02689` (2) |
| clv_bucket | String | 0.0% | 23 | Top: `High` (878), `Medium` (302), `Low` (123) |
| is_reactivated | Boolean | 0.0% | 1 | — |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260108T205055` (1468) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-09T03:32:51+00:00` (1468) |
| event_id | String | 0.0% | 1,468 | Top: `evt_a879180ca126ac4fb89095921d1b4f74da2fdbc74c2210392b1d290801d7d44c` (1), `evt_1ec33daccb85d3d1085276a2c60b403a573ce053cb3a549efc698b51b305aa82` (1), `evt_9c8018c21a6361b07253ec81fef3ed219ae6906f8f7271dbed8992d471727f47` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/orders/ingest_dt=2020-06-01/part-0000.parquet` (1468) |

### product_catalog

**Sample**: `ingest_dt=2020-06-01` (300 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| product_id | Int64 | 0.0% | 300 | Range: `1` to `300`<br>p25=76.0, p50=151.0, p75=225.0, p95=285.0 |
| product_name | String | 0.0% | 109 | Top: `Classic Guide` (9), `Modern Guide` (9), `Hardcover Anthology` (8) |
| category | String | 0.0% | 5 | Top: `Books` (76), `Clothing` (62), `Home` (60) |
| unit_price | Float64 | 0.0% | 299 | Range: `5.15` to `249.46`<br>p25=62.66, p50=129.56, p75=183.58, p95=229.51 |
| cost_price | Float64 | 0.0% | 298 | Range: `2.9` to `161.95`<br>p25=33.93, p50=68.05, p75=98.42, p95=137.74 |
| inventory_quantity | Int64 | 0.0% | 126 | Range: `100` to `250`<br>p25=145.0, p50=179.0, p75=213.0, p95=241.0 |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260108T205055` (300) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-09T03:32:51+00:00` (300) |
| event_id | String | 0.0% | 300 | Top: `evt_083b0123420c531a1689c403477ff329a8c8d708b101d46a5ed11ea880be4efe` (1), `evt_000cf56691e48d207191bd9e8a6421a6221bfbe762f91430f2bbc8de87370a6e` (1), `evt_8e447f5cd59e9c29852d9747c882d5fe50a84cd17a9716387fee48c6ca12681a` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/product_catalog/ingest_dt=2020-06-01/part-0000.parquet` (300) |

### return_items

**Sample**: `ingest_dt=2020-06-01` (95 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| return_item_id | Int64 | 0.0% | 95 | Range: `1` to `95`<br>p25=25.0, p50=48.0, p75=72.0, p95=90.0 |
| return_id | String | 0.0% | 22 | Top: `RTN-52655895` (7), `RTN-55886097` (7), `RTN-29388231` (7) |
| order_id | String | 0.0% | 22 | Top: `ORD-56604380` (7), `ORD-42177429` (7), `ORD-62331461` (7) |
| product_id | Int64 | 0.0% | 87 | Range: `4` to `297`<br>p25=80.0, p50=151.0, p75=232.0, p95=284.0 |
| product_name | String | 0.0% | 57 | Top: `Modern Rug` (4), `Elegant Lamp` (3), `Elegant Chair` (3) |
| category | String | 0.0% | 5 | Top: `Home` (28), `Toys` (21), `Clothing` (18) |
| quantity_returned | Int64 | 0.0% | 6 | Range: `1` to `6`<br>p25=1.0, p50=2.0, p75=3.0, p95=3.0 |
| unit_price | Float64 | 0.0% | 87 | Range: `5.15` to `246.73`<br>p25=59.44, p50=118.33, p75=180.06, p95=229.51 |
| cost_price | Float64 | 0.0% | 87 | Range: `2.9` to `160.56`<br>p25=29.12, p50=62.18, p75=98.78, p95=142.42 |
| refunded_amount | Float64 | 0.0% | 93 | Range: `5.15` to `986.92`<br>p25=94.46, p50=159.28, p75=326.43, p95=597.6 |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260108T205055` (95) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-09T03:32:51+00:00` (95) |
| event_id | String | 0.0% | 95 | Top: `evt_85f87958781cc1b68721b4dbc7bfd2a332c9102cfd42f173fdf3bb211a7b883b` (1), `evt_00e5a1b81993aad9157dc2274ec3a19906edd8aede93026844a3d7d542414c2b` (1), `evt_f3ebc7ed1304c8ff6e6529a8608fa018ced5d872cbc5a65becbf3491343bd1d0` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/return_items/ingest_dt=2020-06-01/part-0000.parquet` (95) |

### returns

**Sample**: `ingest_dt=2020-06-01` (22 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| return_id | String | 0.0% | 22 | Top: `RTN-52655895` (1), `RTN-00032183` (1), `RTN-51119095` (1) |
| order_id | String | 0.0% | 22 | Top: `ORD-56604380` (1), `ORD-39655127` (1), `ORD-15447257` (1) |
| customer_id | String | 0.0% | 22 | Top: `GUEST-100146` (1), `CUST-3548` (1), `CUST-3181` (1) |
| email | String | 0.0% | 22 | Top: `tchapman@example.org` (1), `nicole.novak@gmail.com` (1), `kevin.thompson@gmail.com` (1) |
| return_date | String | 0.0% | 5 | Top: `2020-06-02` (7), `2020-05-31` (5), `2020-05-29` (4) |
| reason | String | 0.0% | 12 | Top: `Arrived damaged` (4), `Found a better price` (3), `Product did not match description` (3) |
| return_type | String | 0.0% | 4 | Top: `Refund` (18), `  Refund  ` (2), `refund` (1) |
| refunded_amount | Float64 | 0.0% | 22 | Range: `7.18` to `2441.72`<br>p25=432.99, p50=879.14, p75=1521.38, p95=2287.65 |
| return_channel | String | 0.0% | 6 | Top: `Ebay` (7), `Web` (7), `Phone` (4) |
| agent_id | String | 4.55% | 5 | Top: `ONLINE` (17), `CSR-0010` (2), `None` (1) |
| refund_method | String | 0.0% | 4 | Top: `Credit Card` (12), `PayPal` (5), `Google Pay` (3) |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260108T205055` (22) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-09T03:32:51+00:00` (22) |
| event_id | String | 0.0% | 22 | Top: `evt_eaab1d91ff28ed125b57ac7bb7859e8c4c5bc4edfe6e9e403578a747fb7aa0a4` (1), `evt_fc88107997c7cf204f82d36e1c77d49bb83e4d9d505f84103fedfc92482beb13` (1), `evt_81800e296528e7b548b410ab032a0705af7c87bfa5c98e570ceeb1dd3d98bb8a` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/returns/ingest_dt=2020-06-01/part-0000.parquet` (22) |

### shopping_carts

**Sample**: `ingest_dt=2020-06-01` (10,978 rows)

| Column | Type | Null % | Distinct | Stats |
| --- | --- | --- | --- | --- |
| cart_id | String | 0.0% | 10,978 | Top: `CART-88553227` (1), `CART-17121684` (1), `CART-08575377` (1) |
| customer_id | String | 0.0% | 6,000 | Top: `CUST-4381` (6), `CUST-3788` (6), `CUST-4053` (6) |
| created_at | String | 0.0% | 10,978 | Top: `2020-06-01T02:40:32.082846` (1), `2020-05-27T17:17:22.556119` (1), `2020-05-31T11:20:29.249210` (1) |
| updated_at | String | 3.74% | 10,545 | Top: `None` (411), `2020-06-02T23:59:59.999999` (24), `2020-06-01T02:58:11.082846` (1) |
| cart_total | Float64 | 0.0% | 9,039 | Range: `0.0` to `10624.95`<br>p25=358.56, p50=931.02, p75=1715.67, p95=3861.59 |
| status | String | 0.0% | 38 | Top: `abandoned` (7076), `converted` (1275), `emptied` (1169) |
| batch_id | String | 0.0% | 1 | Top: `backlog-20260108T205055` (10978) |
| ingestion_ts | String | 0.0% | 1 | Top: `2026-01-09T03:32:52+00:00` (10978) |
| event_id | String | 0.0% | 10,978 | Top: `evt_675699ae7d7b1ae8044ff976e2a52112af60d9bb0bd754b9cddc39e769b41e44` (1), `evt_6aa31e58df9313a6e80e24db0f95e858ad91e392b666203ca65065e195e9eef3` (1), `evt_a944fd14f79af1783254aae24cc7d5e3b12ded3ed39119ad92faeb4cd7b70567` (1) |
| source_file | String | 0.0% | 1 | Top: `gs://acme-analytics-raw/ecom/raw/shopping_carts/ingest_dt=2020-06-01/part-0000.parquet` (10978) |
