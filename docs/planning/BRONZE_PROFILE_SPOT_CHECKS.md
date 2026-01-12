# Bronze Profile Spot Checks

Generated from local parquet samples in `samples/bronze/`.

## Returns and Return Items: ID Reuse

| Table | Column | Rows | Distinct | Top Repeats Across Partitions |
| --- | --- | --- | --- | --- |
| returns | return_id | 2,248 | 2,248 | — |
| returns | order_id | 2,248 | 2,191 | ORD-00863489(2), ORD-00438101(2), ORD-00438946(2), ORD-00866457(2), ORD-00868785(2) |
| returns | customer_id | 2,248 | 2,128 | CUST-102618(3), CUST-64069(3), CUST-91947(3), CUST-114742(3), GUEST-123052(3) |
| return_items | return_item_id | 8,756 | 5,664 | 929(4), 1465(4), 545(4), 1197(4), 146(4) |
| return_items | return_id | 8,756 | 2,199 | — |
| return_items | order_id | 8,756 | 2,180 | ORD-00438946(2), ORD-01281759(2), ORD-00437945(2), ORD-00868785(2), ORD-00438101(2) |
| return_items | product_id | 8,756 | 2,833 | 2752(12), 1608(10), 1150(10), 976(10), 2598(9) |

## Returns Partition Coverage

| Table | Month | Expected Days | Observed Days | Missing Days |
| --- | --- | --- | --- | --- |
| returns | 2020-03 | 31 | 28 | 2020-03-01, 2020-03-02, 2020-03-31 |
| returns | 2023-01 | 31 | 30 | 2023-01-15 |
| returns | 2025-10 | 31 | 29 | 2025-10-01, 2025-10-31 |
| return_items | 2020-03 | 31 | 28 | 2020-03-01, 2020-03-02, 2020-03-31 |
| return_items | 2023-01 | 31 | 30 | 2023-01-15 |
| return_items | 2025-10 | 31 | 29 | 2025-10-01, 2025-10-31 |

## cart_items Spike Validation

- Partitions: 93; avg rows: 11787.73; median rows: 7806

| Partition | Rows | Files | Pct vs Avg |
| --- | --- | --- | --- |
| 2023-01-14 | 57,345 | 1 | 386% |
| 2020-03-30 | 56,832 | 1 | 382% |
| 2025-10-30 | 55,841 | 1 | 374% |
| 2020-03-29 | 34,146 | 1 | 190% |
| 2023-01-13 | 33,423 | 1 | 184% |
| 2025-10-29 | 33,233 | 1 | 182% |
| 2020-03-28 | 25,864 | 1 | 119% |
| 2025-10-28 | 25,585 | 1 | 117% |
| 2023-01-12 | 25,370 | 1 | 115% |
| 2023-01-11 | 21,314 | 1 | 81% |

### Cross-Table Alignment (Top Spikes)

| Partition | cart_items | shopping_carts | orders |
| --- | --- | --- | --- |
| 2023-01-14 | 57,345 | 12,560 | 1,689 |
| 2020-03-30 | 56,832 | 12,363 | 1,645 |
| 2025-10-30 | 55,841 | 12,352 | 1,612 |
| 2020-03-29 | 34,146 | 7,733 | 1,067 |
| 2023-01-13 | 33,423 | 7,673 | 993 |

### cart_items Distribution by cart_id (Top Spikes + Baselines)

| Partition | Carts | Items | Mean | Median | P90 | P95 | P99 | Max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2020-03-29 | 6721 | 34146 | 5.08 | 5 | 9 | 11 | 14 | 15 |
| 2020-03-30 | 10833 | 56832 | 5.25 | 5 | 9 | 11 | 14 | 15 |
| 2020-03-31 | 1131 | 5137 | 4.54 | 4 | 8 | 9 | 13 | 15 |
| 2023-01-13 | 6646 | 33423 | 5.03 | 5 | 9 | 10 | 14 | 15 |
| 2023-01-14 | 10984 | 57345 | 5.22 | 5 | 9 | 11 | 14 | 15 |
| 2025-10-15 | 1705 | 7806 | 4.58 | 4 | 8 | 8 | 13 | 15 |
| 2025-10-30 | 10705 | 55841 | 5.22 | 5 | 9 | 11 | 14 | 15 |

### cart_items Duplicate Checks (Top Spikes + Baselines)

| Partition | cart_item_id dupes | line_key dupes |
| --- | --- | --- |
| 2020-03-29 | 0 | 0 |
| 2020-03-30 | 0 | 1 |
| 2020-03-31 | 0 | 0 |
| 2023-01-13 | 0 | 0 |
| 2023-01-14 | 0 | 1 |
| 2025-10-15 | 0 | 0 |
| 2025-10-30 | 0 | 0 |
<!-- GENERATED META -->
Last updated (UTC): 2026-01-11T15:57:19Z
Content hash (SHA-256): 0f9df0f79bbf36ed485c65e96b69c6539c5290d805fe00f36de5b8cc3c768d33
Profile report hash (SHA-256): 6f86ddd7997da133b7380023b9efa456146be9bba8279ffc0be37ec6c0c5c02f
<!-- END GENERATED META -->
