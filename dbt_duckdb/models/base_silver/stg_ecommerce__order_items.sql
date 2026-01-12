{{ config(
    materialized='table',
    post_hook=[
        "COPY (SELECT * FROM {{ this }}) TO '{{ var('silver_base_path') }}/order_items' (FORMAT PARQUET, PARTITION_BY (order_dt), OVERWRITE_OR_IGNORE)"
    ]
) }}

{#
TRANSACTION TABLE PATTERN: order_items
- 2 FKs: order_id -> orders, product_id -> product_catalog
- NO unique ID - composite key (order_id, product_id)
- Validate: quantity > 0, prices >= 0
- NO partition (fact table without natural partition key)
#}

with raw as (
    select *
    from {{ source_parquet('bronze', 'order_items') }}
),

-- Reference dimensions for FK validation
dim_orders as (
    select distinct
        {{ normalize_string('order_id') }} as order_id,
        cast({{ safe_cast_timestamp('order_date') }} as date) as order_dt
    from {{ source_parquet('bronze', 'orders') }}
    where {{ normalize_string('order_id') }} is not null
),

dim_products as (
    select distinct
        {{ safe_cast_integer('product_id') }} as product_id
    from {{ source_parquet('bronze', 'product_catalog') }}
    where {{ safe_cast_integer('product_id') }} is not null
),

-- Stage 1: Clean and normalize all fields
cleaned as (
    select
        -- Composite key (no single unique ID)
        {{ normalize_string('order_id') }} as order_id,
        {{ safe_cast_integer('product_id') }} as product_id,

        -- Product attributes (denormalized for convenience)
        {{ normalize_string('product_name') }} as product_name,
        {{ normalize_string_lower('category') }} as category,

        -- Timestamps
        {{ safe_cast_timestamp('ingestion_ts') }} as ingestion_ts,

        -- Numeric fields
        {{ safe_cast_integer('quantity') }} as quantity,
        {{ safe_cast_decimal('unit_price', 18, 2) }} as unit_price,
        {{ safe_cast_decimal('discount_amount', 18, 2) }} as discount_amount,
        {{ safe_cast_decimal('cost_price', 18, 2) }} as cost_price,

        -- Lineage columns (required in all Base Silver)
        {{ normalize_string('batch_id') }} as batch_id,
        {{ normalize_string('event_id') }} as event_id,
        {{ normalize_string('source_file') }} as source_file

    from raw
),

-- Stage 2: Validate foreign keys and deduplicate
validated as (
    select
        cleaned.*,

        -- FK validation
        dim_orders.order_id is not null as order_fk_valid,
        dim_products.product_id is not null as product_fk_valid,
        coalesce(dim_orders.order_dt, cast(cleaned.ingestion_ts as date)) as order_dt,

        -- Deduplication: keep most recent record per (order_id, product_id)
        row_number() over (
            partition by cleaned.order_id, cleaned.product_id
            order by cleaned.ingestion_ts desc nulls last, cleaned.event_id desc
        ) as row_num

    from cleaned
    left join dim_orders
        on cleaned.order_id = dim_orders.order_id
    left join dim_products
        on cleaned.product_id = dim_products.product_id
),

-- Stage 3: Score validity and build invalid reasons
scored as (
    select
        *,

        -- Validation rules (ALL must be true for is_valid = true)
        (
            -- Required fields (composite key)
            {{ is_valid_id('order_id') }}
            and {{ is_positive_number('product_id') }}
            and {{ is_positive_number('quantity') }}
            and {{ is_non_negative_number('unit_price') }}

            -- Business rules
            and (discount_amount is null or discount_amount >= 0)
            and (cost_price is null or cost_price >= 0)

            -- FK integrity
            and (order_id is null or order_fk_valid)
            and (product_id is null or product_fk_valid)

            -- Deduplication
            and row_num = 1

        ) as is_valid,

        -- Build detailed invalid_reason for quarantine
        trim(concat_ws(' | ',
            case when not {{ is_valid_id('order_id') }} then 'missing_order_id' end,
            case when product_id is null or product_id <= 0 then 'invalid_product_id' end,
            case when quantity is null or quantity <= 0 then 'invalid_quantity' end,
            case when unit_price is null then 'missing_unit_price' end,
            case when unit_price < 0 then 'negative_unit_price' end,
            case when discount_amount < 0 then 'negative_discount' end,
            case when cost_price < 0 then 'negative_cost_price' end,
            case when order_id is not null and not order_fk_valid then 'order_fk_invalid' end,
            case when product_id is not null and not product_fk_valid then 'product_fk_invalid' end,
            case when row_num > 1 then 'duplicate_order_product' end
        )) as invalid_reason

    from validated
)

-- Final: Return only valid records
select
    order_id,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    discount_amount,
    cost_price,
    batch_id,
    ingestion_ts,
    event_id,
    source_file,
    order_dt
from scored
where is_valid
