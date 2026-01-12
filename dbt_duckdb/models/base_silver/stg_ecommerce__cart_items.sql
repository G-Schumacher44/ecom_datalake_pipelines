{{ config(
    materialized='table',
    post_hook=[
        "COPY (SELECT * FROM {{ this }}) TO '{{ var('silver_base_path') }}/cart_items' (FORMAT PARQUET, PARTITION_BY (added_dt), OVERWRITE_OR_IGNORE)"
    ]
) }}

{#
TRANSACTION TABLE PATTERN: cart_items
- 2 FKs: cart_id -> shopping_carts, product_id -> product_catalog
- Validate: quantity > 0, unit_price >= 0
- Partition by added_dt
#}

with raw as (
    select *
    from {{ source_parquet('bronze', 'cart_items') }}
),

-- Reference dimensions for FK validation
dim_shopping_carts as (
    select distinct
        {{ normalize_string('cart_id') }} as cart_id
    from {{ source_parquet('bronze', 'shopping_carts') }}
    where {{ normalize_string('cart_id') }} is not null
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
        -- Primary key
        {{ safe_cast_integer('cart_item_id') }} as cart_item_id,

        -- Foreign keys
        {{ normalize_string('cart_id') }} as cart_id,
        {{ safe_cast_integer('product_id') }} as product_id,

        -- Product attributes (denormalized for convenience)
        {{ normalize_string('product_name') }} as product_name,
        {{ normalize_string_lower('category') }} as category,

        -- Timestamps
        {{ safe_cast_timestamp('added_at') }} as added_at,
        {{ safe_cast_timestamp('ingestion_ts') }} as ingestion_ts,

        -- Numeric fields
        {{ safe_cast_integer('quantity') }} as quantity,
        {{ safe_cast_decimal('unit_price', 18, 2) }} as unit_price,

        -- Lineage columns (required in all Base Silver)
        {{ normalize_string('batch_id') }} as batch_id,
        {{ normalize_string('event_id') }} as event_id,
        {{ normalize_string('source_file') }} as source_file,

        -- Derived: partition column
        cast({{ safe_cast_timestamp('added_at') }} as date) as added_dt

    from raw
),

-- Stage 2: Validate foreign keys and deduplicate
validated as (
    select
        cleaned.*,

        -- FK validation
        dim_shopping_carts.cart_id is not null as cart_fk_valid,
        dim_products.product_id is not null as product_fk_valid,

        -- Deduplication: keep most recent record per cart_id/product_id/added_at
        row_number() over (
            partition by cleaned.cart_id, cleaned.product_id, cleaned.added_at
            order by cleaned.ingestion_ts desc nulls last, cleaned.event_id desc
        ) as row_num

    from cleaned
    left join dim_shopping_carts
        on cleaned.cart_id = dim_shopping_carts.cart_id
    left join dim_products
        on cleaned.product_id = dim_products.product_id
),

-- Stage 3: Score validity and build invalid reasons
scored as (
    select
        *,

        -- Validation rules (ALL must be true for is_valid = true)
        (
            -- Required fields
            {{ is_positive_number('cart_item_id') }}
            and {{ is_valid_id('cart_id') }}
            and {{ is_positive_number('product_id') }}
            and {{ is_positive_number('quantity') }}
            and {{ is_non_negative_number('unit_price') }}

            -- FK integrity
            and (cart_id is null or cart_fk_valid)
            and (product_id is null or product_fk_valid)

            -- Deduplication
            and row_num = 1

        ) as is_valid,

        -- Build detailed invalid_reason for quarantine
        trim(concat_ws(' | ',
            case when cart_item_id is null or cart_item_id <= 0 then 'invalid_cart_item_id' end,
            case when not {{ is_valid_id('cart_id') }} then 'missing_cart_id' end,
            case when product_id is null or product_id <= 0 then 'invalid_product_id' end,
            case when quantity is null or quantity <= 0 then 'invalid_quantity' end,
            case when unit_price is null then 'missing_unit_price' end,
            case when unit_price < 0 then 'negative_unit_price' end,
            case when cart_id is not null and not cart_fk_valid then 'cart_fk_invalid' end,
            case when product_id is not null and not product_fk_valid then 'product_fk_invalid' end,
            case when row_num > 1 then 'duplicate_cart_item_line' end
        )) as invalid_reason

    from validated
)

-- Final: Return only valid records
select
    cart_item_id,
    cart_id,
    product_id,
    product_name,
    category,
    added_at,
    quantity,
    unit_price,
    batch_id,
    ingestion_ts,
    event_id,
    source_file,
    added_dt
from scored
where is_valid
