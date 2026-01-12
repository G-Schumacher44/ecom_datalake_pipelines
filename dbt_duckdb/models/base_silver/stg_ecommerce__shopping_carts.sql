{{ config(
    materialized='table',
    post_hook=[
        "COPY (SELECT * FROM {{ this }}) TO '{{ var('silver_base_path') }}/shopping_carts' (FORMAT PARQUET, PARTITION_BY (created_dt), OVERWRITE_OR_IGNORE)"
    ]
) }}

{% set enforce_customer_fk = strict_fk() %}

{#
TRANSACTION TABLE PATTERN: shopping_carts
- 1 FK: customer_id -> customers
- Validate: created_at <= updated_at, cart_total >= 0
- Partition by created_dt
#}

with raw as (
    select *
    from {{ source_parquet('bronze', 'shopping_carts') }}
),

{% if enforce_customer_fk %}
-- Reference dimension: customers for FK validation
dim_customers as (
    select distinct
        {{ normalize_string('customer_id') }} as customer_id
    from {{ source_parquet('bronze', 'customers') }}
    where {{ normalize_string('customer_id') }} is not null
),
{% endif %}

-- Stage 1: Clean and normalize all fields
cleaned as (
    select
        -- Primary key
        {{ normalize_string('cart_id') }} as cart_id,

        -- Foreign keys
        {{ normalize_string('customer_id') }} as customer_id,

        -- Timestamps
        {{ safe_cast_timestamp('created_at') }} as created_at,
        {{ safe_cast_timestamp('updated_at') }} as updated_at,
        {{ safe_cast_timestamp('ingestion_ts') }} as ingestion_ts,

        -- Numeric fields
        {{ safe_cast_decimal('cart_total', 18, 2) }} as cart_total,

        -- String fields (lowercase normalized)
        {{ normalize_string_lower('status') }} as status,

        -- Lineage columns (required in all Base Silver)
        {{ normalize_string('batch_id') }} as batch_id,
        {{ normalize_string('event_id') }} as event_id,
        {{ normalize_string('source_file') }} as source_file,

        -- Derived: partition column
        cast({{ safe_cast_timestamp('created_at') }} as date) as created_dt

    from raw
),

-- Stage 2: Validate foreign keys and deduplicate
validated as (
    select
        cleaned.*,

        {% if enforce_customer_fk %}
        -- FK validation (strict mode)
        dim_customers.customer_id is not null as customer_fk_valid,
        {% else %}
        -- FK validation disabled (soft mode)
        true as customer_fk_valid,
        {% endif %}

        -- Deduplication: keep most recent record per cart_id
        row_number() over (
            partition by cleaned.cart_id
            order by cleaned.ingestion_ts desc nulls last, cleaned.event_id desc
        ) as row_num

    from cleaned
    {% if enforce_customer_fk %}
    left join dim_customers
        on cleaned.customer_id = dim_customers.customer_id
    {% endif %}
),

-- Stage 3: Score validity and build invalid reasons
scored as (
    select
        *,

        -- Validation rules (ALL must be true for is_valid = true)
        (
            -- Required fields
            {{ is_valid_id('cart_id') }}
            and {{ is_valid_id('customer_id') }}
            and {{ is_valid_timestamp('created_at') }}

            -- Business rules
            and (cart_total is null or cart_total >= 0)
            and (updated_at is null or created_at is null or updated_at >= created_at)

            {% if enforce_customer_fk %}
            -- FK integrity
            and (customer_id is null or customer_fk_valid)
            {% endif %}

            -- Deduplication
            and row_num = 1

        ) as is_valid,

        -- Build detailed invalid_reason for quarantine
        trim(concat_ws(' | ',
            case when not {{ is_valid_id('cart_id') }} then 'missing_cart_id' end,
            case when not {{ is_valid_id('customer_id') }} then 'missing_customer_id' end,
            case when not {{ is_valid_timestamp('created_at') }} then 'invalid_created_at' end,
            case when cart_total < 0 then 'negative_cart_total' end,
            case when updated_at < created_at and updated_at is not null and created_at is not null
                 then 'updated_before_created' end,
            {% if enforce_customer_fk %}
            case when customer_id is not null and not customer_fk_valid then 'customer_fk_invalid' end,
            {% endif %}
            case when row_num > 1 then 'duplicate_cart_id' end
        )) as invalid_reason

    from validated
)

-- Final: Return only valid records
select
    cart_id,
    customer_id,
    created_at,
    updated_at,
    cart_total,
    status,
    batch_id,
    ingestion_ts,
    event_id,
    source_file,
    created_dt
from scored
where is_valid
