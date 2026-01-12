{{ config(
    materialized='table',
    post_hook=[
        "COPY (SELECT * FROM {{ this }}) TO '{{ var('silver_base_path') }}/returns' (FORMAT PARQUET, PARTITION_BY (return_dt), OVERWRITE_OR_IGNORE)"
    ]
) }}

{% set enforce_fk = strict_fk() %}

{#
TRANSACTION TABLE PATTERN: returns
- 2 FKs: order_id -> orders, customer_id -> customers
- Validate: refunded_amount >= 0
- Partition by return_dt
#}

with raw as (
    select *
    from {{ source_parquet('bronze', 'returns') }}
),

-- Reference dimensions for FK validation
dim_orders as (
    select distinct
        {{ normalize_string('order_id') }} as order_id
    from {{ source_parquet('bronze', 'orders') }}
    where {{ normalize_string('order_id') }} is not null
),

dim_customers as (
    select distinct
        {{ normalize_string('customer_id') }} as customer_id
    from {{ source_parquet('bronze', 'customers') }}
    where {{ normalize_string('customer_id') }} is not null
),

-- Stage 1: Clean and normalize all fields
cleaned as (
    select
        -- Primary key
        {{ normalize_string('return_id') }} as return_id,

        -- Foreign keys
        {{ normalize_string('order_id') }} as order_id,
        {{ normalize_string('customer_id') }} as customer_id,

        -- Timestamps
        {{ safe_cast_timestamp('return_date') }} as return_date,
        {{ safe_cast_timestamp('ingestion_ts') }} as ingestion_ts,

        -- Numeric fields
        {{ safe_cast_decimal('refunded_amount', 18, 2) }} as refunded_amount,

        -- String fields (lowercase normalized)
        {{ normalize_string_lower('email') }} as email,
        {{ normalize_string_lower('return_channel') }} as return_channel,
        {{ normalize_string_lower('refund_method') }} as refund_method,
        {{ normalize_string_lower('return_type') }} as return_type,

        -- String fields (case-preserved)
        {{ normalize_string('reason') }} as reason,
        {{ normalize_string('agent_id') }} as agent_id,

        -- Lineage columns (required in all Base Silver)
        {{ normalize_string('batch_id') }} as batch_id,
        {{ normalize_string('event_id') }} as event_id,
        {{ normalize_string('source_file') }} as source_file,

        -- Derived: partition column
        cast({{ safe_cast_timestamp('return_date') }} as date) as return_dt

    from raw
),

-- Stage 2: Validate foreign keys and deduplicate
validated as (
    select
        cleaned.*,

        -- FK validation
        dim_orders.order_id is not null as order_fk_valid,
        dim_customers.customer_id is not null as customer_fk_valid,

        -- Deduplication: keep most recent record per return_id
        row_number() over (
            partition by cleaned.return_id
            order by cleaned.ingestion_ts desc nulls last, cleaned.event_id desc
        ) as row_num

    from cleaned
    left join dim_orders
        on cleaned.order_id = dim_orders.order_id
    left join dim_customers
        on cleaned.customer_id = dim_customers.customer_id
),

-- Stage 3: Score validity and build invalid reasons
scored as (
    select
        *,

        -- Validation rules (ALL must be true for is_valid = true)
        (
            -- Required fields
            {{ is_valid_id('return_id') }}
            and {{ is_valid_id('order_id') }}
            and {{ is_valid_id('customer_id') }}
            and {{ is_valid_timestamp('return_date') }}

            -- Business rules
            and (refunded_amount is null or refunded_amount >= 0)

            -- FK integrity
            and (not {{ enforce_fk }} or (order_id is null or order_fk_valid))
            and (not {{ enforce_fk }} or (customer_id is null or customer_fk_valid))

            -- Deduplication
            and row_num = 1

        ) as is_valid,

        -- Build detailed invalid_reason for quarantine
        trim(concat_ws(' | ',
            case when not {{ is_valid_id('return_id') }} then 'missing_return_id' end,
            case when not {{ is_valid_id('order_id') }} then 'missing_order_id' end,
            case when not {{ is_valid_id('customer_id') }} then 'missing_customer_id' end,
            case when not {{ is_valid_timestamp('return_date') }} then 'invalid_return_date' end,
            case when refunded_amount < 0 then 'negative_refunded_amount' end,
            case when order_id is not null and not order_fk_valid then 'order_fk_invalid' end,
            case when customer_id is not null and not customer_fk_valid then 'customer_fk_invalid' end,
            case when row_num > 1 then 'duplicate_return_id' end
        )) as invalid_reason

    from validated
)

-- Final: Return only valid records
select
    return_id,
    order_id,
    customer_id,
    email,
    return_date,
    reason,
    return_type,
    refunded_amount,
    return_channel,
    agent_id,
    refund_method,
    batch_id,
    ingestion_ts,
    event_id,
    source_file,
    return_dt
from scored
where is_valid
