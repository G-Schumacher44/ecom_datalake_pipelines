{{ config(
    materialized='table',
    post_hook=[
        "COPY (SELECT * FROM {{ this }}) TO '{{ var('silver_base_path') }}/orders' (FORMAT PARQUET, PARTITION_BY (order_dt), OVERWRITE_OR_IGNORE)"
    ]
) }}

{% set enforce_customer_fk = strict_fk() %}

{#
TRANSACTION TABLE PATTERN: orders
- Multi-stage cleaning with CTEs
- FK validation against dimension tables
- Deduplication using window functions
- Validation scoring with detailed reasons
- Quarantine pattern for invalid rows
#}

with raw as (
    select *
    from {{ source_parquet('bronze', 'orders') }}
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
        {{ normalize_string('order_id') }} as order_id,

        -- Foreign keys
        {{ normalize_string('customer_id') }} as customer_id,

        -- Timestamps
        {{ safe_cast_timestamp('order_date') }} as order_date,
        {{ safe_cast_timestamp('ingestion_ts') }} as ingestion_ts,

        -- Numeric fields
        {{ safe_cast_integer('total_items') }} as total_items,
        {{ safe_cast_decimal('gross_total', 18, 2) }} as gross_total,
        {{ safe_cast_decimal('net_total', 18, 2) }} as net_total,
        {{ safe_cast_decimal('total_discount_amount', 18, 2) }} as total_discount_amount,
        {{ safe_cast_decimal('shipping_cost', 18, 2) }} as shipping_cost,
        {{ safe_cast_decimal('actual_shipping_cost', 18, 2) }} as actual_shipping_cost,
        {{ safe_cast_decimal('payment_processing_fee', 18, 2) }} as payment_processing_fee,

        -- Boolean fields
        {{ safe_cast_boolean('is_expedited') }} as is_expedited,
        {{ safe_cast_boolean('is_reactivated') }} as is_reactivated,

        -- String fields (lowercase normalized)
        {{ normalize_string_lower('email') }} as email,
        {{ normalize_string_lower('order_channel') }} as order_channel,
        {{ normalize_string_lower('customer_tier') }} as customer_tier,
        {{ normalize_string_lower('payment_method') }} as payment_method,
        {{ normalize_string_lower('shipping_speed') }} as shipping_speed,
        {{ normalize_string_lower('clv_bucket') }} as clv_bucket,

        -- String fields (case-preserved)
        {{ normalize_string('agent_id') }} as agent_id,
        {{ normalize_string('shipping_address') }} as shipping_address,
        {{ normalize_string('billing_address') }} as billing_address,

        -- Lineage columns (required in all Base Silver)
        {{ normalize_string('batch_id') }} as batch_id,
        {{ normalize_string('event_id') }} as event_id,
        {{ normalize_string('source_file') }} as source_file,

        -- Derived: partition column
        cast({{ safe_cast_timestamp('order_date') }} as date) as order_dt

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

        -- Deduplication: keep most recent record per order_id
        row_number() over (
            partition by cleaned.order_id
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
            {{ is_valid_id('order_id') }}
            and {{ is_valid_id('customer_id') }}
            and {{ is_valid_timestamp('order_date') }}
            and {{ is_non_negative_number('gross_total') }}
            and {{ is_non_negative_number('net_total') }}

            -- Business rules
            and (total_discount_amount is null or total_discount_amount >= 0)
            and (net_total <= gross_total or gross_total is null or net_total is null)

            {% if enforce_customer_fk %}
            -- FK integrity
            and (customer_id is null or customer_fk_valid)
            {% endif %}

            -- Deduplication
            and row_num = 1

        ) as is_valid,

        -- Build detailed invalid_reason for quarantine
        trim(concat_ws(' | ',
            case when not {{ is_valid_id('order_id') }} then 'missing_order_id' end,
            case when not {{ is_valid_id('customer_id') }} then 'missing_customer_id' end,
            case when not {{ is_valid_timestamp('order_date') }} then 'invalid_order_date' end,
            case when gross_total is null then 'missing_gross_total' end,
            case when net_total is null then 'missing_net_total' end,
            case when gross_total < 0 then 'negative_gross_total' end,
            case when net_total < 0 then 'negative_net_total' end,
            case when total_discount_amount < 0 then 'negative_discount' end,
            case when net_total > gross_total and gross_total is not null and net_total is not null
                 then 'net_exceeds_gross' end,
            {% if enforce_customer_fk %}
            case when customer_id is not null and not customer_fk_valid then 'customer_fk_invalid' end,
            {% endif %}
            case when row_num > 1 then 'duplicate_order_id' end
        )) as invalid_reason

    from validated
)

-- Final: Return only valid records
select
    order_id,
    total_items,
    order_date,
    customer_id,
    email,
    order_channel,
    is_expedited,
    customer_tier,
    gross_total,
    net_total,
    total_discount_amount,
    payment_method,
    shipping_speed,
    shipping_cost,
    agent_id,
    actual_shipping_cost,
    payment_processing_fee,
    shipping_address,
    billing_address,
    clv_bucket,
    is_reactivated,
    batch_id,
    ingestion_ts,
    event_id,
    source_file,
    order_dt
from scored
where is_valid
