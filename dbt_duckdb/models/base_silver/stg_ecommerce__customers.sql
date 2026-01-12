{{ config(
    materialized='table',
    post_hook=[
        "COPY (SELECT * FROM {{ this }}) TO '{{ var('silver_base_path') }}/customers' (FORMAT PARQUET, PARTITION_BY (signup_dt), OVERWRITE_OR_IGNORE)"
    ]
) }}

{#
DIMENSION TABLE PATTERN: customers
- Simpler than transactions: no FK validation needed
- Focus on: cleaning, deduplication, email validation
- Partition by signup_date for efficient querying
#}

with raw as (
    select *
    from {{ source_parquet('bronze', 'customers') }}
),

cleaned as (
    select
        -- Primary key
        {{ normalize_string('customer_id') }} as customer_id,

        -- Required fields
        {{ normalize_string_lower('email') }} as email,
        {{ safe_cast_date('signup_date') }} as signup_date,

        -- Optional customer attributes
        {{ normalize_string('first_name') }} as first_name,
        {{ normalize_string('last_name') }} as last_name,
        {{ normalize_string('phone_number') }} as phone_number,
        {{ normalize_string_lower('gender') }} as gender,
        {{ safe_cast_decimal('age', 10, 2) }} as age,
        {{ safe_cast_boolean('is_guest') }} as is_guest,
        {{ normalize_string_lower('customer_status') }} as customer_status,
        {{ normalize_string_lower('signup_channel') }} as signup_channel,
        {{ normalize_string_lower('loyalty_tier') }} as loyalty_tier,
        {{ normalize_string_lower('initial_loyalty_tier') }} as initial_loyalty_tier,
        {{ safe_cast_boolean('email_verified') }} as email_verified,
        {{ safe_cast_boolean('marketing_opt_in') }} as marketing_opt_in,
        {{ normalize_string('mailing_address') }} as mailing_address,
        {{ normalize_string('billing_address') }} as billing_address,
        {{ safe_cast_date('loyalty_enrollment_date') }} as loyalty_enrollment_date,
        {{ normalize_string_lower('clv_bucket') }} as clv_bucket,

        -- Lineage columns
        {{ normalize_string('batch_id') }} as batch_id,
        {{ safe_cast_timestamp('ingestion_ts') }} as ingestion_ts,
        {{ normalize_string('event_id') }} as event_id,
        {{ normalize_string('source_file') }} as source_file,

        -- Derived: partition column
        cast({{ safe_cast_date('signup_date') }} as date) as signup_dt

    from raw
),

validated as (
    select
        cleaned.*,

        -- Deduplication: keep most recent record per customer_id
        row_number() over (
            partition by cleaned.customer_id
            order by cleaned.ingestion_ts desc nulls last, cleaned.event_id desc
        ) as row_num

    from cleaned
),

scored as (
    select
        *,

        -- Validation rules
        (
            {{ is_valid_id('customer_id') }}
            and {{ is_valid_email('email') }}
            and {{ is_valid_timestamp('signup_date') }}
            and row_num = 1
        ) as is_valid,

        -- Invalid reasons
        trim(concat_ws(' | ',
            case when not {{ is_valid_id('customer_id') }} then 'missing_customer_id' end,
            case when not {{ is_valid_email('email') }} then 'invalid_email' end,
            case when signup_date is null then 'missing_signup_date' end,
            case when row_num > 1 then 'duplicate_customer_id' end
        )) as invalid_reason

    from validated
)

-- Return only valid customers
select
    customer_id,
    email,
    signup_date,
    first_name,
    last_name,
    phone_number,
    gender,
    age,
    is_guest,
    customer_status,
    signup_channel,
    loyalty_tier,
    initial_loyalty_tier,
    email_verified,
    marketing_opt_in,
    mailing_address,
    billing_address,
    loyalty_enrollment_date,
    clv_bucket,
    batch_id,
    ingestion_ts,
    event_id,
    source_file,
    signup_dt
from scored
where is_valid
