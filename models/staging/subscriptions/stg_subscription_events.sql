-- staging/subscriptions/stg_subscription_events.sql

with source as (
    select * from {{ ref('raw_subscription_events') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'event_type', 'event_date']) }} as subscription_event_key,
        customer_id,
        event_type,
        cast(event_date as date)                as event_date,
        plan_type,
        cast(monthly_revenue as decimal(10,2))  as monthly_revenue,
        cancellation_reason
    from source
)

select * from renamed
