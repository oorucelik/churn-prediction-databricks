-- marts/finance/fct_subscription_event.sql
-- Subscription lifecycle events: subscribe, upgrade, downgrade, cancel.
-- Grain: one row per subscription event.
-- Enables MRR analysis, churn rate calculation, and cohort tracking.

{{ config(materialized='table') }}

with subscription_events as (
    select * from {{ ref('stg_subscription_events') }}
),

customers as (
    select
        customer_key,
        customer_id
    from {{ ref('dim_customer') }}
),

dates as (
    select date_day from {{ ref('dim_date') }}
),

final as (
    select
        se.subscription_event_key,
        c.customer_key,
        se.event_date as date_day,
        se.event_type,
        se.plan_type,
        se.monthly_revenue,
        se.cancellation_reason,

        -- Revenue change direction
        case
            when se.event_type = 'subscribe' then 'new'
            when se.event_type = 'upgrade' then 'expansion'
            when se.event_type = 'downgrade' then 'contraction'
            when se.event_type = 'cancel' then 'churned'
            else 'other'
        end as revenue_category,

        -- Previous plan for plan change tracking (per customer timeline)
        lag(se.plan_type) over (
            partition by se.customer_id
            order by se.event_date
        ) as previous_plan_type,

        lag(se.monthly_revenue) over (
            partition by se.customer_id
            order by se.event_date
        ) as previous_monthly_revenue,

        -- Days since previous event (tenure signal)
        datediff(
            day,
            lag(se.event_date) over (
                partition by se.customer_id
                order by se.event_date
            ),
            se.event_date
        ) as days_since_previous_event

    from subscription_events as se
    inner join customers as c
        on se.customer_id = c.customer_id
    inner join dates as d
        on se.event_date = d.date_day
)

select * from final
