-- marts/churn/fct_watch_event.sql
-- Core behavioral fact table — one row per viewing session.
-- Watch frequency and completion rates are the strongest churn signals.

{{ config(materialized='table') }}

with watch_events as (
    select * from {{ ref('stg_watch_events') }}
),

customers as (
    select
        customer_key,
        customer_id
    from {{ ref('dim_customer') }}
),

content as (
    select
        content_key,
        content_id
    from {{ ref('dim_content') }}
),

final as (
    select
        we.watch_event_key,
        c.customer_key,
        ct.content_key,
        we.watch_date as date_day,
        we.watch_event_id,
        we.session_id,
        we.device_type,
        we.watch_start_time,
        we.watch_duration_minutes,
        we.completion_percentage,
        we.is_resumed,

        -- Engagement classification
        case
            when we.completion_percentage >= 90 then 'completed'
            when we.completion_percentage >= 50 then 'engaged'
            when we.completion_percentage >= 10 then 'sampled'
            else 'abandoned'
        end as engagement_level,

        -- Session duration bucket
        case
            when we.watch_duration_minutes <= 15  then 'short'
            when we.watch_duration_minutes <= 60  then 'medium'
            when we.watch_duration_minutes <= 120 then 'long'
            else 'binge'
        end as session_duration_bucket

    from watch_events as we
    inner join customers as c
        on we.customer_id = c.customer_id
    inner join content as ct
        on we.content_id = ct.content_id
)

select * from final
