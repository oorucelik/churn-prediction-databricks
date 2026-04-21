-- marts/churn/fct_daily_user_activity.sql
-- Periodic snapshot fact: one row per customer per active day.
-- Aggregates watch events, ratings, and watchlist activity into daily metrics.
-- This feeds directly into ml_churn_feature_store.

{{ config(materialized='table') }}

with watch_daily as (
    select
        customer_id,
        watch_date,
        count(*) as sessions_count,
        count(distinct content_id) as titles_watched,
        sum(watch_duration_minutes) as total_watch_minutes,
        avg(completion_percentage) as avg_completion_pct,
        sum(case when is_resumed then 1 else 0 end) as resumed_sessions,
        sum(case when completion_percentage >= 90 then 1 else 0 end) as completed_titles
    from {{ ref('stg_watch_events') }}
    group by customer_id, watch_date
),

ratings_daily as (
    select
        customer_id,
        rated_at as activity_date,
        count(*) as ratings_given,
        avg(rating) as avg_rating
    from {{ ref('stg_user_ratings') }}
    group by customer_id, rated_at
),

watchlist_daily as (
    select
        customer_id,
        added_at as activity_date,
        count(*) as watchlist_adds,
        sum(case when is_watched then 1 else 0 end) as watchlist_conversions
    from {{ ref('stg_user_watchlist') }}
    group by customer_id, added_at
),

customers as (
    select
        customer_key,
        customer_id
    from {{ ref('dim_customer') }}
),

-- Combine all activity sources per customer per day
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['c.customer_id', 'w.watch_date']) }} as activity_key,
        c.customer_key,
        w.watch_date as date_day,

        -- Watch metrics
        w.sessions_count,
        w.titles_watched,
        w.total_watch_minutes,
        w.avg_completion_pct,
        w.resumed_sessions,
        w.completed_titles,

        -- Rating metrics
        coalesce(r.ratings_given, 0) as ratings_given,
        r.avg_rating,

        -- Watchlist metrics
        coalesce(wl.watchlist_adds, 0) as watchlist_adds,
        coalesce(wl.watchlist_conversions, 0) as watchlist_conversions,

        -- Engagement score (composite signal)
        (
            w.sessions_count * 1.0
            + w.completed_titles * 2.0
            + coalesce(r.ratings_given, 0) * 1.5
            + coalesce(wl.watchlist_adds, 0) * 0.5
        ) as daily_engagement_score,

        -- Binge indicator (3+ titles in one day)
        case
            when w.titles_watched >= 3 then true
            else false
        end as is_binge_day

    from watch_daily as w
    inner join customers as c
        on w.customer_id = c.customer_id
    left join ratings_daily as r
        on w.customer_id = r.customer_id
        and w.watch_date = r.activity_date
    left join watchlist_daily as wl
        on w.customer_id = wl.customer_id
        and w.watch_date = wl.activity_date
)

select * from final
