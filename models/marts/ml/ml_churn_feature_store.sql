-- marts/ml/ml_churn_feature_store.sql
-- ML-ready feature table: one row per customer with 50+ engineered features.
--Consumed by the XGBoost prediction model — both batch and real-time scoring.
-- All features are numeric or encoded — no raw strings.

{{ config(materialized='table') }}

-- ═══════════════════════════════════════════════════════════════
-- 1. CUSTOMER DEMOGRAPHICS (from dim_customer)
-- ═══════════════════════════════════════════════════════════════
with customer_features as (
    select
        customer_key,
        customer_id,
        is_churned,
        churn_date,

        -- Tenure
        datediff(day, signup_date, current_date()) as tenure_days,

        -- Demographics
        age,
        case age_band
            when '18-24' then 1
            when '25-34' then 2
            when '35-44' then 3
            when '45-54' then 4
            when '55+' then 5
        end as age_band_encoded,

        -- Plan encoding (ordinal — higher = more expensive)
        case plan_type
            when 'basic' then 1
            when 'standard' then 2
            when 'premium' then 3
        end as plan_type_encoded,
        monthly_revenue,

        -- Acquisition channel encoding
        case acquisition_channel
            when 'organic' then 1
            when 'referral' then 2
            when 'paid_search' then 3
            when 'social' then 4
            when 'tv_ad' then 5
        end as acquisition_channel_encoded,

        -- Device encoding
        case device_type
            when 'smart_tv' then 1
            when 'desktop' then 2
            when 'mobile' then 3
            when 'tablet' then 4
            when 'gaming_console' then 5
        end as device_type_encoded,

        -- Activity level encoding
        case activity_level
            when 'low' then 1
            when 'medium' then 2
            when 'high' then 3
        end as activity_level_encoded

    from {{ ref('dim_customer') }}
),

-- ═══════════════════════════════════════════════════════════════
-- 2. WATCH BEHAVIOR FEATURES (from fct_watch_event)
-- ═══════════════════════════════════════════════════════════════
watch_features as (
    select
        customer_key,

        -- Volume metrics
        count(*) as total_watch_events,
        count(distinct date_day) as total_active_days,
        count(distinct content_key) as unique_titles_watched,
        count(distinct session_id) as unique_sessions,

        -- Duration metrics
        sum(watch_duration_minutes) as total_watch_minutes,
        round(avg(watch_duration_minutes), 2) as avg_watch_duration_min,
        max(watch_duration_minutes) as max_watch_duration_min,

        -- Completion metrics
        round(avg(completion_percentage), 2) as avg_completion_pct,
        sum(case when engagement_level = 'completed' then 1 else 0 end) as completed_count,
        sum(case when engagement_level = 'abandoned' then 1 else 0 end) as abandoned_count,
        round(sum(case when engagement_level = 'completed' then 1 else 0 end) 
        * 1.0 / nullif(count(*), 0), 2) as completion_rate,
        round(sum(case when engagement_level = 'abandoned' then
        1 else 0 end) * 1.0 / nullif(count(*), 0), 2) as abandonment_rate,

        -- Resume behavior (re-engagement signal)
        sum(case when is_resumed then 1 else 0 end) as resumed_count,
        round(sum(case when is_resumed then 1 else 0 end) * 1.0 / nullif(count(*), 0), 2) as resume_rate,

        -- Session type distribution
        sum(case when session_duration_bucket = 'binge' then 1 else 0 end) as binge_session_count,
        sum(case when session_duration_bucket = 'short' then 1 else 0 end) as short_session_count,

        -- Device diversity (multi-device users are stickier)
        count(distinct device_type) as device_count,

        -- Recency
        datediff(day, max(date_day), current_date()) as days_since_last_watch,
        datediff(day, min(date_day), max(date_day)) as watch_span_days

    from {{ ref('fct_watch_event') }}
    group by customer_key
),

-- ═══════════════════════════════════════════════════════════════
-- 3. RECENT WATCH BEHAVIOR (last 7 & 30 days — decay signals)
-- ═══════════════════════════════════════════════════════════════
watch_recent as (
    select
        customer_key,

        -- Last 7 days
        sum(case when date_day >= dateadd(day, -7, current_date()) then 1 else 0 end)
            as watch_events_last_7d,
        sum(case when date_day >= dateadd(day, -7, current_date()) then watch_duration_minutes else 0 end)
            as watch_minutes_last_7d,

        -- Last 30 days
        sum(case when date_day >= dateadd(day, -30, current_date()) then 1 else 0 end)
            as watch_events_last_30d,
        sum(case when date_day >= dateadd(day, -30, current_date()) then watch_duration_minutes else 0 end)
            as watch_minutes_last_30d,

        -- Trend: 7d vs 30d ratio (declining = churn risk)
        round(case 
            when sum(case when date_day >= dateadd(day, -30, current_date()) then 1 else 0 end) > 0 
            then sum(case when date_day >= dateadd(day, -7, current_date()) then 1 else 0 end) * 1.0 
            /(sum(case when date_day >= dateadd(day, -30, current_date()) then 1 else 0 end) / 4.28)
            else null
        end, 2) as watch_trend_7d_vs_30d

    from {{ ref('fct_watch_event') }}
    group by customer_key
),

-- 4. SUBSCRIPTION HISTORY FEATURES (from fct_subscription_event)
subscription_features as (
select
    customer_key,

    count(*) as total_subscription_events,
    sum(case when event_type = 'upgrade' then 1 else 0 end) as upgrade_count,
    sum(case when event_type = 'downgrade' then 1 else 0 end) as downgrade_count,
    sum(case when event_type = 'cancel' then 1 else 0 end) as cancel_count,

    -- Net plan direction (positive = upgrading, negative = downgrading)
    sum(case when event_type = 'upgrade' then 1 else 0 end) 
    - sum(case when event_type = 'downgrade' then 1 else 0 end) as net_plan_changes,

    -- Had a downgrade (binary — strong churn signal)
    max(case when event_type = 'downgrade' then 1 else 0 end) as has_downgraded

    -- Note: current_revenue omitted — monthly_revenue already in dim_customer

from {{ ref('fct_subscription_event') }}
group by customer_key
),

-- 5. SUPPORT INTERACTION FEATURES (from fct_support_interaction)
support_features as (
    select
        customer_key,

        count(*) as total_support_tickets,
        round(avg(resolution_time_hours), 2) as avg_resolution_hours,
        max(resolution_time_hours) as max_resolution_hours,
        round(avg(sentiment_score), 2) as avg_sentiment_score,
        min(sentiment_score) as min_sentiment_score,

        -- Negative interactions (churn amplifier)
        sum(case when sentiment_tier = 'negative' then 1 else 0 end) as negative_ticket_count,
        round(sum(case when sentiment_tier = 'negative' then 1 else 0 end) * 1.0
            / nullif(count(*), 0), 2) as negative_ticket_rate,

        -- Unresolved tickets
        sum(case when is_resolved = false then 1 else 0 end) as unresolved_ticket_count,

        -- Category distribution
        sum(case when category = 'billing' then 1 else 0 end) as billing_tickets,
        sum(case when category = 'technical' then 1 else 0 end) as technical_tickets,
        sum(case when category = 'content' then 1 else 0 end) as content_tickets,

        -- Resolution tier distribution
        sum(case when resolution_tier = 'critical' then 1 else 0 end) as critical_tickets,

        -- Recency
        datediff(day, max(date_day), current_date()) as days_since_last_ticket

    from {{ ref('fct_support_interaction') }}
    group by customer_key
),

-- 6. RATING BEHAVIOR FEATURES (from stg_user_ratings)
rating_features as (
    select
        c.customer_key,

        count(*) as total_ratings,
        round(avg(r.rating), 2) as avg_rating_given,
        min(r.rating) as min_rating_given,
        sum(case when r.rating <= 2 then 1 else 0 end) as low_ratings_count,
        sum(case when r.rating >= 4 then 1 else 0 end) as high_ratings_count

    from {{ ref('stg_user_ratings') }} as r
    inner join {{ ref('dim_customer') }} as c
        on r.customer_id = c.customer_id
    group by c.customer_key
),

-- ═══════════════════════════════════════════════════════════════
-- 7. WATCHLIST BEHAVIOR FEATURES (from stg_user_watchlist)
-- ═══════════════════════════════════════════════════════════════
watchlist_features as (
    select
        c.customer_key,

        count(*) as total_watchlist_items,
        sum(case when wl.is_watched then 1 else 0 end) as watchlist_watched_count,
        round(sum(case when wl.is_watched then 1 else 0 end) * 1.0
            / nullif(count(*), 0), 2) as watchlist_conversion_rate

    from {{ ref('stg_user_watchlist') }} as wl
    inner join {{ ref('dim_customer') }} as c
        on wl.customer_id = c.customer_id
    group by c.customer_key
),

-- ═══════════════════════════════════════════════════════════════
-- 8. ASSEMBLE FEATURE VECTOR
-- ═══════════════════════════════════════════════════════════════
final as (
    select
        -- Keys and target
        cf.customer_key,
        cf.customer_id,
        cf.is_churned as target_is_churned,
        cf.churn_date as target_churn_date,

        -- ── DEMOGRAPHIC FEATURES (7) ────────────────────────
        cf.tenure_days,
        cf.age,
        cf.age_band_encoded,
        cf.plan_type_encoded,
        cf.monthly_revenue,
        cf.acquisition_channel_encoded,
        cf.device_type_encoded,

        -- ── WATCH BEHAVIOR FEATURES (17) ────────────────────
        coalesce(wf.total_watch_events, 0) as total_watch_events,
        coalesce(wf.total_active_days, 0) as total_active_days,
        coalesce(wf.unique_titles_watched, 0) as unique_titles_watched,
        coalesce(wf.unique_sessions, 0) as unique_sessions,
        coalesce(wf.total_watch_minutes, 0) as total_watch_minutes,
        wf.avg_watch_duration_min,
        wf.max_watch_duration_min,
        wf.avg_completion_pct,
        coalesce(wf.completed_count, 0) as completed_count,
        coalesce(wf.abandoned_count, 0) as abandoned_count,
        wf.completion_rate,
        wf.abandonment_rate,
        coalesce(wf.resumed_count, 0) as resumed_count,
        wf.resume_rate,
        coalesce(wf.binge_session_count, 0) as binge_session_count,
        coalesce(wf.device_count, 0) as device_count,
        coalesce(wf.days_since_last_watch, 9999) as days_since_last_watch,

        -- ── RECENT ACTIVITY FEATURES (5) ────────────────────
        coalesce(wr.watch_events_last_7d, 0) as watch_events_last_7d,
        coalesce(wr.watch_minutes_last_7d, 0) as watch_minutes_last_7d,
        coalesce(wr.watch_events_last_30d, 0) as watch_events_last_30d,
        coalesce(wr.watch_minutes_last_30d, 0) as watch_minutes_last_30d,
        wr.watch_trend_7d_vs_30d,

        -- ── SUBSCRIPTION FEATURES (6) ──────────────────────
        coalesce(sf.total_subscription_events, 0) as total_subscription_events,
        coalesce(sf.upgrade_count, 0) as upgrade_count,
        coalesce(sf.downgrade_count, 0) as downgrade_count,
        coalesce(sf.net_plan_changes, 0) as net_plan_changes,
        coalesce(sf.has_downgraded, 0) as has_downgraded,
        cf.activity_level_encoded,

        -- ── SUPPORT FEATURES (10) ──────────────────────────
        coalesce(sup.total_support_tickets, 0) as total_support_tickets,
        sup.avg_resolution_hours,
        sup.avg_sentiment_score,
        sup.min_sentiment_score,
        coalesce(sup.negative_ticket_count, 0) as negative_ticket_count,
        sup.negative_ticket_rate,
        coalesce(sup.unresolved_ticket_count, 0) as unresolved_ticket_count,
        coalesce(sup.billing_tickets, 0) as billing_tickets,
        coalesce(sup.technical_tickets, 0) as technical_tickets,
        coalesce(sup.critical_tickets, 0) as critical_tickets,

        -- ── RATING FEATURES (5) ────────────────────────────
        coalesce(rf.total_ratings, 0) as total_ratings,
        rf.avg_rating_given,
        coalesce(rf.low_ratings_count, 0) as low_ratings_count,
        coalesce(rf.high_ratings_count, 0) as high_ratings_count,

        -- ── WATCHLIST FEATURES (3) ──────────────────────────
        coalesce(wlf.total_watchlist_items, 0) as total_watchlist_items,
        coalesce(wlf.watchlist_watched_count, 0) as watchlist_watched_count,
        wlf.watchlist_conversion_rate,

        -- ── COMPOSITE RISK SCORES (3) ──────────────────────
        -- Engagement decay risk (higher = more at risk)
        case
            when coalesce(wf.days_since_last_watch, 9999) >= 14 then 5
            when coalesce(wf.days_since_last_watch, 9999) >= 7 then 4
            when coalesce(wf.days_since_last_watch, 9999) >= 3 then 3
            when coalesce(wf.days_since_last_watch, 9999) >= 1 then 2
            else 1
        end as recency_risk_score,

        -- Support frustration score
        (
            coalesce(sup.negative_ticket_count, 0) * 3.0
            + coalesce(sup.unresolved_ticket_count, 0) * 2.0
            + coalesce(sup.critical_tickets, 0) * 2.0
        ) as support_frustration_score,

        -- Overall churn risk heuristic (pre-ML baseline)
        (
            case when coalesce(wf.days_since_last_watch, 9999) >= 7 then 3 else 0 end
            + coalesce(sf.has_downgraded, 0) * 2
            + case when coalesce(sup.negative_ticket_count, 0) >= 2 then 2 else 0 end
            + case when wf.abandonment_rate >= 0.5 then 2 else 0 end
            + case when coalesce(wr.watch_events_last_7d, 0) = 0 then 3 else 0 end
        ) as heuristic_churn_risk_score

    from customer_features as cf
    left join watch_features as wf
        on cf.customer_key = wf.customer_key
    left join watch_recent as wr
        on cf.customer_key = wr.customer_key
    left join subscription_features as sf
        on cf.customer_key = sf.customer_key
    left join support_features as sup
        on cf.customer_key = sup.customer_key
    left join rating_features as rf
        on cf.customer_key = rf.customer_key
    left join watchlist_features as wlf
        on cf.customer_key = wlf.customer_key
)

select * from final
