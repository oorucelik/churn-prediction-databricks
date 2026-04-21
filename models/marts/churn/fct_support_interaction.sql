-- marts/churn/fct_support_interaction.sql
-- Support ticket fact table joined to customer and date dimensions.
-- High ticket volume + negative sentiment = strong churn predictor.

{{ config(materialized='table') }}

with support as (
    select * from {{ ref('stg_support_tickets') }}
),

customers as (
    select
        customer_key,
        customer_id
    from {{ ref('dim_customer') }}
),

final as (
    select
        s.interaction_key,
        c.customer_key,
        cast(s.created_at as date) as date_day,
        s.ticket_id,
        s.category,
        s.channel,
        s.created_at,
        s.resolution_time_hours,
        s.is_resolved,
        s.sentiment_score,

        -- Severity classification based on resolution time
        case
            when s.resolution_time_hours <= 4   then 'quick'
            when s.resolution_time_hours <= 24  then 'standard'
            when s.resolution_time_hours <= 48  then 'slow'
            else 'critical'
        end as resolution_tier,

        -- Sentiment classification
        case
            when s.sentiment_score >= 0.3  then 'positive'
            when s.sentiment_score >= -0.3 then 'neutral'
            else 'negative'
        end as sentiment_tier

    from support as s
    inner join customers as c
        on s.customer_id = c.customer_id
)

select * from final
