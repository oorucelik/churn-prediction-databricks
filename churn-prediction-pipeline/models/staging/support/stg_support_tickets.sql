-- staging/support/stg_support_tickets.sql

with source as (
    select * from {{ ref('raw_support_tickets') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['ticket_id']) }} as interaction_key,
        ticket_id,
        customer_id,
        category,
        channel,
        cast(created_at as timestamp)               as created_at,
        cast(resolution_time_hours as decimal(10,1)) as resolution_time_hours,
        cast(is_resolved as boolean)                 as is_resolved,
        cast(sentiment_score as decimal(5,2))        as sentiment_score
    from source
)

select * from renamed
