-- staging/watchlist/stg_user_watchlist.sql
-- Content saved to user watchlists.
-- Watchlist-to-watch conversion is a churn predictor.

with source as (
    select * from {{ ref('raw_user_watchlist') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['watchlist_id']) }} as watchlist_key,
        watchlist_id,
        customer_id,
        content_id,
        cast(added_at as date)        as added_at,
        cast(is_watched as bit)  as is_watched
    from source
)

select * from renamed
