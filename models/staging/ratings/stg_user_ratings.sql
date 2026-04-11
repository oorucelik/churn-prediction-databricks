-- staging/ratings/stg_user_ratings.sql
-- User ratings for watched content (1-5 scale).
-- ~30% of watched content gets rated. Key for collaborative filtering.

with source as (
    select * from {{ ref('raw_user_ratings') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['rating_id']) }} as rating_key,
        rating_id,
        customer_id,
        content_id,
        cast(rating as integer)     as rating,
        cast(rated_at as date)      as rated_at
    from source
)

select * from renamed
