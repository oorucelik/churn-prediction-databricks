-- staging/content/stg_content_catalog.sql
-- Content catalog from TMDB API (movies & TV shows).
-- Splits pipe-delimited genres into primary_genre for joins.
{{config(
    materialized='table',
    schema='dev_ocelik',
    alias='stg_content_catalog'
)}}
with content as (
    {{ dbt_utils.deduplicate(
    relation=ref('raw_content_catalog'),
    partition_by='content_id',
    order_by='content_id desc',
   )
}}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['content_id']) }} as content_key,
        content_id,
        cast(tmdb_id as integer)            as tmdb_id,
        title,
        content_type,
        genres                               as genre_list,       -- pipe-delimited, keep for exploding later
        primary_genre,
        cast(release_date as date)           as release_date,
        cast(vote_average as decimal(3,1))   as vote_average,
        cast(vote_count as integer)          as vote_count,
        cast(popularity as decimal(10,2))    as popularity,
        original_language,
        overview,
        cast(runtime_minutes as integer)     as runtime_minutes
    from content
)

select * from renamed
