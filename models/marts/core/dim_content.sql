-- marts/core/dim_content.sql
-- Content dimension built from TMDB catalog data.
-- Enables genre-based analysis and content recommendation joins.

{{ config(materialized='table') }}

with content as (
    select * from {{ ref('stg_content_catalog') }}
),

final as (
    select
        content_key,
        content_id,
        tmdb_id,
        title,
        content_type,
        genre_list,
        primary_genre,
        release_date,
        -- Content age bucket (for recommendation freshness weighting)
        case
            when release_date >= DATEADD(year, -1, GETDATE()) then 'new_release'
            when release_date >= DATEADD(year, -3, GETDATE()) then 'recent'
            when release_date >= DATEADD(year, -10, GETDATE()) then 'catalog'
            else 'classic'
        end as content_age_bucket,
        vote_average,
        -- Rating tier for segmentation
        case
            when vote_average >= 8.0 then 'highly_rated'
            when vote_average >= 6.5 then 'well_rated'
            when vote_average >= 5.0 then 'average'
            else 'below_average'
        end as rating_tier,
        vote_count,
        popularity,
        original_language,
        overview,
        runtime_minutes
    from content
)

select * from final
