-- staging/customers/stg_customers.sql
-- One staging model per source table. Clean, cast, rename, dedup.
-- NO joins. NO business logic. Just trustworthy data.

with source as (
    select * from {{ ref('raw_users') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
        customer_id,
        email,
        country,
        cast(age as integer)                   as age,
        cast(signup_date as date)              as signup_date,
        plan_type,
        cast(monthly_revenue as decimal(10,2)) as monthly_revenue,
        cast(is_churned as bit)                as is_churned,
        cast(churn_date as date)               as churn_date,
        acquisition_channel,
        device_type,
        preferred_genres,
        activity_level
    from source
)

select * from renamed
