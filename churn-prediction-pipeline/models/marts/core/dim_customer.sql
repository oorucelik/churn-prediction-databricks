-- marts/core/dim_customer.sql
-- Customer dimension with age banding, genre preferences, and derived attributes.
-- TODO: Convert to SCD Type 2 via dbt snapshot when plan_type changes matter.

{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        customer_key,
        customer_id,
        email,
        country,
        age,
        case
            when age < 25 then '18-24'
            when age < 35 then '25-34'
            when age < 45 then '35-44'
            when age < 55 then '45-54'
            else '55+'
        end as age_band,
        signup_date,
        plan_type,
        monthly_revenue,
        is_churned,
        churn_date,
        acquisition_channel,
        device_type,
        preferred_genres,
        activity_level
    from customers
)

select * from final
