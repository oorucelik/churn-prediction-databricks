-- marts/core/dim_date.sql

{{ config(materialized='table') }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast('2027-12-31' as date)"
    ) }}
),

final as (
    select
        date_day,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day_of_month,
        extract(dow from date_day) as day_of_week_num,
        case extract(dow from date_day)
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
            when 7 then 'Sunday'
        end as day_of_week_name,
        case
            when extract(dow from date_day) in (6, 7) then true
            else false
        end as is_weekend,
        case extract(month from date_day)
            when 1 then 'January'
            when 2 then 'February'
            when 3 then 'March'
            when 4 then 'April'
            when 5 then 'May'
            when 6 then 'June'
            when 7 then 'July'
            when 8 then 'August'
            when 9 then 'September'
            when 10 then 'October'
            when 11 then 'November'
            when 12 then 'December'
        end as month_name,
        ceil(extract(month from date_day) / 3.0) as quarter,
        extract(year from date_day) as fiscal_year
    from date_spine
)

select * from final