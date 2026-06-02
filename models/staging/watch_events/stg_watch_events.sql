-- staging/watch_events/stg_watch_events.sql
-- User viewing history — the core behavioral signal for churn prediction.

with source as (
    select * from {{ ref('raw_watch_events') }}
),

renamed as (
select
{{dbt_utils.generate_surrogate_key(['watch_event_id'])}} as watch_event_key,
watch_event_id,
customer_id,
content_id,
cast(watch_date as date) as watch_date,
cast(watch_start_time as varchar(10)) as watch_start_time,
cast(watch_duration_minutes as integer) as watch_duration_minutes,
cast(completion_percentage as decimal(5, 2)) as completion_percentage,
device_type,
session_id,
cast(is_resumed as boolean) as is_resumed
from source
)

select * from renamed