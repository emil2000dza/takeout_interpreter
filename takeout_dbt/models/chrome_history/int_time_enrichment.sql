{{ config(materialized="table") }}

with base as (
    select
        datetime,
        title,
        url,
        domain,
        time_since_last_visit
    from {{ ref('raw_chrome_history') }}
),

enriched as (
    select
        *,
        -- Day of week (1=Monday, 7=Sunday in Snowflake)
        dayofweek(datetime) as day_of_week,
        -- Hour of day
        extract(hour from datetime) as hour_of_day,
        -- Time of day buckets
        case
            when extract(hour from datetime) between 5 and 11 then 'Morning'
            when extract(hour from datetime) between 12 and 17 then 'Afternoon'
            when extract(hour from datetime) between 18 and 22 then 'Evening'
            else 'Night'
        end as time_of_day
    from base
)

select * from enriched;
