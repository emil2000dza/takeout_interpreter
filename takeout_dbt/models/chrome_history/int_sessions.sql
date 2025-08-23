with base as (
    select
        *,
        -- Identify breaks between sessions (30 min = 1800 seconds)
        case when time_since_last_visit > interval '30 minutes' 
             then 1 else 0 end as new_session_flag
    from {{ ref('raw_history') }}
),

sessions as (
    select
        *,
        -- Assign session ids by cumulative sum over time
        sum(new_session_flag) over (
            order by datetime 
            rows between unbounded preceding and current row
        ) as session_id
    from base
)

select * from sessions;
