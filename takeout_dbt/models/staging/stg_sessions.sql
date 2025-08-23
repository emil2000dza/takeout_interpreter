WITH base AS (
    SELECT
        id,
        -- Identify breaks between sessions (30 min = 1800 seconds)
        CASE WHEN time_since_last_visit > interval '30 minutes' 
             THEN 1 ELSE 0 END AS new_session_flag
    FROM {{ source('chrome_schema', 'raw_history') }}
),

sessions AS (
    SELECT
        id,
        -- Assign session ids by cumulative sum over time
        SUM(new_session_flag) OVER (
            ORDER BY datetime 
            rows BETWEEN unbounded preceding and current row
        ) as session_id
    FROM base
)

SELECT * FROM sessions
