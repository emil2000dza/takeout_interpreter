WITH base AS (
    SELECT
        id,
        datetime,
        LAG(datetime) OVER (ORDER BY datetime) AS prev_datetime,
        LEAD(datetime) OVER (ORDER BY datetime) AS next_datetime,

        DATEDIFF(minute, LAG(datetime) OVER (ORDER BY datetime), datetime) AS minutes_since_last_visit,
        DATEDIFF(second, LAG(datetime) OVER (ORDER BY datetime), datetime) AS seconds_since_last_visit,

        -- new feature: seconds until the next visit
        DATEDIFF(second, datetime, LEAD(datetime) OVER (ORDER BY datetime)) AS seconds_until_next_visit,

        CASE 
            WHEN DATEDIFF(minute, LAG(datetime) OVER (ORDER BY datetime), datetime) >= 20
                OR LAG(datetime) OVER (ORDER BY datetime) IS NULL
            THEN 1 ELSE 0 
        END AS flag_20_min_new_session,

        CASE 
            WHEN DATEDIFF(minute, LAG(datetime) OVER (ORDER BY datetime), datetime) >= 5
                OR LAG(datetime) OVER (ORDER BY datetime) IS NULL
            THEN 1 ELSE 0 
        END AS flag_5_min_new_session
    FROM {{ source('chrome_history','raw_history') }}
),

sessions AS (
    SELECT DISTINCT
        id,
        seconds_since_last_visit,
        minutes_since_last_visit,
        seconds_until_next_visit,

        CASE 
            WHEN seconds_since_last_visit <= 15 THEN '<15 seconds'
            WHEN seconds_since_last_visit <= 60 THEN '15-60 seconds'
            WHEN seconds_since_last_visit <= 180 THEN '1 min to 3 min'
            WHEN seconds_since_last_visit < 12000 THEN '3 min to 20 min'
            ELSE '>20 min'
        END AS seconds_since_last_visit_categories,

        CASE 
            WHEN seconds_since_last_visit <= 60 THEN 'Quick Switching'
            WHEN seconds_since_last_visit <= 12000 THEN 'Sustained Engagement'
            ELSE 'Session Break'
        END AS attention_span,

        SUM(flag_20_min_new_session) OVER (
            ORDER BY datetime 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_id_20_min,

        SUM(flag_5_min_new_session) OVER (
            ORDER BY datetime 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_id_5_min
    FROM base
)

SELECT * FROM sessions
