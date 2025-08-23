{{ config(materialized="table") }}

WITH base AS (
    SELECT
        id,
        datetime,
        title,
        url,
        domain,
        time_since_last_visit
    FROM {{ ref('raw_history') }}
),

enriched AS (
    SELECT
        id,
        dayofweek(DATETIME) AS day_of_week,
        extract(HOUR FROM DATETIME) as hour_of_day,
        CASE
            WHEN extract(hour from datetime) BETWEEN 7 AND 11 THEN 'Morning'
            WHEN extract(hour from datetime) BETWEEN 12 AND 14 THEN 'Lunch'
            WHEN extract(hour from datetime) BETWEEN 15 AND 18 THEN 'Afternoon'
            WHEN extract(hour from datetime) BETWEEN 19 AND 23 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day,
        SUM(EXTRACT(EPOCH FROM time_since_last_visit)) AS seconds_since_last_visit
    FROM base
)

SELECT * FROM enriched;
