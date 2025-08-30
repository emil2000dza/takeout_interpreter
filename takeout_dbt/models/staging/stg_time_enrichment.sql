{{ config(materialized="table") }}

WITH base AS (
    SELECT
        id,
        datetime,
    FROM {{ source('chrome_history', 'raw_history') }}
),

enriched AS (
    SELECT DISTINCT
        id,
        dayofweek(DATETIME) AS day_of_week,
        extract(HOUR FROM DATETIME) as hour_of_day,
        CASE
            WHEN extract(hour from datetime) BETWEEN 7 AND 11 THEN 'Morning'
            WHEN extract(hour from datetime) BETWEEN 12 AND 14 THEN 'Lunch'
            WHEN extract(hour from datetime) BETWEEN 15 AND 18 THEN 'Afternoon'
            WHEN extract(hour from datetime) BETWEEN 19 AND 23 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day
    FROM base
)

SELECT * FROM enriched
