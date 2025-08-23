{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        url,
        split_part(url, '/', 1) AS url_part1,
        split_part(url, '/', 2) AS url_part2,
        split_part(url, '/', 3) AS url_part3
    FROM {{ source('chrome_schema', 'raw_history') }}
),

paths AS (
    SELECT
        url,
        CASE
            WHEN url_part2 IS NOT NULL
            THEN url_part1 || '/' || url_part2 
            ELSE NULL 
        END AS domain_depth_1,
        CASE 
            WHEN url_part3 IS NOT NULL AND url_part2 IS NOT NULL
            THEN url_part1 || '/' || url_part2 || '/' || url_part3
            ELSE NULL
        END AS domain_depth_2,
        
    FROM source_data
)

SELECT * FROM paths;
