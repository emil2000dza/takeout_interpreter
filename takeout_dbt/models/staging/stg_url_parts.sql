WITH source_data AS (
    SELECT
        url,
        parse_url(url):host::string AS domain,
        parse_url(url):path::string AS path
    FROM {{ source('chrome_history', 'raw_history') }}
),

paths AS (
    SELECT DISTINCT
        url,

        domain AS path_level_1,
        split_part(path, '/', 1) AS path_level_2,

        split_part(path, '/', 2) AS path_level_3,

        split_part(path, '/', 3) AS path_level_4,
        split_part(path, '/', 4) AS path_level_5,
        split_part(path, '/', 5) AS path_level_6,

        -- Rebuild progressive depths
        domain AS domain_depth_1,
        domain || '/' || split_part(path, '/', 1) AS domain_depth_2,
        domain || '/' || split_part(path, '/', 1) || '/' || split_part(path, '/', 2) AS domain_depth_3
    FROM source_data
)

SELECT * FROM paths