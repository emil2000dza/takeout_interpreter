{{ config(materialized='table') }}

with source_data as (
    select
        url,
        split_part(url, '/', 1) as url_part1,
        split_part(url, '/', 2) as url_part2,
        split_part(url, '/', 3) as url_part3
    from {{ ref('raw_history') }}
),

paths as (
    select
        url,
        url_part1 || '/' || url_part2                  as url_depth1,
        url_part1 || '/' || url_part2 || '/' || url_part3  as url_depth2,
        case 
            when url_part3 is not null 
            then url_part1 || '/' || url_part2 || '/' || url_part3
            else null
        end as depth3,

        -- Real estate flag
        case 
            when col1 ilike '%seloger%' 
              or col1 ilike '%bienici%' 
            then true 
            else false 
        end as real_estate
    from source_data
)

select * from paths;
