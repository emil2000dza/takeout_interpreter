
{{ config(materialized='table') }}

with source_data as (

    select * from {{ ref('youtube_history') }}

)

select
    "Date" as date,
    "Time" as time,
    "Action" as action,
    "Title" as title,
    "Channel" as channel,
    "URL" as url
from source_data
