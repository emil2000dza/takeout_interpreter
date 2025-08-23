
{{ config(materialized='table') }}

with source_data as (

    select * from {{ ref('google_analytics_data') }}

)

select
    "Header" as header,
    "URL" as url,
    "Datetime" as datetime,
    "Product" as product
from source_data
