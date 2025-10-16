{{
    config(
        materialized='table',
        tags = ['stg']
    )
}}
select
REGION_ID,
REGION_NAME,
LOAD_TIME
from {{ source('hr','src_regions')}}