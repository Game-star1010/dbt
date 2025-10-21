{{ 
    config(
        materialized = 'table',
        tags = ['DIM']
    )
}}

select
    COUNTRY_ID,
    COUNTRY_NAME,
    REGION_ID,
    LOAD_TIME
from {{ source('hr','src_departments')}}
