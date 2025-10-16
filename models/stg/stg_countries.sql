{{ 
    config(
        materialized = 'table',
        tags = ['STG']
    )
}}

select
    COUNTRY_ID,
    COUNTRY_NAME,
    REGION_ID,
    LOAD_TIME
from {{ source('hr','src_countries')}}
