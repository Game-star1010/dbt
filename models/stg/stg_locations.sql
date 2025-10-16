{{
    config(
        materialized='table',
        tags = ['stg']
    )
}}
select
LOCATION_ID,
STREET_ADDRESS,
POSTAL_CODE,
CITY,
STATE_PROVINCE,
COUNTRY_ID,
LOAD_TIME
from {{source('hr','src_locations')}}