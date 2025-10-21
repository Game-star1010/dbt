{{
    config(
        materialized='incremental',
        unique_key = 'location_id',
        incremental_statergy = 'delete+insert'
        database = 'dev',
        schema = 'dim'
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
from {{ ref 'stg_locations'}}

{% if is_incremental %}

inc()

{% endif %}