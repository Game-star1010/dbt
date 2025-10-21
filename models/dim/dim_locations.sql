{{
    config(
        materialized='incremental',
        unique_key = 'location_id',
        incremental_statergy = 'delete+insert',
        database = 'dev',
        schema = 'DIM',
        tags = ['DIM']
    )
}}
select
LOCATION_ID,
STREET_ADDRESS,
POSTAL_CODE,
CITY,
STATE_PROVINCE,
COUNTRY_ID,
current_timestamp as LOAD_TIME
from {{ ref('stg_locations')}} as src

{% if is_incremental %}

{{ incr() }}

{% endif %}
