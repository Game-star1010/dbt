{{ 
    config(
        materialized = 'incremental',
        unique_key = 'country_id',
        incremental_statergy = 'delete+insert',
        database = 'dev',
        schema = 'dim',
        tags = ['DIM']
    )
}}

select
    COUNTRY_ID,
    COUNTRY_NAME,
    REGION_ID,
    current_timestamp as LOAD_TIME
from {{ ref('stg_countries')}} as src

{% if is_incremental %}

{{ incr() }}

{% endif %}



