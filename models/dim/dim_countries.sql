{{ 
    config(
        materialized = 'incremental',
        unique_key = 'country_id',
        incremental_statergy = 'delete+insert',
        schema = 'dim',
        tags = ['DIM']
    )
}}

select
    COUNTRY_ID,
    COUNTRY_NAME,
    REGION_ID,
    LOAD_TIME
from {{ ref('stg_countries')}}

{% if is_incremental %}

inc()

{% endif %}