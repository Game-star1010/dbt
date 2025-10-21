{{
    config(
        materialized='incremental',
        unique_key = 'DEPARTMENT_ID',
        incremental_statergy = 'delete+insert',
        database = 'dev',
        schema = 'dim',
        tags = ['DIM']
    )
}}

select 
DEPARTMENT_ID,
DEPARTMENT_NAME,
MANAGER_ID,
LOCATION_ID,
current_timestamp as LOAD_TIME
from {{ ref('stg_departments')}} as src

{% if is_incremental %}

{{ incr() }}

{% endif %}

