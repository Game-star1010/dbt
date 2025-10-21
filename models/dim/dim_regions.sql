{{
    config(
        materialized = 'incremental',
        unique_key = 'REGION_ID',
        incremental_statergy = 'delete+insert',
        schema = 'dim',
        tags = ['dim']
    )
}}
select
REGION_ID,
REGION_NAME,
LOAD_TIME
from {{ ref('src_regions')}}

{% if is_incremental %}

inc()

{% endif %}