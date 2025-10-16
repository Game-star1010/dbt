{{
    config(materialized = 'table',
    tags = ['stg']
    )
}}

select
JOB_ID,
JOB_TITLE,
MIN_SALARY,
MAX_SALARY,
LOAD_TIME
from {{ source('hr','src_jobs')}}