{{
    config(
        materialized = 'incremental',
        unique_key = 'employee_id',
        incremental_statergy = 'delete+insert',
        tags = ['DIM'],
        database = 'dev',
        schema = 'dim'
    )
}}

select
EMPLOYEE_ID,
FIRST_NAME,
LAST_NAME,
EMAIL,
PHONE_NUMBER,
HIRE_DATE,
JOB_ID,
SALARY,
COMISSION_PCT,
MANAGER_ID,
DEPARTMENT_ID,
current_timestamp as LOAD_TIME
from {{ref('stg_employees')}} as src

{% if is_incremental() %}

{{ incr() }}

{% endif %}