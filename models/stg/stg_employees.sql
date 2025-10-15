{{
    config(
        materialized='table',
        database = 'dev',
        schema = 'stg'
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
LOAD_TIME
from {{source('hr','src_employees')}} as src