{{
    config(
        materialized='incremental',
        unique_key = 'employee_id',
        incremental_statergy = 'delete+insert',
        tags = ['fct'],
        schema = 'fct'
    )
}}
{% set x = "(select coalesce(max(load_time), '1900-01-01') from "~this~")" %}
Select 
emp.employee_id,
jobs.job_id,
dept.department_id,
con.country_id,
reg.region_id,
emp.salary,
salary_date,
HRA,
allowances,
PF,
to_decimal((emp.salary + HRA + allowances + PF),10,2) as gross_income,
to_decimal((emp.salary + HRA + allowances),10,2) as net_income,
current_timestamp as load_time
From {{ ref('stg_salary')}} as sal
Left join {{ ref('dim_employees')}} as emp
on sal.employee_id = emp.employee_id
Left join {{ ref('dim_departments')}} as dept
on emp.department_id = dept.department_id
Left join {{ ref('dim_locations')}} as loc
on dept.location_id = loc.location_id
Left join {{ ref('dim_countries')}} as con
on loc.country_id = con.country_id
Left join {{ ref('dim_regions')}} as reg
on con.region_id = reg.region_id
Left join {{ ref('dim_jobs')}} as jobs
on jobs.job_id = emp.job_id



{% if is_incremental() %}

where sal.load_time > {{ x }}

or    emp.load_time > {{ x }}

or    dept.load_time > {{ x }}

or    loc.load_time > {{ x }}

or    con.load_time > {{ x }}

or    reg.load_time > {{ x }}

or    jobs.load_time > {{ x }}

{% endif %}

