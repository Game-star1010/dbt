{{
    config(
        materialized='table'
    )
}}

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
(emp.salary + HRA + allowances + PF) as gross_income,
(emp.salary + HRA + allowances) as net_income
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