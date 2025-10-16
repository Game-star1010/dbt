{{
    config(materialized = 'table',
           database = 'dbt',
           schema = 'stg')
}}

select * from dbt.hr.employees