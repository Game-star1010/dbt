{{
    config(materialized = 'table',
           database = 'dbt',
           schema = 'dev')
}}

select * from dbt.hr.employees