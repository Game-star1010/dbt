{% macro inc() %}

where load_time > (
                   select coalese(max(load_time),'1900-01-01') 
                   from {{ this}}
                   )

{% endmacro %}