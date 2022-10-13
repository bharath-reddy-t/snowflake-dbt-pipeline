{% macro prev(column_name) %}
     lag({{column_name}}) over(partition by deal_id
       order by created_date asc) 
{% endmacro %}