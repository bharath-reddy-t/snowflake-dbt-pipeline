{% macro next(column_name) %}
     lead({{column_name}}) over(partition by deal_id
       order by created_date asc) 
{% endmacro %}