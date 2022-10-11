{% macro prev(column_name) %}
     LAG({{column_name}}, 1, NULL) OVER(
       ORDER BY created_date ASC) 
{% endmacro %}