{% macro next(column_name) %}
     Lead({{column_name}}, 1, NULL) OVER(
       ORDER BY created_date ASC) 
{% endmacro %}