-- By default, dbt appends custom schema with target.schema name
-- This macro is to override this default dehaviour of dbt. This enables us to use custom schemas for model creation based on requirements

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
