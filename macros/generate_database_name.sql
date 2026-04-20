{% macro generate_database_name(custom_database_name, node) -%}

    {%- set default_catalog = target.catalog -%}
    
    {%- if target.name == 'dev' and node.config.schema == 'raw' -%}
        prod

    {%- elif custom_database_name is not none -%}
        {{ custom_database_name | trim }}    

    {%- else -%}
        {{default_catalog}}

    {%- endif -%}
    
{%- endmacro %}
