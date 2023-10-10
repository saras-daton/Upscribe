{% macro extract_nested_value(variable1, variable2, variable3) %}
{% if target.type =='snowflake' %}
  {% if variable3 == 'string' %}
    coalesce({{variable1}}.value:{{variable2}}, 'N/A')::{{variable3}}
  {% elif variable3 == 'boolean' %}
    coalesce({{variable1}}.value:{{variable2}}, false)::{{variable3}}
  {% elif variable3 == 'TIMESTAMP' %}
    to_timestamp(coalesce({{variable1}}.value:{{variable2}}, '1970-01-01 00:00:00'))::{{variable3}}
  {% else %}
    coalesce({{variable1}}.value:{{variable2}}, 0)::{{variable3}}
  {% endif %}
{% else %}
  {% if variable3 == 'string' %}
    cast(coalesce({{variable1}}.{{variable2}}, 'N/A') as {{variable3}})
  {% elif variable3 == 'boolean' %}
    cast(coalesce({{variable1}}.{{variable2}}, false) as {{variable3}})
  {% elif variable3 == 'TIMESTAMP' %}
    cast(coalesce({{variable1}}.{{variable2}}, '1970-01-01 00:00:00') as {{variable3}})
  {% else %}
    cast(coalesce({{variable1}}.{{variable2}}, 0) as {{variable3}})
  {% endif %}
{% endif %}
{% endmacro %}