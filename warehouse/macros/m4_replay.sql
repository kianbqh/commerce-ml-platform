{% macro m4_batch_index(order_number) -%}
  {%- set width = var('m4_orders_per_batch', 10) | int -%}
  {%- if width < 1 -%}{{ exceptions.raise_compiler_error('m4_orders_per_batch must be positive') }}{%- endif -%}
  cast(floor(({{ order_number }} - 1) / {{ width }}.0) as bigint)
{%- endmacro %}

{% macro m4_batch_key(order_number) -%}
  lpad(cast({{ m4_batch_index(order_number) }} as varchar), 6, '0')
{%- endmacro %}

{% macro m4_visible(order_number) -%}
  {%- if var('m4_enabled', false) -%}
    {%- if var('m4_prefix', -1) | int < 0 -%}{{ exceptions.raise_compiler_error('explicit m4_prefix is required') }}{%- endif -%}
    {{ m4_batch_index(order_number) }} <= {{ var('m4_prefix') | int }}
  {%- else -%}true{%- endif -%}
{%- endmacro %}

{% macro m4_affected(order_number) -%}
  {%- if var('m4_enabled', false) -%}
    {%- set selected = var('m4_affected_batches', []) -%}
    {%- if selected | length == 0 -%}{{ exceptions.raise_compiler_error('explicit nonempty m4_affected_batches is required') }}{%- endif -%}
    {{ m4_batch_index(order_number) }} in ({% for b in selected %}{{ b | int }}{% if not loop.last %}, {% endif %}{% endfor %})
  {%- else -%}true{%- endif -%}
{%- endmacro %}

{% macro m4_overrides() -%}
  {%- if var('m4_enabled', false) and var('m4_overlay_path', '') -%}
    read_csv('{{ var("m4_overlay_path") | replace("'", "''") }}', header=true,
      columns={'order_id':'BIGINT','product_id':'BIGINT','new_reordered':'INTEGER','new_product_id':'BIGINT'})
  {%- else -%}
    (select cast(null as bigint) order_id, cast(null as bigint) product_id,
      cast(null as integer) new_reordered, cast(null as bigint) new_product_id where false)
  {%- endif -%}
{%- endmacro %}
