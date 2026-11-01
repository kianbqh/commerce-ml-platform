{%- set sets = var('eval_sets') -%}
{% for s in sets %}
select
    cast(order_id as bigint)          as order_id,
    cast(product_id as bigint)        as product_id,
    cast(add_to_cart_order as integer) as add_to_cart_order,
    cast(reordered as integer)        as reordered,
    '{{ s }}'                         as eval_set
from {{ raw_source('order_products__' ~ s) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
