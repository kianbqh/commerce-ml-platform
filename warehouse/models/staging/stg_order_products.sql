{%- set sets = var('eval_sets') -%}
with raw_lines as (
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
)
select l.order_id,
       coalesce(x.new_product_id, l.product_id) as product_id,
       l.add_to_cart_order,
       coalesce(x.new_reordered, l.reordered) as reordered,
       l.eval_set
from raw_lines l
left join {{ m4_overrides() }} x on x.order_id=l.order_id and x.product_id=l.product_id
{% if var('m4_enabled', false) %}
-- Metadata exposes only received prefixes. No hidden future basket contents.
where exists (select 1 from {{ ref('stg_orders') }} o where o.order_id=l.order_id)
{% endif %}
