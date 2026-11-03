{{ config(tags=['m4_input'], meta={'dagster': {'ref': {'name': 'stg_order_products'}}}) }}
-- A dropped invalid product must fail BEFORE promotion, never reconcile away.
select l.order_id, l.product_id
from {{ ref('stg_order_products') }} l
left join {{ ref('stg_products') }} p on p.product_id=l.product_id
left join {{ ref('stg_orders') }} o on o.order_id=l.order_id
where l.order_id is null or l.product_id is null or o.order_id is null or p.product_id is null
   or l.add_to_cart_order is null or l.reordered is null or l.reordered not in (0,1)
   or l.add_to_cart_order < 1
