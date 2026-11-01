{{ config(materialized='table', schema='recommendations') }}
select cast(o.user_id as integer) as user_id, cast(l.product_id as integer) as product_id,
       cast(o.order_id as integer) as order_id, o.order_number, o.split,
       l.reordered, p.aisle_id, p.department_id
from {{ ref('stg_order_products') }} l
join {{ ref('rec_order_split') }} o using (order_id)
join {{ ref('stg_products') }} p using (product_id)
