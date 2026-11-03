{{ config(materialized='incremental', incremental_strategy='delete+insert',
          unique_key=['order_id', 'product_id'], on_schema_change='fail') }}
-- Existing-key correction support. Complete affected order lines replace keys.
-- This contract does not support deletions or primary-key changes.
select
    l.order_id, l.product_id, o.user_id, o.order_number, o.order_dow, o.order_hour_of_day,
    p.aisle_id, p.department_id, l.add_to_cart_order, l.reordered, l.eval_set
from {{ ref('stg_order_products') }} l
join {{ ref('stg_orders') }} o on o.order_id = l.order_id
join {{ ref('stg_products') }} p on p.product_id = l.product_id
{% if is_incremental() %}
where {{ m4_affected('o.order_number') }}
{% endif %}
