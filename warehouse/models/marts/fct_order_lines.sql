{{ config(materialized='incremental', incremental_strategy='append', on_schema_change='fail') }}
-- Line grain fact. Incremental: append only lines of orders not yet loaded (anti-join works on DuckDB and BigQuery).
select
    l.order_id,
    l.product_id,
    o.user_id,
    o.order_number,
    o.order_dow,
    o.order_hour_of_day,
    p.aisle_id,
    p.department_id,
    l.add_to_cart_order,
    l.reordered,
    l.eval_set
from {{ ref('stg_order_products') }} l
join {{ ref('stg_orders') }} o on o.order_id = l.order_id
join {{ ref('stg_products') }} p on p.product_id = l.product_id
{% if is_incremental() %}
where not exists (select 1 from {{ this }} t where t.order_id = l.order_id)
{% endif %}
