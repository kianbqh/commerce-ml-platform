{{ config(materialized='table', schema='recommendations') }}
with order_history as (
    select user_id,
           count(*) filter (where split = 'train') as train_orders,
           max(order_number) filter (where split = 'train') as train_cutoff,
           count(*) as test_orders, max(order_number) as test_cutoff
    from {{ ref('rec_order_split') }} where split != 'test' group by user_id
), products as (
    select stage, user_id, sum(purchase_count) as item_count, count(*) as unique_products
    from {{ ref('rec_history_product') }} group by stage, user_id
)
select p.*, case when stage = 'validation' then train_orders else test_orders end as history_orders,
       case when stage = 'validation' then train_cutoff else test_cutoff end as cutoff_order_number
from products p join order_history using (user_id)
