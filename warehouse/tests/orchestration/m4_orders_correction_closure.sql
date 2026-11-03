{{ config(tags=['m4_output'], meta={'dagster': {'ref': {'name': 'fct_orders'}}}) }}
with recomputed as (
    select order_id,count(*) basket_size,sum(reordered) n_reordered,count(distinct department_id) n_departments
    from {{ ref('fct_order_lines') }} group by order_id
)
select coalesce(r.order_id,o.order_id) order_id
from recomputed r full join {{ ref('fct_orders') }} o using(order_id)
where r.order_id is null or o.order_id is null or r.basket_size<>o.basket_size
   or r.n_reordered<>o.n_reordered or r.n_departments<>o.n_departments
