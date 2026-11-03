{{ config(materialized='incremental', incremental_strategy='delete+insert',
          unique_key='order_id', on_schema_change='fail') }}
-- Corrections must update basket metrics too, not only line rows.
with lines as (
    select order_id, count(*) as basket_size, sum(reordered) as n_reordered,
           count(distinct department_id) as n_departments
    from {{ ref('fct_order_lines') }}
    {% if is_incremental() %}
    where {{ m4_affected('order_number') }}
    {% endif %}
    group by order_id
)
select
    o.order_id, o.user_id, o.eval_set, o.order_number, o.order_dow, o.order_hour_of_day,
    o.days_since_prior_order, o.days_since_first_order,
    l.basket_size, l.n_reordered, l.n_departments,
    case when o.order_number = 1 then null else cast(l.n_reordered as double) / l.basket_size end as reorder_share
from lines l
join {{ ref('int_orders_timeline') }} o on o.order_id = l.order_id
