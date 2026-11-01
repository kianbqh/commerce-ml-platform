select
    user_id,
    count(*)                                   as n_orders,
    max(days_since_first_order)                as observed_order_history_days,
    avg(days_since_prior_order)                as avg_days_between_orders,
    avg(cast(basket_size as double))           as avg_basket_size,
    sum(n_reordered) * 1.0 / sum(basket_size)  as reorder_rate,
    case when count(*) >= 40 then 'power'
         when count(*) >= 10 then 'regular'
         else 'light' end                      as segment
from {{ ref('fct_orders') }}
group by user_id
