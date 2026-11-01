select
    order_dow, order_hour_of_day,
    count(*)                          as n_orders,
    avg(cast(basket_size as double))  as avg_basket_size,
    avg(reorder_share)                as avg_reorder_share
from {{ ref('fct_orders') }}
group by order_dow, order_hour_of_day
