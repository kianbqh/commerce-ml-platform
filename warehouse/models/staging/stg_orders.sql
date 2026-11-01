select
    cast(order_id as bigint)                      as order_id,
    cast(user_id as bigint)                       as user_id,
    cast(eval_set as varchar)                     as eval_set,
    cast(order_number as integer)                 as order_number,
    cast(order_dow as integer)                    as order_dow,
    cast(order_hour_of_day as integer)            as order_hour_of_day,
    cast(days_since_prior_order as double)        as days_since_prior_order
from {{ raw_source('orders') }}
