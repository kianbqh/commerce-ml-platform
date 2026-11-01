-- Instacart has no calendar dates, only days since the previous order (capped at 30).
-- Rebuild a per-user timeline: day offset of each order relative to the user's first order.
select
    o.*,
    coalesce(sum(coalesce(o.days_since_prior_order, 0)) over (
        partition by o.user_id order by o.order_number
        rows between unbounded preceding and current row), 0) as days_since_first_order
from {{ ref('stg_orders') }} o
