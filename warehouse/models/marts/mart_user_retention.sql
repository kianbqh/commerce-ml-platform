-- Share of users whose observed order-history span is at least N reconstructed days.
-- This is an empirical span distribution; it does not model return events or censoring.
with horizons as (
    {% for n in [7, 14, 30, 60, 90, 180, 270, 365] %}
    select {{ n }} as span_threshold_days {% if not loop.last %}union all{% endif %}
    {% endfor %}
),
u as (select user_id, observed_order_history_days, segment from {{ ref('dim_users') }})
select
    h.span_threshold_days,
    count(*)                                                         as n_users,
    sum(case when u.observed_order_history_days >= h.span_threshold_days then 1 else 0 end)        as n_users_with_span_ge_n,
    sum(case when u.observed_order_history_days >= h.span_threshold_days then 1 else 0 end) * 1.0 / count(*) as observed_span_share
from horizons h cross join u
group by h.span_threshold_days
