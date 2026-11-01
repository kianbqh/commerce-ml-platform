-- Observed span shares must be non-increasing in the span threshold.
select * from (
    select span_threshold_days, observed_span_share, lag(observed_span_share) over (order by span_threshold_days) as prev from {{ ref('mart_user_retention') }}
) x where prev is not null and observed_span_share > prev + 1e-12
