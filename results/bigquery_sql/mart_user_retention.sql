/* Share of users whose observed order-history span is at least N reconstructed days. */
/* This is an empirical span distribution; it does not model return events or censoring. */
WITH horizons AS (
  SELECT
    7 AS span_threshold_days
  UNION ALL
  SELECT
    14 AS span_threshold_days
  UNION ALL
  SELECT
    30 AS span_threshold_days
  UNION ALL
  SELECT
    60 AS span_threshold_days
  UNION ALL
  SELECT
    90 AS span_threshold_days
  UNION ALL
  SELECT
    180 AS span_threshold_days
  UNION ALL
  SELECT
    270 AS span_threshold_days
  UNION ALL
  SELECT
    365 AS span_threshold_days
), u AS (
  SELECT
    user_id,
    observed_order_history_days,
    segment
  FROM `warehouse`.`marts`.`dim_users`
)
SELECT
  h.span_threshold_days,
  COUNT(*) AS n_users,
  SUM(
    CASE WHEN u.observed_order_history_days >= h.span_threshold_days THEN 1 ELSE 0 END
  ) AS n_users_with_span_ge_n,
  SUM(
    CASE WHEN u.observed_order_history_days >= h.span_threshold_days THEN 1 ELSE 0 END
  ) * 1.0 / COUNT(*) AS observed_span_share
FROM horizons AS h
CROSS JOIN u
GROUP BY
  h.span_threshold_days
