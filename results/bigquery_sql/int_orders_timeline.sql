/* Instacart has no calendar dates, only days since the previous order (capped at 30). */
/* Rebuild a per-user timeline: day offset of each order relative to the user's first order. */
SELECT
  o.*,
  COALESCE(
    SUM(COALESCE(o.days_since_prior_order, 0)) OVER (
      PARTITION BY o.user_id
      ORDER BY o.order_number
      rows BETWEEN UNBOUNDED preceding AND CURRENT ROW
    ),
    0
  ) AS days_since_first_order
FROM `warehouse`.`staging`.`stg_orders` AS o
