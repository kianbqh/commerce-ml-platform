SELECT
  user_id,
  COUNT(*) AS n_orders,
  MAX(days_since_first_order) AS observed_order_history_days,
  AVG(days_since_prior_order) AS avg_days_between_orders,
  AVG(CAST(basket_size AS FLOAT64)) AS avg_basket_size,
  SUM(n_reordered) * 1.0 / SUM(basket_size) AS reorder_rate,
  CASE
    WHEN COUNT(*) >= 40
    THEN 'power'
    WHEN COUNT(*) >= 10
    THEN 'regular'
    ELSE 'light'
  END AS segment
FROM `warehouse`.`marts`.`fct_orders`
GROUP BY
  user_id
