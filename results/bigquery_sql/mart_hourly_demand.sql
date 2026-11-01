SELECT
  order_dow,
  order_hour_of_day,
  COUNT(*) AS n_orders,
  AVG(CAST(basket_size AS FLOAT64)) AS avg_basket_size,
  AVG(reorder_share) AS avg_reorder_share
FROM `warehouse`.`marts`.`fct_orders`
GROUP BY
  order_dow,
  order_hour_of_day
