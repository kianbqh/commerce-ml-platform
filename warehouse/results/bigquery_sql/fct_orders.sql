/* Order grain fact with basket metrics; only orders that have line items. */
WITH lines AS (
  SELECT
    order_id,
    COUNT(*) AS basket_size,
    SUM(reordered) AS n_reordered,
    COUNT(DISTINCT department_id) AS n_departments
  FROM `warehouse`.`marts`.`fct_order_lines`
  WHERE
    NOT order_id IN (
      SELECT
        order_id
      FROM `warehouse`.`marts`.`fct_orders`
    )
  GROUP BY
    order_id
)
SELECT
  o.order_id,
  o.user_id,
  o.eval_set,
  o.order_number,
  o.order_dow,
  o.order_hour_of_day,
  o.days_since_prior_order,
  o.days_since_first_order,
  l.basket_size,
  l.n_reordered,
  l.n_departments,
  CASE
    WHEN o.order_number = 1
    THEN NULL
    ELSE CAST(l.n_reordered AS FLOAT64) / l.basket_size
  END AS reorder_share
FROM lines AS l
JOIN `warehouse`.`intermediate`.`int_orders_timeline` AS o
  ON o.order_id = l.order_id
