/* Market-basket association rules among the top-N products: support, confidence and lift. */
WITH top_products AS (
  SELECT
    product_id
  FROM `warehouse`.`marts`.`dim_products`
  WHERE
    popularity_rank <= 1000
), lines AS (
  SELECT
    l.order_id,
    l.product_id
  FROM `warehouse`.`marts`.`fct_order_lines` AS l
  JOIN top_products AS t
    ON t.product_id = l.product_id
), n_orders AS (
  SELECT
    COUNT(*) AS n
  FROM `warehouse`.`marts`.`fct_orders`
), item_cnt AS (
  SELECT
    product_id,
    COUNT(*) AS cnt
  FROM lines
  GROUP BY
    product_id
), pairs AS (
  SELECT
    a.product_id AS product_a,
    b.product_id AS product_b,
    COUNT(*) AS n_together
  FROM lines AS a
  JOIN lines AS b
    ON a.order_id = b.order_id AND a.product_id < b.product_id
  GROUP BY
    a.product_id,
    b.product_id
  HAVING
    COUNT(*) >= 200
)
SELECT
  p.product_a,
  pa.product_name AS product_a_name,
  p.product_b,
  pb.product_name AS product_b_name,
  p.n_together,
  p.n_together * 1.0 / n.n AS support,
  p.n_together * 1.0 / ca.cnt AS confidence_a_to_b,
  p.n_together * 1.0 * n.n / (
    ca.cnt * cb.cnt
  ) AS lift
FROM pairs AS p
CROSS JOIN n_orders AS n
JOIN item_cnt AS ca
  ON ca.product_id = p.product_a
JOIN item_cnt AS cb
  ON cb.product_id = p.product_b
JOIN `warehouse`.`marts`.`dim_products` AS pa
  ON pa.product_id = p.product_a
JOIN `warehouse`.`marts`.`dim_products` AS pb
  ON pb.product_id = p.product_b
