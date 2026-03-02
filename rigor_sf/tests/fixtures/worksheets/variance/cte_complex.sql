WITH orders_base AS (
  SELECT o.id, o.customer_id, o.order_ts
  FROM SALES.PUBLIC.ORDERS o
),
customer_dim AS (
  SELECT c.id, c.account_id
  FROM SALES.PUBLIC.CUSTOMERS c
)
SELECT *
FROM orders_base ob
JOIN customer_dim cd
  ON ob.customer_id = cd.id
JOIN SALES.PUBLIC.ACCOUNTS a
  ON cd.account_id = a.id;
