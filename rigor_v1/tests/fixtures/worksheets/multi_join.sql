-- Multi-table join with order details
SELECT
    c.name AS customer_name,
    o.order_date,
    p.name AS product_name,
    oi.quantity,
    oi.unit_price
FROM CUSTOMERS c
JOIN ORDERS o ON o.CUSTOMER_ID = c.ID
JOIN ORDER_ITEMS oi ON oi.ORDER_ID = o.ID
JOIN PRODUCTS p ON oi.PRODUCT_ID = p.ID
WHERE c.region = 'WEST';
