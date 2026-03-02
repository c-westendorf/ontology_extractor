-- Simple customer-order join for testing
SELECT c.name, o.order_date, o.total
FROM CUSTOMERS c
JOIN ORDERS o ON o.CUSTOMER_ID = c.ID
WHERE o.order_date >= '2024-01-01';
