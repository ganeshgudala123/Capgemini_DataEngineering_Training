-- Daily Sales
SELECT order_date, SUM(amount)
FROM orders
GROUP BY order_date;

-- City Revenue
SELECT c.city, SUM(o.amount)
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.city;

-- Top Customers
SELECT c.customer_name, SUM(o.amount) AS total_spend
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_name
ORDER BY total_spend DESC
LIMIT 5;

-- Repeat Customers
SELECT customer_id, COUNT(*)
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Final Report
SELECT c.customer_name, c.city,
       SUM(o.amount), COUNT(o.order_id)
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_name, c.city;
