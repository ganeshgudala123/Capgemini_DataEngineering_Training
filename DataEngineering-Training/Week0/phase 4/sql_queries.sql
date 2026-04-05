-- Create Customers Table
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    customer_id INTEGER,
    customer_name TEXT,
    city TEXT,
    age INTEGER
);

INSERT INTO customers VALUES
(1, 'Ravi', 'Hyderabad', 25),
(2, 'Meena', 'Chennai', 30),
(3, 'John', 'Bangalore', 28),
(4, 'Arun', 'Hyderabad', 35),
(5, 'Kiran', 'Chennai', 40);

-- Create Orders Table
DROP TABLE IF EXISTS orders;-- Daily Sales
CREATE TABLE orders (
    order_id INTEGER,
    customer_id INTEGER,
    amount INTEGER,
    order_date TEXT
);

INSERT INTO orders VALUES
(101, 1, 5000, '2024-01-01'),
(102, 1, 7000, '2024-01-02'),
(103, 2, 3000, '2024-01-01'),
(104, 3, 8000, '2024-01-03'),
(105, 3, 2000, '2024-01-03'),
(106, 4, 15000, '2024-01-04'),
(107, 5, 1000, '2024-01-02');

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
