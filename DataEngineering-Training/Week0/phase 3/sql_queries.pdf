
-- STEP 1: CREATE TABLES

DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS orders;

CREATE TABLE customers (
    customer_id INT,
    customer_name TEXT,
    city TEXT
);

CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    amount INT,
    order_date TEXT
);


-- STEP 2: INSERT DATA

INSERT INTO customers VALUES
(1, 'Ravi', 'Hyderabad'),
(2, 'Sita', 'Chennai'),
(3, 'Arun', 'Bangalore'),
(4, 'John', 'Delhi');

INSERT INTO orders VALUES
(101, 1, 500, '2024-01-01'),
(102, 2, 700, '2024-01-01'),
(103, 1, 300, '2024-01-02'),
(104, 3, 400, '2024-01-02'),
(105, NULL, 200, '2024-01-03');  -- bad data

-- STEP 3: CLEANING

DELETE FROM orders WHERE customer_id IS NULL;

-- STEP 4: TRANSFORMATIONS

-- City-wise revenue
SELECT c.city, SUM(o.amount) AS revenue
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.city;

-- Repeat customers (>2 orders)
SELECT c.customer_name, COUNT(o.order_id) AS order_count
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_name
HAVING COUNT(o.order_id) > 2;

-- Highest spending customer
SELECT c.customer_name, SUM(o.amount) AS total_spend
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_name
ORDER BY total_spend DESC;

-- Final report
SELECT c.customer_name, c.city,
       SUM(o.amount) AS total_spend,
       COUNT(o.order_id) AS order_count
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_name, c.city;
