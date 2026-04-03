CREATE TABLE customers (
    customer_id INT,
    customer_name VARCHAR(50),
    city VARCHAR(50),
    age INT
);

INSERT INTO customers VALUES
(1, 'Ganesh', 'Hyderabad', 23),
(2, 'Hari', 'Chennai', 30),
(3, 'Vihar', 'Bangalore', 27),
(4, 'Varma', 'Hyderabad', 35),
(5, 'Anil', 'Chennai', 22),
(6, 'Kiran', 'Delhi', 29);

SELECT * FROM customers;

SELECT * FROM customers WHERE city = 'Chennai';

SELECT * FROM customers WHERE age > 25;

SELECT customer_name, city FROM customers;

SELECT city, COUNT(*) FROM customers GROUP BY city;
