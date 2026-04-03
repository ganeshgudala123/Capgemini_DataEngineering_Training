from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Phase1").getOrCreate()

#Create DataFrame (same as SQL table data)
customers = spark.createDataFrame([
    (1, "Ravi", "Hyderabad", 25),
    (2, "Sita", "Chennai", 32),
    (3, "Arun", "Hyderabad", 28)
], ["customer_id", "customer_name", "city", "age"])


SELECT * FROM customers;
print("1. Show all customers")
customers.show()


SELECT * FROM customers WHERE city = 'Chennai';
print("2. Customers from Chennai")
customers.filter(customers.city == "Chennai").show()


SELECT * FROM customers WHERE age > 25;
print("3. Customers with age > 25")
customers.filter(customers.age > 25).show()


SELECT customer_name, city FROM customers;
print("4. Select name and city")
customers.select("customer_name", "city").show()


SELECT city, COUNT(*) FROM customers GROUP BY city;
print("5. Count customers city-wise")
customers.groupBy("city").count().show()
