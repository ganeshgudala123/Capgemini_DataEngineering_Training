from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, desc

spark = SparkSession.builder.appName("Phase2").getOrCreate()

# Sample Data
customers = spark.createDataFrame([
    (1, "Ravi", "Hyderabad"),
    (2, "Sita", "Chennai"),
    (3, "Arun", "Hyderabad"),
    (4, "Kiran", "Delhi")
], ["customer_id", "customer_name", "city"])

orders = spark.createDataFrame([
    (101, 1, 200),
    (102, 2, 500),
    (103, 1, 300),
    (104, 3, 400)
], ["order_id", "customer_id", "amount"])

# Cleaning
customers = customers.dropna(subset=["customer_id"])
orders = orders.dropna(subset=["customer_id"])

# Total order amount per customer
customers.join(orders, "customer_id") \
    .groupBy("customer_name") \
    .agg(sum("amount").alias("total_spend")) \
    .show()

# Top 3 customers
customers.join(orders, "customer_id") \
    .groupBy("customer_name") \
    .agg(sum("amount").alias("total_spend")) \
    .orderBy(desc("total_spend")) \
    .show(3)

# Customers with no orders
customers.join(orders, "customer_id", "left") \
    .filter(orders.customer_id.isNull()) \
    .show()

# City-wise revenue
customers.join(orders, "customer_id") \
    .groupBy("city") \
    .agg(sum("amount").alias("revenue")) \
    .show()

# Average order amount
orders.groupBy("customer_id") \
    .agg(avg("amount").alias("avg_amount")) \
    .show()

# Customers with more than one order
orders.groupBy("customer_id") \
    .count() \
    .filter("count > 1") \
    .show()

# Sort by spend
customers.join(orders, "customer_id") \
    .groupBy("customer_name") \
    .agg(sum("amount").alias("total_spend")) \
    .orderBy(desc("total_spend")) \
    .show()
