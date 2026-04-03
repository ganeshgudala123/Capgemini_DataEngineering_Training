from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, desc

# Create Spark session
spark = SparkSession.builder.appName("Phase3").getOrCreate()

# -------------------- STEP 1: CREATE DATA --------------------

customers = spark.createDataFrame([
    (1, "Ravi", "Hyderabad"),
    (2, "Sita", "Chennai"),
    (3, "Arun", "Bangalore"),
    (4, "John", "Delhi")
], ["customer_id", "customer_name", "city"])

orders = spark.createDataFrame([
    (101, 1, 500, "2024-01-01"),
    (102, 2, 700, "2024-01-01"),
    (103, 1, 300, "2024-01-02"),
    (104, 3, 400, "2024-01-02"),
    (105, None, 200, "2024-01-03")  # bad data
], ["order_id", "customer_id", "amount", "order_date"])


# -------------------- STEP 2: CLEANING --------------------

# Remove rows where customer_id is NULL
orders_clean = orders.dropna(subset=["customer_id"])


# -------------------- STEP 3: TRANSFORMATIONS --------------------

# Join customers and orders
df = customers.join(orders_clean, "customer_id")


# 1. City-wise revenue
print("City-wise Revenue")
city_revenue = df.groupBy("city") \
    .agg(sum("amount").alias("revenue"))
city_revenue.show()


# 2. Repeat customers (>2 orders)
print("Repeat Customers")
repeat_customers = df.groupBy("customer_name") \
    .agg(count("order_id").alias("order_count")) \
    .filter("order_count > 2")
repeat_customers.show()


# 3. Highest spending customer
print("Top Customer")
top_customer = df.groupBy("customer_name") \
    .agg(sum("amount").alias("total_spend")) \
    .orderBy(desc("total_spend"))
top_customer.show()


# 4. Final report
print("Final Report")
final_report = df.groupBy("customer_name", "city") \
    .agg(
        sum("amount").alias("total_spend"),
        count("order_id").alias("order_count")
    )
final_report.show()
