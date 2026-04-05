from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, desc, when

spark = SparkSession.builder.appName("Phase4").getOrCreate()

# DATA 

customers = spark.createDataFrame([
    (1, "Ravi", "Hyderabad"),
    (2, "Sita", "Chennai"),
    (3, "Arun", "Bangalore"),
    (4, "John", "Delhi")
], ["customer_id", "customer_name", "city"])

orders = spark.createDataFrame([
    (101, 1, 5000, "2024-01-01"),
    (102, 1, 3000, "2024-01-02"),
    (103, 2, 7000, "2024-01-01"),
    (104, 3, 2000, "2024-01-02"),
    (105, 3, 4000, "2024-01-03"),
    (106, 4, -100, "2024-01-03")  # invalid
], ["order_id", "customer_id", "amount", "order_date"])


#  CLEANING

orders_clean = orders.filter(orders.amount > 0)

df = customers.join(orders_clean, "customer_id")


#  TASK 1: DAILY SALES
print("Daily Sales")
df.groupBy("order_date").agg(sum("amount").alias("total_sales")).show()


#  TASK 2: CITY REVENUE
print("City Revenue")
df.groupBy("city").agg(sum("amount").alias("total_revenue")).show()


#  TASK 3: TOP 5 CUSTOMERS 
print("Top Customers")
top_customers = df.groupBy("customer_name") \
    .agg(sum("amount").alias("total_spend")) \
    .orderBy(desc("total_spend"))

top_customers.show(5)


# TASK 4: REPEAT CUSTOMERS 
print("Repeat Customers")
repeat_customers = df.groupBy("customer_id") \
    .agg(count("order_id").alias("order_count")) \
    .filter("order_count > 1")

repeat_customers.show()


# TASK 5: CUSTOMER SEGMENTATION -
segmented = top_customers.withColumn(
    "segment",
    when(top_customers.total_spend > 10000, "Gold")
    .when((top_customers.total_spend >= 5000), "Silver")
    .otherwise("Bronze")
)

segmented.show()


# TASK 6: FINAL REPORT 
final_df = df.groupBy("customer_name", "city") \
    .agg(
        sum("amount").alias("total_spend"),
        count("order_id").alias("order_count")
    )

final_df = final_df.join(segmented, "customer_name")

print("Final Report")
final_df.show()
