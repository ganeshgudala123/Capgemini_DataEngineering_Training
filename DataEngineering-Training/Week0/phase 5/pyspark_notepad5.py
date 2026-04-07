from pyspark.sql.functions import sum, count, desc
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, dense_rank

# LOAD DATA 
customers = spark.read.csv("/FileStore/olist/olist_customers_dataset.csv", header=True)
orders = spark.read.csv("/FileStore/olist/olist_orders_dataset.csv", header=True)
payments = spark.read.csv("/FileStore/olist/olist_order_payments_dataset.csv", header=True)
products = spark.read.csv("/FileStore/olist/olist_products_dataset.csv", header=True)

# JOIN DATA 
df = orders.join(customers, "customer_id") \
           .join(payments, "order_id")

#  TASK 1: TOP CUSTOMERS PER CITY 

city_window = Window.partitionBy("customer_city").orderBy(desc("payment_value"))

top_customers = df.groupBy("customer_city", "customer_id") \
    .agg(sum("payment_value").alias("total_spend")) \
    .withColumn("rank", rank().over(city_window))

top_customers.show(3)

# TASK 2: RUNNING TOTAL 
date_sales = df.groupBy("order_purchase_timestamp") \
    .agg(sum("payment_value").alias("daily_sales"))

running_window = Window.orderBy("order_purchase_timestamp")

running_total = date_sales.withColumn(
    "running_total",
    sum("daily_sales").over(running_window)
)

running_total.show()

# TASK 4: CUSTOMER LIFETIME VALUE 
clv = df.groupBy("customer_id") \
    .agg(sum("payment_value").alias("total_spend"))

clv.show()

# TASK 5: SEGMENTATION 
from pyspark.sql.functions import when

segmented = clv.withColumn(
    "segment",
    when(clv.total_spend > 10000, "Gold")
    .when(clv.total_spend >= 5000, "Silver")
    .otherwise("Bronze")
)

segmented.show()

# FINAL REPORT
final_df = segmented.join(customers, "customer_id") \
    .groupBy("customer_id", "customer_city", "segment") \
    .agg(count("*").alias("total_orders"))

final_df.show()
