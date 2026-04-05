from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, when
from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank

spark = SparkSession.builder.appName("Phase4A").getOrCreate()

# SAMPLE DATA 

customers = spark.createDataFrame([
    (1, "Ravi", "Hyderabad"),
    (2, "Sita", "Chennai"),
    (3, "Arun", "Bangalore"),
    (4, "John", "Delhi")
], ["customer_id", "customer_name", "city"])

orders = spark.createDataFrame([
    (101, 1, 5000),
    (102, 1, 3000),
    (103, 2, 7000),
    (104, 3, 2000),
    (105, 3, 4000)
], ["order_id", "customer_id", "amount"])

# TOTAL SPEND 

df = customers.join(orders, "customer_id")

spend_df = df.groupBy("customer_name") \
    .agg(sum("amount").alias("total_spend"))

spend_df.show()


# METHOD 1: CONDITIONAL SEGMENTATION 

segmented = spend_df.withColumn(
    "segment",
    when(spend_df.total_spend > 10000, "Gold")
    .when((spend_df.total_spend >= 5000), "Silver")
    .otherwise("Bronze")
)

print("Segmentation using when()")
segmented.show()


# TASK 2: COUNT PER SEGMENT 

segmented.groupBy("segment").count().show()


# METHOD 2: QUANTILE SEGMENTATION 

quantiles = spend_df.approxQuantile("total_spend", [0.33, 0.66], 0)

q1 = quantiles[0]
q2 = quantiles[1]

quantile_segmented = spend_df.withColumn(
    "segment",
    when(spend_df.total_spend <= q1, "Bronze")
    .when((spend_df.total_spend <= q2), "Silver")
    .otherwise("Gold")
)

print("Quantile Segmentation")
quantile_segmented.show()


# METHOD 3: WINDOW RANKING 

window = Window.orderBy("total_spend")

ranked = spend_df.withColumn("rank_pct", percent_rank().over(window))

ranked.show()
