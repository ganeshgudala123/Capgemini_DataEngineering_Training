from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Phase3A").getOrCreate()

# -------------------- RAW DATA --------------------
data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, None, "Chennai", 32),
    (None, "Arun", "Hyderabad", 28),
    (4, "Meena", None, 30),
    (4, "Meena", None, 30),
    (5, "John", "Bangalore", -5)
]

columns = ["customer_id", "name", "city", "age"]

df = spark.createDataFrame(data, columns)

print("Original Data")
df.show()

# -------------------- CLEANING --------------------

# 1. Remove rows with null customer_id
df_clean = df.dropna(subset=["customer_id"])

# 2. Remove rows with null name or city
df_clean = df_clean.dropna(subset=["name", "city"])

# 3. Remove duplicates
df_clean = df_clean.dropDuplicates()

# 4. Remove invalid age (< 0)
df_clean = df_clean.filter(df_clean.age > 0)

print("Cleaned Data")
df_clean.show()

# -------------------- VALIDATION --------------------

print("Row count before cleaning:", df.count())
print("Row count after cleaning:", df_clean.count())

# -------------------- AGGREGATION --------------------

print("Customers per city")
df_clean.groupBy("city").count().show()
