from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Phase1").getOrCreate()

customers = spark.createDataFrame([
    (1, "Ganesh", "Hyderabad", 23),
    (2, "Hari", "Chennai", 30),
    (3, "Vihar", "Bangalore", 27),
    (4, "Varma", "Hyderabad", 35),
    (5, "Anil", "Chennai", 22),
    (6, "Kiran", "Delhi", 29)
], ["customer_id", "customer_name", "city", "age"])

print("All Customers")
customers.show()

print("Customers from Chennai")
customers.filter(customers.city == "Chennai").show()

print("Customers age > 25")
customers.filter(customers.age > 25).show()

print("Selected Columns")
customers.select("customer_name", "city").show()

print("City-wise Count")
customers.groupBy("city").count().show()
