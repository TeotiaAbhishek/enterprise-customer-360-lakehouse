from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("InspectBronze")
    .master("local[*]")
    .getOrCreate()
)

customers = spark.read.parquet(
    "data/bronze/crm_customers"
)

orders = spark.read.parquet(
    "data/bronze/orders"
)

print("\nCUSTOMERS SCHEMA")
customers.printSchema()

print("\nORDERS SCHEMA")
orders.printSchema()

print("\nCUSTOMERS COUNT")
print(customers.count())

print("\nORDERS COUNT")
print(orders.count())

customers.show(5, truncate=False)

spark.stop()