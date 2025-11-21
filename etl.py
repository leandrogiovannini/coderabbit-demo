# Databricks notebook source

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Production PySpark ETL - Delta Writes") \
    .getOrCreate()

# COMMAND ----------

schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("region", StringType(), True)
])

data = [
    ("P001", "Electronics", 10, 99.99, "North America"),
    ("P002", "Clothing", 20, 49.99, "Europe"),
    ("P003", "Electronics", 5, 199.99, "Asia"),
    ("P001", "Electronics", 15, 99.99, "Europe"),
    ("P004", "Books", 30, 9.99, "North America"),
    ("P002", "Clothing", 10, 49.99, "Asia"),
    ("P005", "Electronics", 8, 149.99, "North America")
]

# Load data into DataFrame
sales_df = spark.createDataFrame(data, schema)

# COMMAND ----------

# Calculate total revenue per transaction
sales_with_revenue = sales_df.withColumn(
    "total_revenue", col("quantity") * col("price")
)

# Filter for Electronics category (this lineage will be reused)
electronics_df = sales_with_revenue.filter(col("category") == "Electronics")

# COMMAND ----------

(
    electronics_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save("/tmp/delta/electronics_raw")
)