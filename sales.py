# Databricks notebook source

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Sales Processing") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %run "./Common"

# COMMAND ----------

data = [
    ("P001", "Electronics", 10, 99.99, "North America"),
    ("P002", "Clothing", 20, 49.99, "Europe"),
    ("P003", "Electronics", 5, 199.99, "Asia"),
    ("P001", "Electronics", 15, 99.99, "Europe"),
    ("P004", "Books", 30, 9.99, "North America"),
    ("P002", "Clothing", 10, 49.99, "Asia"),
    ("P005", "Electronics", 8, 149.99, "North America")
]

# Load data into DataFrame using shared schema
sales_df = spark.createDataFrame(data, sales_schema)

# COMMAND ----------

# Use built-in multiplication
sales_with_revenue = sales_df.withColumn(
    "total_revenue", col("quantity") * col("price")
)

# COMMAND ----------

# Write main sales with revenue
(
    sales_with_revenue
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save("/tmp/delta/sales_with_revenue")
)