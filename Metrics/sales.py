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

# Compute revenue
sales_with_revenue = sales_df.withColumn(
    "total_revenue", calculate_revenue(col("quantity"), col("price"))
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

# COMMAND ----------

# Filter for high-value sales
high_value_df = sales_with_revenue.filter(col("total_revenue") > 1000)

# Get high values
high_value_list = high_value_df.collect()

# Sum high values
total_high_value = sum(row["total_revenue"] for row in high_value_list)

# Create summary DF
summary_data = [("High Value Total", total_high_value)]
summary_df = spark.createDataFrame(summary_data, ["type", "amount"])

# COMMAND ----------

# Write summary
(
    summary_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save("/tmp/delta/sales_summary")
)