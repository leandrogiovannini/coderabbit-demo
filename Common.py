# Databricks notebook source

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Define shared schema
sales_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("region", StringType(), True)
])

@udf(returnType=DoubleType())
def calculate_revenue(quantity, price):
    return quantity * price