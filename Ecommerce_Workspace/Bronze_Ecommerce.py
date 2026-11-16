# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------
# MAGIC %md
# MAGIC ### Configuration
# COMMAND ----------

dbutils.widgets.text("storage_account", "stdataproject")
dbutils.widgets.text("source_file", "Ecommerce_2000.csv")

storage_account = dbutils.widgets.get("storage_account")
source_file = dbutils.widgets.get("source_file")

bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/ecommerce/ecommerce_data"
source_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/ecommerce/{source_file}"

# COMMAND ----------
# MAGIC %md
# MAGIC ### Layer 1: Data Extraction & Raw Ingestion (Bronze)
# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .load(source_path)

df.display()

# COMMAND ----------

df = df.withColumn("FullName", concat(col("FirstName"), lit(" "), col("LastName")))
df.limit(10).display()

# COMMAND ----------

# Write raw data to Bronze as Delta (transactional, time-travel capable)
df.write.mode("overwrite") \
    .format("delta") \
    .save(bronze_path)

df.limit(5).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Basic Data Quality: Row Count
# COMMAND ----------

print(f"Bronze rows ingested: {df.count()}")
df.display()
