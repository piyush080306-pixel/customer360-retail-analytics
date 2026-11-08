# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","false")\
    .load("abfss://bronze@stdataproject.dfs.core.windows.net/ecommerce/Ecommerce_2000.csv")

df.display()

# COMMAND ----------

df = df.withColumn("FullName",concat(col("FirstName"),lit(" "),col("LastName")))
df.limit(10).display()

# COMMAND ----------

df.write.mode("overwrite")\
    .format("csv")\
    .option("header","true")\
    .save("abfss://bronze@stdataproject.dfs.core.windows.net/ecommerce/ecommerce_data")

df.limit(5).display()

# COMMAND ----------

df.display()

# COMMAND ----------

