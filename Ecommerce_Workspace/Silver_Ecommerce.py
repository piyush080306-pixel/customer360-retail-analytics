# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

orders_df = spark.read.format("csv")\
        .option("header","true")\
        .option("inferSchema","true")\
        .load("abfss://bronze@stdataproject.dfs.core.windows.net/ecommerce/ecommerce_data")

# COMMAND ----------

orders_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading Data from Azure SQL Database

# COMMAND ----------

# Configure these values before running the notebook:
# databricks secrets create-scope azure-sql
# databricks secrets put-secret azure-sql username
# databricks secrets put-secret azure-sql password
dbutils.widgets.text("sql_server", "<sql-server>")
dbutils.widgets.text("sql_database", "<database>")

sql_server = dbutils.widgets.get("sql_server")
sql_database = dbutils.widgets.get("sql_database")
sql_username = dbutils.secrets.get(scope="azure-sql", key="username")
sql_password = dbutils.secrets.get(scope="azure-sql", key="password")

jdbc_url = (
    f"jdbc:sqlserver://{sql_server}:1433;"
    f"database={sql_database};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "loginTimeout=30;"
)

# COMMAND ----------

customers_df = spark.read\
    .format("jdbc")\
    .option("url", jdbc_url)\
    .option("dbtable","dbo.customers")\
    .option("user", sql_username)\
    .option("password", sql_password)\
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
    .load()

# COMMAND ----------

customers_df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining Ecommerce Data and Customers Table

# COMMAND ----------

merged_data = orders_df.join(
    customers_df,
    (orders_df["CustomerID"] == customers_df["customer_id"]) &
    (orders_df["FullName"] == customers_df["customer_name"]) &
    (orders_df["City"] == customers_df["city"]),
    how='left'
).drop("customer_id","FullName")

# COMMAND ----------

merged_data.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Transformation

# COMMAND ----------

silver_merged = merged_data.withColumn("IsDiscounted",when(col("DiscountPercent")>0,"Yes").otherwise("No"))\
    .withColumn("Weekend",when(col("OrderDayOfWeek").isin("Saturday","Sunday"),"Weekend").otherwise("Weekday"))\
    .withColumn("Value_Orders", when(col("TotalAmount")>1000,"High").when((col("TotalAmount")<1000) & (col("TotalAmount")>500),"Medium").otherwise("Low"))\
    .withColumn("Order_Year",year(col("OrderDate")))\
    .withColumn("Order_Month",month(col("OrderDate")))\
    .withColumn("Order_Quarter",quarter(col("OrderDate")))

# COMMAND ----------

silver = silver_merged.drop(customers_df["city"])

# COMMAND ----------

silver.display()

# COMMAND ----------

if silver is not None: 
    print(f"Silver rows total: {silver.count()}") 
else:
    print("silver is None")

if silver is not None: 
    print(f"Matched customers: {silver.filter(col('customer_name').isNotNull()).count()}")
else:
    print("silver is None")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing to Silver**

# COMMAND ----------

silver.write.mode("overwrite")\
      .format("delta")\
      .save("abfss://silver@stdataproject.dfs.core.windows.net/ecommerce_orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ecommerce_catalog.silver.orders_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@stdataproject.dfs.core.windows.net/ecommerce_orders'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ecommerce_catalog.silver.orders_silver

# COMMAND ----------

