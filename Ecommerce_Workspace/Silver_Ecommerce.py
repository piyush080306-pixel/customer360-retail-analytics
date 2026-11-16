# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------
# MAGIC %md
# MAGIC ### Configuration
# COMMAND ----------

dbutils.widgets.text("storage_account", "stdataproject")
dbutils.widgets.text("sql_server", "<sql-server>")
dbutils.widgets.text("sql_database", "<database>")

storage_account = dbutils.widgets.get("storage_account")
sql_server = dbutils.widgets.get("sql_server")
sql_database = dbutils.widgets.get("sql_database")

bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/ecommerce/ecommerce_data"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/ecommerce_orders"

# COMMAND ----------
# MAGIC %md
# MAGIC ### Layer 2: Data Engineering (Silver) - Read Bronze & Azure SQL
# COMMAND ----------

orders_df = spark.read.format("delta") \
    .load(bronze_path)

orders_df.limit(5).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Loading Customer Master from Azure SQL Database
# COMMAND ----------

sql_username = dbutils.secrets.get(scope="azure-sql", key="username")
sql_password = dbutils.secrets.get(scope="azure-sql", key="password")

jdbc_url = (
    f"jdbc:sqlserver://{sql_server}:1433;"
    f"database={sql_database};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "loginTimeout=30;"
)

customers_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dbo.customers") \
    .option("user", sql_username) \
    .option("password", sql_password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

customers_df.limit(5).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Join Ecommerce Data with Customer Master
# COMMAND ----------

# Join on CustomerID (primary business key).
# FullName and City are validated separately as data quality checks,
# not used as join conditions (avoids false mismatches on name formatting).
merged_data = orders_df.join(
    customers_df,
    orders_df["CustomerID"] == customers_df["customer_id"],
    how="left"
).drop(customers_df["customer_id"])

merged_data.limit(5).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Data Transformation & Enrichment
# COMMAND ----------

silver_merged = merged_data \
    .withColumn("IsDiscounted", when(col("DiscountPercent") > 0, "Yes").otherwise("No")) \
    .withColumn("Weekend", when(col("OrderDayOfWeek").isin("Saturday", "Sunday"), "Weekend").otherwise("Weekday")) \
    .withColumn("Value_Orders",
        when(col("TotalAmount") > 1000, "High")
        .when((col("TotalAmount") < 1000) & (col("TotalAmount") > 500), "Medium")
        .otherwise("Low")) \
    .withColumn("Order_Year", year(col("OrderDate"))) \
    .withColumn("Order_Month", month(col("OrderDate"))) \
    .withColumn("Order_Quarter", quarter(col("OrderDate")))

# Drop duplicate city column from customers table (keep orders City)
silver = silver_merged.drop(customers_df["city"])

# COMMAND ----------
# MAGIC %md
# MAGIC ### Data Quality & Validation
# COMMAND ----------

silver.cache()

total_rows = silver.count()
matched_customers = silver.filter(col("customer_name").isNotNull()).count()
unmatched_orders = total_rows - matched_customers
null_order_ids = silver.filter(col("OrderID").isNull()).count()
invalid_quantity = silver.filter(col("Quantity") <= 0).count()
negative_final_amount = silver.filter(col("FinalAmount") < 0).count()

print(f"Silver rows total: {total_rows}")
print(f"Matched customers: {matched_customers}")
print(f"Unmatched orders: {unmatched_orders}")
print(f"Orders with null OrderID: {null_order_ids}")
print(f"Orders with invalid Quantity (<=0): {invalid_quantity}")
print(f"Orders with negative FinalAmount: {negative_final_amount}")

silver.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Write to Silver (Delta)
# COMMAND ----------

silver.write.mode("overwrite") \
    .format("delta") \
    .save(silver_path)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Register in Unity Catalog
# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS ecommerce_catalog.silver.orders_silver
    USING DELTA
    LOCATION 'abfss://silver@{storage_account}.dfs.core.windows.net/ecommerce_orders'
""")

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT * FROM ecommerce_catalog.silver.orders_silver
