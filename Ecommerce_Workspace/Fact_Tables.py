# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------
# MAGIC %md
# MAGIC ### Configuration
# COMMAND ----------

dbutils.widgets.text("storage_account", "stdataproject")

storage_account = dbutils.widgets.get("storage_account")

silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/ecommerce_orders"
gold_base = f"abfss://gold@{storage_account}.dfs.core.windows.net"

# COMMAND ----------
# MAGIC %md
# MAGIC ### Layer 3: Gold - Fact & Dimension Tables
# COMMAND ----------

silver = spark.read.format("delta") \
    .load(silver_path)

silver.limit(20).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Fact Tables
# COMMAND ----------

fact_order_data = silver.select(
    "OrderID",
    "CustomerID",
    "customer_name",
    "OrderDate",
    "City",
    "State",
    "ProductCategory",
    "ProductSubCategory",
    "PaymentMethod",
    "DeviceType",
    "MembershipTier",
    "Returned",
    "OrderSeason",
    "OrderDayOfWeek",
    "IsDiscounted",
    "Weekend",
    "Value_Orders"
)

fact_order_data.limit(20).display()

fact_order_data.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_base}/fact_order_data")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS ecommerce_catalog.gold.fact_data
    USING DELTA
    LOCATION '{gold_base}/fact_order_data'
""")

spark.sql(f"SELECT * FROM ecommerce_catalog.gold.fact_data")

# COMMAND ----------

fact_orders_numerical_data = silver.select(
    "OrderID",
    "CustomerID",
    "Quantity",
    "UnitPrice",
    "TotalAmount",
    "DiscountAmount",
    "FinalAmount",
    "ShippingDays",
    "ReviewScore",
    "DiscountPercent",
    "Order_Year",
    "Order_Month",
    "Order_Quarter"
)

fact_orders_numerical_data.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_base}/fact_orders_numerical_data")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS ecommerce_catalog.gold.fact_numerical
    USING DELTA
    LOCATION '{gold_base}/fact_orders_numerical_data'
""")

spark.sql(f"SELECT * FROM ecommerce_catalog.gold.fact_numerical")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Dimension Tables
# COMMAND ----------

orders_silver = spark.table("ecommerce_catalog.silver.orders_silver")

# Deterministic surrogate key using row_number (stable across runs)
window_spec = Window.orderBy("CustomerID")

dim_customer = orders_silver.select(
    "CustomerID",
    "customer_name",
    "City",
    "State",
    "MembershipTier"
).dropDuplicates(["CustomerID"]) \
 .withColumn("customer_key", row_number().over(window_spec))

dim_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_catalog.gold.dim_customer")

print("dim_customer:", dim_customer.count())

spark.sql("SELECT * FROM ecommerce_catalog.gold.dim_customer")

# COMMAND ----------

dim_product = orders_silver.select(
    "ProductCategory",
    "ProductSubCategory"
).dropDuplicates(["ProductCategory", "ProductSubCategory"]) \
 .withColumn("product_key", row_number().over(Window.orderBy("ProductCategory", "ProductSubCategory")))

dim_product.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_catalog.gold.dim_product")

print("dim_product:", dim_product.count())

spark.sql("SELECT * FROM ecommerce_catalog.gold.dim_product")

# COMMAND ----------

dim_date = orders_silver.select(
    col("OrderDate").cast("date").alias("date"),
    col("Order_Year").alias("year"),
    col("Order_Month").alias("month"),
    col("Order_Quarter").alias("quarter"),
    col("OrderSeason").alias("season"),
    col("OrderDayOfWeek").alias("day_of_week"),
    col("Weekend").alias("is_weekend")
).dropDuplicates(["date"]).filter("date is not null") \
 .withColumn("date_key", row_number().over(Window.orderBy("date")))

dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_catalog.gold.dim_date")

print("dim_date:", dim_date.count())

spark.sql("SELECT * FROM ecommerce_catalog.gold.dim_date")

# COMMAND ----------

dim_payment = orders_silver.select(
    "PaymentMethod"
).dropDuplicates(["PaymentMethod"]) \
 .withColumn("payment_key", row_number().over(Window.orderBy("PaymentMethod")))

dim_payment.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_catalog.gold.dim_payment")

print("dim_payment:", dim_payment.count())

spark.sql("SELECT * FROM ecommerce_catalog.gold.dim_payment")

# COMMAND ----------

dim_device = orders_silver.select(
    "DeviceType"
).dropDuplicates(["DeviceType"]) \
 .withColumn("device_key", row_number().over(Window.orderBy("DeviceType")))

dim_device.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce_catalog.gold.dim_device")

print("dim_device:", dim_device.count())

spark.sql("SELECT * FROM ecommerce_catalog.gold.dim_device")
