# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading from Silver

# COMMAND ----------

silver = spark.read.format("delta")\
    .option("header","true")\
    .load("abfss://silver@stdataproject.dfs.core.windows.net/ecommerce_orders")

# COMMAND ----------

silver.limit(20).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Fact Tables in Gold

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

# COMMAND ----------

fact_order_data.limit(20).display()

# COMMAND ----------

fact_order_data.write\
    .format("delta")\
    .mode("overwrite")\
    .save("abfss://gold@stdataproject.dfs.core.windows.net/fact_order_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ecommerce_catalog.gold.fact_data
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@stdataproject.dfs.core.windows.net/fact_order_data'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ecommerce_catalog.gold.fact_data

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

# COMMAND ----------

fact_orders_numerical_data.write\
    .format("delta")\
    .mode("overwrite")\
    .save("abfss://gold@stdataproject.dfs.core.windows.net/fact_orders_numerical_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ecommerce_catalog.gold.fact_numerical
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@stdataproject.dfs.core.windows.net/fact_orders_numerical_data'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ecommerce_catalog.gold.fact_numerical

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Dim Table for Customers

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, concat_ws

orders_silver = spark.table("ecommerce_catalog.silver.orders_silver")

dim_customer = orders_silver.select(
    "CustomerID",
    "customer_name",
    "City",
    "State",
    "MembershipTier"
)

dim_customer = dim_customer.withColumn("customer_key", monotonically_increasing_id())

dim_customer.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable("ecommerce_catalog.gold.dim_customer")

print("dim_customer:", dim_customer.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecommerce_catalog.gold.dim_customer

# COMMAND ----------

dim_product = orders_silver.select(
    "ProductCategory",
    "ProductSubCategory",
    "UnitPrice"
).dropDuplicates(["ProductCategory", "ProductSubCategory"])

dim_product = dim_product.withColumn("product_key", monotonically_increasing_id())

dim_product.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable("ecommerce_catalog.gold.dim_product")
print("dim_product:", dim_product.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecommerce_catalog.gold.dim_product

# COMMAND ----------

from pyspark.sql.functions import col

dim_date = orders_silver.select(
    col("OrderDate").cast("date").alias("date"),
    col("Order_Year").alias("year"),
    col("Order_Month").alias("month"),
    col("Order_Quarter").alias("quarter"),
    col("OrderSeason").alias("season"),
    col("OrderDayOfWeek").alias("day_of_week"),
    col("Weekend").alias("is_weekend")
).dropDuplicates(["date"]).filter("date is not null")

dim_date.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable("ecommerce_catalog.gold.dim_date")
print("dim_date:", dim_date.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecommerce_catalog.gold.dim_date

# COMMAND ----------

dim_payment = orders_silver.select(
    "PaymentMethod"
).dropDuplicates(["PaymentMethod"])

dim_payment = dim_payment.withColumn("payment_key", monotonically_increasing_id())

dim_payment.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable("ecommerce_catalog.gold.dim_payment")
print("dim_payment:", dim_payment.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecommerce_catalog.gold.dim_payment

# COMMAND ----------

dim_device = orders_silver.select(
    "DeviceType"
).dropDuplicates(["DeviceType"])

dim_device = dim_device.withColumn("device_key", monotonically_increasing_id())

dim_device.write.format("delta").mode("overwrite").saveAsTable("ecommerce_catalog.gold.dim_device")
print("dim_device:", dim_device.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecommerce_catalog.gold.dim_device

# COMMAND ----------

fact = spark.table("ecommerce_catalog.gold.fact_data")
fact.printSchema()

# COMMAND ----------

