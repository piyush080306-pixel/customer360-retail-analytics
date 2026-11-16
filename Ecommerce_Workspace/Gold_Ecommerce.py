# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

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
# MAGIC ### Layer 3: Gold - Business Aggregations
# COMMAND ----------

silver = spark.read.format("delta") \
    .load(silver_path)

silver.display()

# COMMAND ----------

gold_orders = silver.groupBy("CustomerID", "customer_name", "City") \
    .agg(count("OrderID").alias("Total_Orders")) \
    .orderBy(desc("Total_Orders"))

gold_orders.display()

# COMMAND ----------

gold_tier = silver.groupBy("MembershipTier") \
    .agg(countDistinct("CustomerID").alias("Total_Customers"))

gold_tier.display()

# COMMAND ----------

gold_revenue = silver.groupBy("City") \
    .agg(sum("FinalAmount").alias("Total_Revenue"))

gold_revenue.display()

# COMMAND ----------

gold_returns = silver.groupBy("Returned") \
    .agg(count("OrderID").alias("Total_Orders"))

gold_returns.display()

# COMMAND ----------

gold_customer_segments = silver.groupBy("CustomerID", "customer_name", "MembershipTier") \
    .agg(
        round(sum("FinalAmount"), 2).alias("Lifetime_Value"),
        count("OrderID").alias("Total_Orders")
    ).withColumn(
        "Customer_Segment",
        when(col("Lifetime_Value") >= 10000, "Platinum")
        .when(col("Lifetime_Value") >= 5000, "Gold")
        .when(col("Lifetime_Value") >= 1000, "Silver")
        .otherwise("Bronze")
    ).orderBy(desc("Lifetime_Value"))

gold_customer_segments.display()

# COMMAND ----------

gold_season_category = silver.groupBy("OrderSeason", "ProductCategory") \
    .agg(
        count("OrderID").alias("Total_Orders"),
        round(sum("FinalAmount"), 2).alias("Total_Revenue")
    ).orderBy("OrderSeason", desc("Total_Revenue"))

gold_season_category.display()

# COMMAND ----------

gold_product_category = silver.groupBy("ProductCategory") \
    .agg(
        count("OrderID").alias("Total_Orders"),
        round(sum("FinalAmount"), 2).alias("Total_Revenue")
    ).orderBy("ProductCategory", desc("Total_Revenue"))

gold_product_category.display()

# COMMAND ----------

gold_payment_method = silver.groupBy("PaymentMethod") \
    .agg(
        count("OrderID").alias("Total_Orders"),
        round(sum("FinalAmount"), 2).alias("Total_Revenue"),
        round(avg("FinalAmount"), 2).alias("Avg_Order_Value")
    ).orderBy(desc("Total_Revenue"))

gold_payment_method.display()

# COMMAND ----------

gold_device_type = silver.groupBy("DeviceType") \
    .agg(
        count("OrderID").alias("Total_Orders"),
        round(sum("FinalAmount"), 2).alias("Total_Revenue"),
        round(avg("FinalAmount"), 2).alias("Avg_Order_Value")
    ).orderBy(desc("Total_Revenue"))

gold_device_type.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Write Gold Tables to Delta & Unity Catalog
# COMMAND ----------

gold_tables = {
    "gold_orders": gold_orders,
    "gold_tier": gold_tier,
    "gold_revenue": gold_revenue,
    "gold_returns": gold_returns,
    "gold_customer_segments": gold_customer_segments,
    "gold_season_category": gold_season_category,
    "gold_product_category": gold_product_category,
    "gold_payment_method": gold_payment_method,
    "gold_device_type": gold_device_type,
}

for table_name, df in gold_tables.items():
    path = f"{gold_base}/{table_name}"
    df.write.format("delta").mode("overwrite").save(path)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS ecommerce_catalog.gold.{table_name}
        USING DELTA
        LOCATION '{path}'
    """)

# COMMAND ----------
# MAGIC %sql
# MAGIC USE ecommerce_catalog.gold

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT * FROM gold_customer_segments
