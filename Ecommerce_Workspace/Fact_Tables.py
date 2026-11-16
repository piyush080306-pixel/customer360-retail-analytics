from pyspark.sql.functions import *
from pyspark.sql.types import *
silver = spark.read.format("delta")\
    .option("header","true")\
    .load("abfss://silver@stdataproject.dfs.core.windows.net/ecommerce_orders")
silver.limit(20).display()
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
fact_order_data.write\
    .format("delta")\
    .mode("overwrite")\
    .save("abfss://gold@stdataproject.dfs.core.windows.net/fact_order_data")
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
fact_orders_numerical_data.write\
    .format("delta")\
    .mode("overwrite")\
    .save("abfss://gold@stdataproject.dfs.core.windows.net/fact_orders_numerical_data")
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
dim_payment = orders_silver.select(
    "PaymentMethod"
).dropDuplicates(["PaymentMethod"])
dim_payment = dim_payment.withColumn("payment_key", monotonically_increasing_id())
dim_payment.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable("ecommerce_catalog.gold.dim_payment")
print("dim_payment:", dim_payment.count())
dim_device = orders_silver.select(
    "DeviceType"
).dropDuplicates(["DeviceType"])
dim_device = dim_device.withColumn("device_key", monotonically_increasing_id())
dim_device.write.format("delta").mode("overwrite").saveAsTable("ecommerce_catalog.gold.dim_device")
print("dim_device:", dim_device.count())
fact = spark.table("ecommerce_catalog.gold.fact_data")
fact.printSchema()
