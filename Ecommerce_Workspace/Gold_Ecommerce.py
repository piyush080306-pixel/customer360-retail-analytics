from pyspark.sql.functions import *
from pyspark.sql.types import *
silver = spark.read.format("delta")\
    .option("header","true")\
    .load("abfss://silver@stdataproject.dfs.core.windows.net/ecommerce_orders")
silver.display()
gold_orders = silver.groupBy("CustomerID","customer_name","City").agg(count("OrderID").alias("Total_Orders")).orderBy(desc("Total_Orders"))
gold_orders.display()
gold_tier = silver.groupBy("MembershipTier")\
    .agg(countDistinct("CustomerID").alias("Total_Customers"))    
gold_tier.display()
gold_revenue = silver.groupBy("City") \
    .agg(sum("FinalAmount").alias("Total_Revenue"))
gold_revenue.display()
gold_returns = silver.groupBy("Returned") \
    .agg(count("OrderID").alias("Total_Orders"))
gold_returns.display()
gold_customer_segments = silver.groupBy("CustomerID", "customer_name", "MembershipTier") \
    .agg(
        round(sum("TotalAmount"), 2).alias("Lifetime_Value"),
        count("OrderID").alias("Total_Orders")
    ).withColumn(
        "Customer_Segment",
        when(col("Lifetime_Value") >= 10000, "Platinum")
        .when(col("Lifetime_Value") >= 5000, "Gold")
        .when(col("Lifetime_Value") >= 1000, "Silver")
        .otherwise("Bronze")
    ).orderBy(desc("Lifetime_Value"))
gold_customer_segments.display()
gold_season_category = silver.groupBy("OrderSeason", "ProductCategory") \
    .agg(
        count("OrderID").alias("Total_Orders"),
        round(sum("FinalAmount"), 2).alias("Total_Revenue")
    ).orderBy("OrderSeason", desc("Total_Revenue"))
gold_season_category.display()
gold_product_category = silver.groupBy("ProductCategory") \
    .agg(
        count("OrderID").alias("Total_Orders"),
        round(sum("FinalAmount"), 2).alias("Total_Revenue")
    ).orderBy("ProductCategory", desc("Total_Revenue"))
gold_product_category.display()
gold_payment_method = silver.groupBy("PaymentMethod") \
    .agg(
        count("OrderID").alias("Total_Orders"),
        round(sum("FinalAmount"), 2).alias("Total_Revenue"),
        round(avg("FinalAmount"), 2).alias("Avg_Order_Value")
    ).orderBy(desc("Total_Revenue"))
gold_payment_method.display()
gold_device_type = silver.groupBy("DeviceType") \
    .agg(
        count("OrderID").alias("Total_Orders"),
        round(sum("FinalAmount"), 2).alias("Total_Revenue"),
        round(avg("FinalAmount"), 2).alias("Avg_Order_Value")
    ).orderBy(desc("Total_Revenue"))
gold_device_type.display()
gold_orders.write\
    .format("delta")\
    .mode("overwrite")\
    .save("abfss://gold@stdataproject.dfs.core.windows.net/gold_orders")
gold_tier.write\
    .format("delta")\
    .mode("overwrite")\
    .save("abfss://gold@stdataproject.dfs.core.windows.net/gold_tier")
gold_revenue.write\
    .format("delta")\
    .mode("overwrite")\
    .save("abfss://gold@stdataproject.dfs.core.windows.net/gold_revenue")
gold_returns.write\
    .format("delta")\
    .mode("overwrite")\
    .save("abfss://gold@stdataproject.dfs.core.windows.net/gold_returns")
gold_customer_segments.write\
    .format("delta")\
    .mode("overwrite")\
    .save("abfss://gold@stdataproject.dfs.core.windows.net/gold_customer_segments")
gold_season_category.write\
    .format("delta")\
    .mode("overwrite")\
    .save("abfss://gold@stdataproject.dfs.core.windows.net/gold_season_category")
gold_product_category.write\
    .format("delta")\
    .mode("overwrite")\
    .save("abfss://gold@stdataproject.dfs.core.windows.net/gold_product_category")
gold_payment_method.write\
    .format("delta")\
    .mode("overwrite")\
    .save("abfss://gold@stdataproject.dfs.core.windows.net/gold_payment_method")
gold_device_type.write\
    .format("delta")\
    .mode("overwrite")\
    .save("abfss://gold@stdataproject.dfs.core.windows.net/gold_device_type")
