from pyspark.sql.functions import *
from pyspark.sql.types import *
df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","false")\
    .load("abfss://bronze@stdataproject.dfs.core.windows.net/ecommerce/Ecommerce_2000.csv")
df.display()
df = df.withColumn("FullName",concat(col("FirstName"),lit(" "),col("LastName")))
df.limit(10).display()
df.write.mode("overwrite")\
    .format("csv")\
    .option("header","true")\
    .save("abfss://bronze@stdataproject.dfs.core.windows.net/ecommerce/ecommerce_data")
df.limit(5).display()
df.display()
