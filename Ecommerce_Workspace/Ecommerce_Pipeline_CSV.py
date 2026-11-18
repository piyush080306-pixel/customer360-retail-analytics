"""Build Bronze, Silver, Gold, fact, and dimension tables from the ecommerce CSV."""

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


CATALOG = os.environ.get("ECOMMERCE_CATALOG", "workspace")
SCHEMA = os.environ.get("ECOMMERCE_SCHEMA", "default")
VOLUME_INPUT_PATH = "/Volumes/workspace/default/ecommerce_files/Ecommerce_2000.csv"


def get_input_path() -> str:
    configured_path = os.environ.get("ECOMMERCE_CSV_PATH")
    if configured_path:
        return configured_path

    local_csv = Path(__file__).resolve().parent.parent / "Ecommerce_2000.csv"
    if local_csv.is_file():
        return str(local_csv)
    return VOLUME_INPUT_PATH


def save_table(df: DataFrame, table_name: str) -> None:
    (df.write.format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"{CATALOG}.{SCHEMA}.{table_name}"))


def validate_aggregate(
    actual: DataFrame,
    expected: DataFrame,
    columns: list[str],
    label: str,
) -> None:
    actual_rows = actual.select(*columns)
    expected_rows = expected.select(*columns)
    differences = (
        actual_rows.exceptAll(expected_rows).count()
        + expected_rows.exceptAll(actual_rows).count()
    )
    if differences:
        raise ValueError(
            f"Gold validation failed for {label}: {differences} differing rows"
        )


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    input_path = get_input_path()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

    raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .load(input_path)
    )

    required_columns = [
        "OrderID", "CustomerID", "FirstName", "LastName", "State", "City",
        "OrderDate", "OrderSeason", "OrderDayOfWeek", "ProductCategory",
        "ProductSubCategory", "UnitPrice", "Quantity", "TotalAmount",
        "DiscountPercent", "DiscountAmount", "FinalAmount", "PaymentMethod",
        "DeviceType", "MembershipTier", "ShippingDays", "Returned", "ReviewScore",
    ]
    missing_columns = sorted(set(required_columns) - set(raw.columns))
    if missing_columns:
        raise ValueError(f"CSV is missing required columns: {missing_columns}")

    bronze = raw.withColumn(
        "FullName", F.concat_ws(" ", "FirstName", "LastName")
    )
    save_table(bronze, "ecommerce_bronze")
    bronze_count = bronze.count()

    # Keep one copy of exact duplicate rows and reject conflicting order records.
    silver = bronze.dropDuplicates()
    conflicting_order_ids = (
        silver.groupBy("OrderID")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if conflicting_order_ids:
        raise ValueError(
            "Conflicting rows share "
            f"{conflicting_order_ids} OrderIDs after exact-row deduplication"
        )

    for column in [
        "UnitPrice", "TotalAmount", "DiscountPercent", "DiscountAmount",
        "FinalAmount",
    ]:
        silver = silver.withColumn(column, F.col(column).cast("double"))
    for column in ["Quantity", "ShippingDays", "ReviewScore"]:
        silver = silver.withColumn(column, F.col(column).cast("int"))

    silver = (
        silver.withColumn("OrderDate", F.to_date("OrderDate", "M/d/yyyy"))
        .withColumn("CustomerName", F.concat_ws(" ", "FirstName", "LastName"))
        .withColumn(
            "IsReturned", F.lower(F.trim("Returned")) == F.lit("yes")
        )
        .withColumn("IsWeekend", F.dayofweek("OrderDate").isin(1, 7))
        .withColumn("OrderYear", F.year("OrderDate"))
        .withColumn("OrderMonth", F.month("OrderDate"))
        .withColumn("OrderQuarter", F.quarter("OrderDate"))
    )

    invalid_rows = silver.filter(
        F.col("OrderID").isNull()
        | F.col("CustomerID").isNull()
        | F.col("OrderDate").isNull()
    ).count()
    if invalid_rows:
        raise ValueError(
            f"Silver quality check failed for {invalid_rows} invalid rows"
        )

    silver_count = silver.count()
    if silver_count > bronze_count:
        raise ValueError(
            f"Silver row count exceeded Bronze: {silver_count} > {bronze_count}"
        )
    save_table(silver, "ecommerce_silver")

    gold_tables = {
        "ecommerce_gold_customer_orders": silver.groupBy(
            "CustomerID", "CustomerName", "MembershipTier"
        ).agg(
            F.count("OrderID").alias("order_count"),
            F.sum("FinalAmount").alias("revenue"),
            F.avg("ReviewScore").alias("avg_review_score"),
        ),
        "ecommerce_gold_membership_tier": silver.groupBy(
            "MembershipTier"
        ).agg(
            F.count("OrderID").alias("order_count"),
            F.sum("FinalAmount").alias("revenue"),
            F.avg("FinalAmount").alias("avg_order_value"),
        ),
        "ecommerce_gold_city_revenue": silver.groupBy("State", "City").agg(
            F.count("OrderID").alias("order_count"),
            F.sum("FinalAmount").alias("revenue"),
        ),
        "ecommerce_gold_returns": silver.groupBy("IsReturned").agg(
            F.count("OrderID").alias("order_count"),
            F.sum("FinalAmount").alias("revenue"),
        ),
        "ecommerce_gold_customer_segments": silver.groupBy(
            "CustomerID", "CustomerName"
        ).agg(
            F.count("OrderID").alias("order_count"),
            F.sum("FinalAmount").alias("lifetime_spend"),
            F.avg("FinalAmount").alias("avg_order_value"),
        ).withColumn(
            "customer_segment",
            F.when(F.col("lifetime_spend") >= 1000, "High Value")
            .when(F.col("lifetime_spend") >= 300, "Mid Value")
            .otherwise("Standard"),
        ),
        "ecommerce_gold_season_category": silver.groupBy(
            "OrderSeason", "ProductCategory"
        ).agg(
            F.count("OrderID").alias("order_count"),
            F.sum("FinalAmount").alias("revenue"),
        ),
        "ecommerce_gold_product_category": silver.groupBy(
            "ProductCategory", "ProductSubCategory"
        ).agg(
            F.sum("Quantity").alias("units_sold"),
            F.count("OrderID").alias("order_count"),
            F.sum("FinalAmount").alias("revenue"),
        ),
        "ecommerce_gold_payment_method": silver.groupBy(
            "PaymentMethod"
        ).agg(
            F.count("OrderID").alias("order_count"),
            F.sum("FinalAmount").alias("revenue"),
        ),
        "ecommerce_gold_device_type": silver.groupBy("DeviceType").agg(
            F.count("OrderID").alias("order_count"),
            F.sum("FinalAmount").alias("revenue"),
        ),
    }
    for table_name, frame in gold_tables.items():
        save_table(frame, table_name)

    customer_window = Window.partitionBy("CustomerID").orderBy(
        F.col("OrderDate").desc(), F.col("OrderID").desc()
    )
    dim_customer = (
        silver.withColumn("_rank", F.row_number().over(customer_window))
        .filter(F.col("_rank") == 1)
        .select(
            "CustomerID", "CustomerName", "FirstName", "LastName",
            "State", "City", "MembershipTier",
        )
        .withColumn("customer_key", F.col("CustomerID"))
    )
    dim_product = (
        silver.select("ProductCategory", "ProductSubCategory")
        .distinct()
        .withColumn(
            "product_key",
            F.xxhash64("ProductCategory", "ProductSubCategory"),
        )
    )
    dim_date = (
        silver.select(
            "OrderDate", "OrderYear", "OrderMonth", "OrderQuarter",
            "OrderSeason", "OrderDayOfWeek", "IsWeekend",
        )
        .distinct()
        .withColumn(
            "date_key", F.date_format("OrderDate", "yyyyMMdd").cast("int")
        )
    )
    dim_payment_method = (
        silver.select("PaymentMethod")
        .distinct()
        .withColumn("payment_key", F.col("PaymentMethod"))
    )
    dim_device_type = (
        silver.select("DeviceType")
        .distinct()
        .withColumn("device_key", F.col("DeviceType"))
    )
    dimensions = {
        "ecommerce_dim_customer": dim_customer,
        "ecommerce_dim_product": dim_product,
        "ecommerce_dim_date": dim_date,
        "ecommerce_dim_payment_method": dim_payment_method,
        "ecommerce_dim_device_type": dim_device_type,
    }
    for table_name, frame in dimensions.items():
        save_table(frame, table_name)

    fact = (
        silver.join(
            dim_customer.select("CustomerID", "customer_key"), "CustomerID"
        )
        .join(
            dim_product.select(
                "ProductCategory", "ProductSubCategory", "product_key"
            ),
            ["ProductCategory", "ProductSubCategory"],
        )
        .join(dim_date.select("OrderDate", "date_key"), "OrderDate")
        .join(dim_payment_method, "PaymentMethod")
        .join(dim_device_type, "DeviceType")
    )
    fact_orders = fact.select(
        "OrderID", "customer_key", "product_key", "date_key", "payment_key",
        "device_key", "Quantity", "UnitPrice", "TotalAmount",
        "DiscountPercent", "DiscountAmount", "FinalAmount", "ShippingDays",
        "IsReturned", "ReviewScore",
    )
    fact_tables = {
        "ecommerce_fact_orders": fact_orders,
        "ecommerce_fact_orders_numeric": fact_orders.drop("IsReturned"),
        "ecommerce_fact_orders_categorical": fact.select(
            "OrderID", "customer_key", "product_key", "date_key",
            "payment_key", "device_key", "OrderSeason", "OrderDayOfWeek",
            "MembershipTier", "IsReturned",
        ),
    }
    for table_name, frame in fact_tables.items():
        save_table(frame, table_name)

    validate_aggregate(
        spark.table(f"{CATALOG}.{SCHEMA}.ecommerce_gold_membership_tier"),
        silver.groupBy("MembershipTier").agg(
            F.count("OrderID").alias("order_count"),
            F.sum("FinalAmount").alias("revenue"),
            F.avg("FinalAmount").alias("avg_order_value"),
        ),
        ["MembershipTier", "order_count", "revenue", "avg_order_value"],
        "membership tier",
    )
    validate_aggregate(
        spark.table(f"{CATALOG}.{SCHEMA}.ecommerce_gold_product_category"),
        silver.groupBy("ProductCategory", "ProductSubCategory").agg(
            F.sum("Quantity").alias("units_sold"),
            F.count("OrderID").alias("order_count"),
            F.sum("FinalAmount").alias("revenue"),
        ),
        [
            "ProductCategory", "ProductSubCategory", "units_sold",
            "order_count", "revenue",
        ],
        "product category",
    )
    validate_aggregate(
        spark.table(f"{CATALOG}.{SCHEMA}.ecommerce_gold_city_revenue"),
        silver.groupBy("State", "City").agg(
            F.count("OrderID").alias("order_count"),
            F.sum("FinalAmount").alias("revenue"),
        ),
        ["State", "City", "order_count", "revenue"],
        "city revenue",
    )

    expected_tables = [
        "ecommerce_bronze",
        "ecommerce_silver",
        *gold_tables.keys(),
        *dimensions.keys(),
        *fact_tables.keys(),
    ]
    if len(expected_tables) != 19:
        raise ValueError(f"Expected 19 output tables, found {len(expected_tables)}")
    row_counts = [
        (table_name, spark.table(f"{CATALOG}.{SCHEMA}.{table_name}").count())
        for table_name in expected_tables
    ]
    fact_count = row_counts[
        expected_tables.index("ecommerce_fact_orders")
    ][1]
    if fact_count != silver_count:
        raise ValueError(
            f"Fact/Silver row-count mismatch: fact={fact_count}, "
            f"silver={silver_count}"
        )

    print(
        f"Pipeline passed: Bronze={bronze_count}, Silver={silver_count}, "
        f"fact={fact_count}, exact duplicates removed="
        f"{bronze_count - silver_count}; 19 tables and 3 Gold aggregates "
        f"validated. Input: {input_path}"
    )
    for table_name, row_count in sorted(row_counts):
        print(f"{table_name}: {row_count}")


if __name__ == "__main__":
    main()
