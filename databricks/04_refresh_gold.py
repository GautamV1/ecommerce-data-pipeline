# Databricks notebook source
from pyspark.sql.functions import col, round, sum, countDistinct

customers = spark.table("workspace.default.silver_customers")
products = spark.table("workspace.default.silver_products")
orders = spark.table("workspace.default.silver_orders")
order_items = spark.table("workspace.default.silver_order_items")

# COMMAND ----------

gold_sales_df = (
    order_items
    .join(
        orders,
        order_items.order_id == orders.order_id,
        "inner"
    )
    .join(
        customers,
        orders.customer_id == customers.customer_id,
        "inner"
    )
    .join(
        products,
        order_items.product_id == products.product_id,
        "inner"
    )
    .select(
        orders.order_id,
        orders.order_date,
        orders.order_status,

        customers.customer_id,
        customers.customer_name,
        customers.city,
        customers.province,

        products.product_id,
        products.product_name,
        products.category,

        order_items.quantity,
        order_items.unit_price
    )
    .withColumn(
        "line_revenue",
        round(col("quantity") * col("unit_price"), 2)
    )
)

display(gold_sales_df)

# COMMAND ----------

gold_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_sales")

# COMMAND ----------

completed_sales = gold_sales_df.filter(
    col("order_status") == "Completed"
)

gold_product_sales = (
    completed_sales
    .groupBy(
        "product_id",
        "product_name",
        "category"
    )
    .agg(
        sum("quantity").alias("units_sold"),
        round(sum("line_revenue"), 2).alias("total_revenue")
    )
)

gold_province_sales = (
    completed_sales
    .groupBy("province")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        round(sum("line_revenue"), 2).alias("total_revenue")
    )
)

gold_daily_sales = (
    completed_sales
    .groupBy("order_date")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        round(sum("line_revenue"), 2).alias("total_revenue")
    )
    .orderBy("order_date")
)

# COMMAND ----------

gold_product_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_product_sales")

gold_province_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_province_sales")

gold_daily_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_daily_sales")

print("Gold layer refreshed successfully.")

# COMMAND ----------

print(
    "Gold rows:",
    spark.table("workspace.default.gold_sales").count()
)