# Databricks notebook source
customers = spark.table("workspace.default.silver_customers")
products = spark.table("workspace.default.silver_products")
orders = spark.table("workspace.default.silver_orders")
order_items = spark.table("workspace.default.silver_order_items")

# COMMAND ----------

from pyspark.sql.functions import col

errors = []

# 1. Null primary keys
if customers.filter(col("customer_id").isNull()).count() > 0:
    errors.append("customers contains null customer_id")

if products.filter(col("product_id").isNull()).count() > 0:
    errors.append("products contains null product_id")

if orders.filter(col("order_id").isNull()).count() > 0:
    errors.append("orders contains null order_id")

if order_items.filter(col("order_item_id").isNull()).count() > 0:
    errors.append("order_items contains null order_item_id")


# 2. Duplicate IDs
if customers.groupBy("customer_id").count().filter(col("count") > 1).count() > 0:
    errors.append("Duplicate customer_id found")

if orders.groupBy("order_id").count().filter(col("count") > 1).count() > 0:
    errors.append("Duplicate order_id found")


# 3. Invalid quantities
if order_items.filter(col("quantity") <= 0).count() > 0:
    errors.append("Invalid quantity found")


# 4. Invalid prices
if order_items.filter(col("unit_price") < 0).count() > 0:
    errors.append("Negative unit_price found")


# 5. Invalid order status
valid_status = ["Completed", "Pending", "Cancelled"]

if orders.filter(~col("order_status").isin(valid_status)).count() > 0:
    errors.append("Invalid order_status found")

# COMMAND ----------

if errors:
    print("DATA QUALITY CHECK FAILED")

    for error in errors:
        print("-", error)

    raise Exception("Pipeline stopped because data quality checks failed.")

else:
    print("✅ All data quality checks passed.")