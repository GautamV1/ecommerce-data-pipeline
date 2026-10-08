# Databricks notebook source
orders_source = spark.table("neon_ecommerce.public.orders")
order_items_source = spark.table("neon_ecommerce.public.order_items")

print("Neon orders:", orders_source.count())
print("Neon order items:", order_items_source.count())

# COMMAND ----------

from pyspark.sql.functions import col, max as spark_max, current_timestamp, trim, initcap
from delta.tables import DeltaTable


# --------------------------------
# 1. Find current watermark
# --------------------------------

last_order_id = (
    spark.table("workspace.default.silver_orders")
    .agg(spark_max("order_id"))
    .first()[0]
)

last_order_item_id = (
    spark.table("workspace.default.silver_order_items")
    .agg(spark_max("order_item_id"))
    .first()[0]
)

last_order_id = last_order_id or 0
last_order_item_id = last_order_item_id or 0

print("Last processed order:", last_order_id)
print("Last processed item:", last_order_item_id)


# --------------------------------
# 2. Read ONLY new Neon rows
# --------------------------------

new_orders = (
    spark.table("neon_ecommerce.public.orders")
    .filter(col("order_id") > last_order_id)
)

new_items = (
    spark.table("neon_ecommerce.public.order_items")
    .filter(col("order_item_id") > last_order_item_id)
)

print("New orders:", new_orders.count())
print("New order items:", new_items.count())

# COMMAND ----------

# Save raw incremental records into Bronze

if new_orders.count() > 0:
    (
        new_orders
        .withColumn("_ingested_at", current_timestamp())
        .write
        .format("delta")
        .mode("append")
        .saveAsTable("workspace.default.bronze_orders")
    )

if new_items.count() > 0:
    (
        new_items
        .withColumn("_ingested_at", current_timestamp())
        .write
        .format("delta")
        .mode("append")
        .saveAsTable("workspace.default.bronze_order_items")
    )

# COMMAND ----------

new_orders_silver = (
    new_orders
    .dropDuplicates(["order_id"])
    .dropna(subset=["order_id", "customer_id", "order_date"])
    .withColumn(
        "order_status",
        initcap(trim(col("order_status")))
    )
)

new_items_silver = (
    new_items
    .dropDuplicates(["order_item_id"])
    .dropna(subset=["order_item_id", "order_id", "product_id"])
    .filter(col("quantity") > 0)
    .filter(col("unit_price") >= 0)
)

# COMMAND ----------

if new_orders_silver.count() > 0:

    silver_orders = DeltaTable.forName(
        spark,
        "workspace.default.silver_orders"
    )

    (
        silver_orders.alias("target")
        .merge(
            new_orders_silver.alias("source"),
            "target.order_id = source.order_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


if new_items_silver.count() > 0:

    silver_items = DeltaTable.forName(
        spark,
        "workspace.default.silver_order_items"
    )

    (
        silver_items.alias("target")
        .merge(
            new_items_silver.alias("source"),
            "target.order_item_id = source.order_item_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


print("✅ Neon incremental ingestion completed.")

# COMMAND ----------

print("Silver orders:", spark.table("workspace.default.silver_orders").count())
print("Silver items:", spark.table("workspace.default.silver_order_items").count())