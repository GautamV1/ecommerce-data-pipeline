# Databricks notebook source
bronze_path = "/Volumes/workspace/default/ecommerce_bronze/"

customers_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(bronze_path + "customers.csv")
)

display(customers_df)

# COMMAND ----------

customers_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, trim, lower, initcap

# COMMAND ----------

customers_silver_df = (
    customers_df

    # Remove duplicate customers
    .dropDuplicates(["customer_id"])

    # Remove records missing important fields
    .dropna(subset=["customer_id", "customer_name", "email"])

    # Clean text columns
    .withColumn("customer_name", initcap(trim(col("customer_name"))))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("city", initcap(trim(col("city"))))
    .withColumn("province", initcap(trim(col("province"))))
)

display(customers_silver_df)

# COMMAND ----------

print("Bronze rows:", customers_df.count())
print("Silver rows:", customers_silver_df.count())

# COMMAND ----------

customers_silver_df.printSchema()

# COMMAND ----------

products_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(bronze_path + "products.csv")
)

orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(bronze_path + "orders.csv")
)

order_items_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(bronze_path + "order_items.csv")
)

print("Products:", products_df.count())
print("Orders:", orders_df.count())
print("Order Items:", order_items_df.count())

# COMMAND ----------

from pyspark.sql.functions import col, trim, initcap

# COMMAND ----------

products_silver_df = (
    products_df
    .dropDuplicates(["product_id"])
    .dropna(subset=["product_id", "product_name", "price"])
    .filter(col("price") >= 0)
    .withColumn("product_name", initcap(trim(col("product_name"))))
    .withColumn("category", initcap(trim(col("category"))))
)

orders_silver_df = (
    orders_df
    .dropDuplicates(["order_id"])
    .dropna(subset=["order_id", "customer_id", "order_date"])
    .withColumn("order_status", initcap(trim(col("order_status"))))
)

order_items_silver_df = (
    order_items_df
    .dropDuplicates(["order_item_id"])
    .dropna(subset=["order_item_id", "order_id", "product_id"])
    .filter(col("quantity") > 0)
    .filter(col("unit_price") >= 0)
)

display(products_silver_df)
display(orders_silver_df)
display(order_items_silver_df)

# COMMAND ----------

customers_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_customers")

products_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_products")

orders_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_orders")

order_items_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_order_items")

print("Silver Delta tables created successfully.")

# COMMAND ----------

display(spark.sql("SELECT * FROM workspace.default.silver_orders"))