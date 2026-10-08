import os
import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import psycopg2
from dotenv import load_dotenv


# -----------------------------
# Project paths
# -----------------------------

project_root = Path(__file__).resolve().parents[1]

load_dotenv(project_root / ".env")

state_folder = project_root / "state"
bronze_folder = project_root / "data" / "bronze" / "incremental"

state_folder.mkdir(exist_ok=True)
bronze_folder.mkdir(parents=True, exist_ok=True)

state_file = state_folder / "watermark.json"


# -----------------------------
# Read previous watermark
# -----------------------------

if state_file.exists():

    with open(state_file, "r") as file:
        watermark = json.load(file)

else:

    watermark = {
        "last_order_id": 1010,
        "last_order_item_id": 13
    }


print("Current watermark:")
print(watermark)


# -----------------------------
# PostgreSQL connection
# -----------------------------

connection = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)


# -----------------------------
# Extract ONLY new orders
# -----------------------------

orders_query = """
SELECT *
FROM orders
WHERE order_id > %s
ORDER BY order_id
"""

new_orders = pd.read_sql_query(
    orders_query,
    connection,
    params=[watermark["last_order_id"]]
)


# -----------------------------
# Extract ONLY new order items
# -----------------------------

items_query = """
SELECT *
FROM order_items
WHERE order_item_id > %s
ORDER BY order_item_id
"""

new_order_items = pd.read_sql_query(
    items_query,
    connection,
    params=[watermark["last_order_item_id"]]
)

connection.close()


# -----------------------------
# Save incremental files
# -----------------------------

run_time = datetime.now().strftime("%Y%m%d_%H%M%S")


if not new_orders.empty:

    orders_file = bronze_folder / f"orders_{run_time}.csv"

    new_orders.to_csv(
        orders_file,
        index=False
    )

    watermark["last_order_id"] = int(
        new_orders["order_id"].max()
    )


if not new_order_items.empty:

    items_file = bronze_folder / f"order_items_{run_time}.csv"

    new_order_items.to_csv(
        items_file,
        index=False
    )

    watermark["last_order_item_id"] = int(
        new_order_items["order_item_id"].max()
    )


# -----------------------------
# Update watermark
# -----------------------------

with open(state_file, "w") as file:
    json.dump(
        watermark,
        file,
        indent=4
    )


print()
print("New orders extracted:", len(new_orders))
print("New order items extracted:", len(new_order_items))

print()
print("Updated watermark:")
print(watermark)

print()
print("Incremental ingestion completed successfully.")
SS