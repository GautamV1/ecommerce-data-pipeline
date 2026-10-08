import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

connection = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

tables = [
    "customers",
    "products",
    "orders",
    "order_items"
]

for table in tables:
    query = f"SELECT * FROM {table}"

    df = pd.read_sql(query, connection)

    output_path = f"data/bronze/{table}.csv"

    df.to_csv(output_path, index=False)

    print(f"{table}: {len(df)} rows extracted")

connection.close()

print("Bronze ingestion completed successfully.")