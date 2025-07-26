import mysql.connector

cnx = mysql.connector.connect(
    user="root", password="Kollati@2004", host="localhost", database="easytradez"
)
cursor = cnx.cursor()

# Example: count rows in each table
tables = [
    "products",
    "orders",
    "users",
    "order_items",
    "inventory_items",
    "distribution_centers",
]
for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"Table `{table}` has {count} rows.")

# Example: preview first 5 rows from each table
for table in tables:
    cursor.execute(f"SELECT * FROM {table} LIMIT 5")
    rows = cursor.fetchall()
    print(f"\nSample rows from `{table}`:")
    for row in rows:
        print(row)

cursor.close()
cnx.close()
