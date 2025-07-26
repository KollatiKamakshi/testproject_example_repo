import mysql.connector
import pandas as pd

df = pd.read_csv('dataset/products.csv')

df = df.where(pd.notnull(df), None)

cnx = mysql.connector.connect(
    user='root',
    password='Kollati@2004',
    host='localhost',
    database='easytradez'
)
cursor = cnx.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS products (
    id INT PRIMARY KEY,
    cost FLOAT,
    category VARCHAR(255),
    name VARCHAR(255),
    brand VARCHAR(255),
    retail_price FLOAT,
    department VARCHAR(255),
    sku VARCHAR(255),
    distribution_center_id INT
)
""")

for index, row in df.iterrows():
    cursor.execute("""
        INSERT INTO products (id, cost, category, name, brand, retail_price, department, sku, distribution_center_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        int(row['id']) if row['id'] else None,
        float(row['cost']) if row['cost'] else None,
        row['category'],
        row['name'],
        row['brand'],
        float(row['retail_price']) if row['retail_price'] else None,
        row['department'],
        row['sku'],
        int(row['distribution_center_id']) if row['distribution_center_id'] else None
    ))


# Load orders.csv
df_orders = pd.read_csv('dataset/orders.csv')
df_orders = df_orders.where(pd.notnull(df_orders), None)

# Create orders table
cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (
    id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT,
    user_id INT,
    status VARCHAR(50),
    gender CHAR(1),
    created_at DATETIME,
    returned_at DATETIME,
    shipped_at DATETIME,
    delivered_at DATETIME,
    num_of_item INT
)
""")

for _, row in df_orders.iterrows():
    cursor.execute("""
        INSERT INTO orders (order_id, user_id, status, gender, created_at, returned_at, shipped_at, delivered_at, num_of_item)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        int(row['order_id']) if row['order_id'] else None,
        int(row['user_id']) if row['user_id'] else None,
        row['status'],
        row['gender'],
        row['created_at'],
        row['returned_at'],
        row['shipped_at'],
        row['delivered_at'],
        int(row['num_of_item']) if row['num_of_item'] else None
    ))


df_users = pd.read_csv('dataset/users.csv')
df_users = df_users.where(pd.notnull(df_users), None)

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    age INT,
    gender CHAR(1),
    state VARCHAR(100),
    street_address VARCHAR(255),
    postal_code VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT,
    traffic_source VARCHAR(50),
    created_at DATETIME
)
""")

for _, row in df_users.iterrows():
    cursor.execute("""
        INSERT INTO users (id, first_name, last_name, email, age, gender, state, street_address, postal_code, city, country, latitude, longitude, traffic_source, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        int(row['id']),
        row['first_name'],
        row['last_name'],
        row['email'],
        int(row['age']) if row['age'] else None,
        row['gender'],
        row['state'],
        row['street_address'],
        row['postal_code'],
        row['city'],
        row['country'],
        float(row['latitude']) if row['latitude'] else None,
        float(row['longitude']) if row['longitude'] else None,
        row['traffic_source'],
        row['created_at']
    ))


df_order_items = pd.read_csv('dataset/order_items.csv')
df_order_items = df_order_items.where(pd.notnull(df_order_items), None)

cursor.execute("""
CREATE TABLE IF NOT EXISTS order_items (
    id INT PRIMARY KEY,
    order_id INT,
    user_id INT,
    product_id INT,
    inventory_item_id INT,
    status VARCHAR(50),
    created_at DATETIME,
    shipped_at DATETIME,
    delivered_at DATETIME,
    returned_at DATETIME,
    sale_price FLOAT
)
""")

for _, row in df_order_items.iterrows():
    cursor.execute("""
        INSERT INTO order_items (id, order_id, user_id, product_id, inventory_item_id, status, created_at, shipped_at, delivered_at, returned_at, sale_price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        int(row['id']),
        int(row['order_id']) if row['order_id'] else None,
        int(row['user_id']) if row['user_id'] else None,
        int(row['product_id']) if row['product_id'] else None,
        int(row['inventory_item_id']) if row['inventory_item_id'] else None,
        row['status'],
        row['created_at'],
        row['shipped_at'],
        row['delivered_at'],
        row['returned_at'],
        float(row['sale_price']) if row['sale_price'] else None
    ))



df_inventory_items = pd.read_csv('dataset/inventory_items.csv')
df_inventory_items = df_inventory_items.where(pd.notnull(df_inventory_items), None)

cursor.execute("""
CREATE TABLE IF NOT EXISTS inventory_items (
    id INT PRIMARY KEY,
    product_id INT,
    created_at DATETIME,
    sold_at DATETIME,
    cost FLOAT,
    product_category VARCHAR(100),
    product_name VARCHAR(255),
    product_brand VARCHAR(255),
    product_retail_price FLOAT,
    product_department VARCHAR(100),
    product_sku VARCHAR(255),
    product_distribution_center_id INT
)
""")

for _, row in df_inventory_items.iterrows():
    cursor.execute("""
        INSERT INTO inventory_items (id, product_id, created_at, sold_at, cost, product_category, product_name, product_brand, product_retail_price, product_department, product_sku, product_distribution_center_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        int(row['id']),
        int(row['product_id']) if row['product_id'] else None,
        row['created_at'],
        row['sold_at'],
        float(row['cost']) if row['cost'] else None,
        row['product_category'],
        row['product_name'],
        row['product_brand'],
        float(row['product_retail_price']) if row['product_retail_price'] else None,
        row['product_department'],
        row['product_sku'],
        int(row['product_distribution_center_id']) if row['product_distribution_center_id'] else None
    ))




df_distribution_centers = pd.read_csv('dataset/distribution_centers.csv')
df_distribution_centers = df_distribution_centers.where(pd.notnull(df_distribution_centers), None)

cursor.execute("""
CREATE TABLE IF NOT EXISTS distribution_centers (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT
)
""")

for _, row in df_distribution_centers.iterrows():
    cursor.execute("""
        INSERT INTO distribution_centers (id, name, latitude, longitude)
        VALUES (%s, %s, %s, %s)
    """, (
        int(row['id']),
        row['name'],
        float(row['latitude']) if row['latitude'] else None,
        float(row['longitude']) if row['longitude'] else None
    ))









cnx.commit()
cursor.close()
cnx.close()
