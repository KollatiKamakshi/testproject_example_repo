import mysql.connector
import pandas as pd


def create_connection():
    return mysql.connector.connect(
        user="root", password="Kollati@2004", host="localhost", database="easytradez"
    )


def create_cursor(connection):
    return connection.cursor()


def create_tables(cursor):
    cursor.execute(
        """
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
    """
    )
    cursor.execute(
        """
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
    """
    )
    cursor.execute(
        """
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
    """
    )
    cursor.execute(
        """
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
    """
    )
    cursor.execute(
        """
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
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS distribution_centers (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT
        )
    """
    )


def insert_data(cursor, df, query, transform_row):
    for _, row in df.iterrows():
        values = transform_row(row)
        cursor.execute(query, values)


def main():
    try:
        cnx = create_connection()
        cursor = create_cursor(cnx)
        create_tables(cursor)

        # Load and insert products
        products_df = pd.read_csv("dataset/products.csv").where(
            pd.notnull(pd.read_csv("dataset/products.csv")), None
        )
        insert_data(
            cursor,
            products_df,
            """
            INSERT IGNORE INTO products 
            (id, cost, category, name, brand, retail_price, department, sku, distribution_center_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            lambda row: (
                int(row["id"]) if row["id"] else None,
                float(row["cost"]) if row["cost"] else None,
                row["category"],
                row["name"],
                row["brand"],
                float(row["retail_price"]) if row["retail_price"] else None,
                row["department"],
                row["sku"],
                int(row["distribution_center_id"])
                if row["distribution_center_id"]
                else None,
            ),
        )

        # Load and insert orders
        orders_df = pd.read_csv("dataset/orders.csv").where(
            pd.notnull(pd.read_csv("dataset/orders.csv")), None
        )
        insert_data(
            cursor,
            orders_df,
            """
            INSERT INTO orders 
            (order_id, user_id, status, gender, created_at, returned_at, shipped_at, delivered_at, num_of_item)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            lambda row: (
                int(row["order_id"]) if row["order_id"] else None,
                int(row["user_id"]) if row["user_id"] else None,
                row["status"],
                row["gender"],
                row["created_at"],
                row["returned_at"],
                row["shipped_at"],
                row["delivered_at"],
                int(row["num_of_item"]) if row["num_of_item"] else None,
            ),
        )

        # Load and insert users
        users_df = pd.read_csv("dataset/users.csv").where(
            pd.notnull(pd.read_csv("dataset/users.csv")), None
        )
        insert_data(
            cursor,
            users_df,
            """
            INSERT INTO users 
            (id, first_name, last_name, email, age, gender, state, street_address, postal_code, city, country, latitude, longitude, traffic_source, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            lambda row: (
                int(row["id"]),
                row["first_name"],
                row["last_name"],
                row["email"],
                int(row["age"]) if row["age"] else None,
                row["gender"],
                row["state"],
                row["street_address"],
                row["postal_code"],
                row["city"],
                row["country"],
                float(row["latitude"]) if row["latitude"] else None,
                float(row["longitude"]) if row["longitude"] else None,
                row["traffic_source"],
                row["created_at"],
            ),
        )

        # Load and insert order_items
        order_items_df = pd.read_csv("dataset/order_items.csv").where(
            pd.notnull(pd.read_csv("dataset/order_items.csv")), None
        )
        insert_data(
            cursor,
            order_items_df,
            """
            INSERT INTO order_items 
            (id, order_id, user_id, product_id, inventory_item_id, status, created_at, shipped_at, delivered_at, returned_at, sale_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            lambda row: (
                int(row["id"]),
                int(row["order_id"]) if row["order_id"] else None,
                int(row["user_id"]) if row["user_id"] else None,
                int(row["product_id"]) if row["product_id"] else None,
                int(row["inventory_item_id"]) if row["inventory_item_id"] else None,
                row["status"],
                row["created_at"],
                row["shipped_at"],
                row["delivered_at"],
                row["returned_at"],
                float(row["sale_price"]) if row["sale_price"] else None,
            ),
        )

        # Load and insert inventory_items
        inventory_items_df = pd.read_csv("dataset/inventory_items.csv").where(
            pd.notnull(pd.read_csv("dataset/inventory_items.csv")), None
        )
        insert_data(
            cursor,
            inventory_items_df,
            """
            INSERT INTO inventory_items 
            (id, product_id, created_at, sold_at, cost, product_category, product_name, product_brand, product_retail_price, product_department, product_sku, product_distribution_center_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            lambda row: (
                int(row["id"]),
                int(row["product_id"]) if row["product_id"] else None,
                row["created_at"],
                row["sold_at"],
                float(row["cost"]) if row["cost"] else None,
                row["product_category"],
                row["product_name"],
                row["product_brand"],
                float(row["product_retail_price"])
                if row["product_retail_price"]
                else None,
                row["product_department"],
                row["product_sku"],
                int(row["product_distribution_center_id"])
                if row["product_distribution_center_id"]
                else None,
            ),
        )

        # Load and insert distribution_centers
        distribution_centers_df = pd.read_csv("dataset/distribution_centers.csv").where(
            pd.notnull(pd.read_csv("dataset/distribution_centers.csv")), None
        )
        insert_data(
            cursor,
            distribution_centers_df,
            """
            INSERT INTO distribution_centers 
            (id, name, latitude, longitude)
            VALUES (%s, %s, %s, %s)
            """,
            lambda row: (
                int(row["id"]),
                row["name"],
                float(row["latitude"]) if row["latitude"] else None,
                float(row["longitude"]) if row["longitude"] else None,
            ),
        )

        cnx.commit()
        print("All data loaded successfully!")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()


if __name__ == "__main__":
    main()
