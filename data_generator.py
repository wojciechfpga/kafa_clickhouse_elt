import time
import psycopg2
import pandas as pd
import seaborn as sns
import numpy as np
from faker import Faker
from random import randint, gauss
from psycopg2.extras import execute_batch

fake = Faker('en_US')

PG_HOST = "localhost"
PG_PORT = 5433
PG_USER = "user"
PG_PASS = "password"
PG_DB   = "store"

print(f"🔗 Connecting to PostgreSQL ({PG_HOST}:{PG_PORT}) ...")
try:
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASS, dbname=PG_DB
    )
    conn.autocommit = True
    cur = conn.cursor()
    print("✅ Connected to the database.")
except psycopg2.Error as e:
    print(f"❌ Error connecting to the database: {e}")
    exit(1)

print("reating tables...")
cur.execute("""
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    carat FLOAT,
    cut VARCHAR(50),
    color VARCHAR(50),
    clarity VARCHAR(50),
    depth FLOAT,
    table_val FLOAT,
    price FLOAT,
    x FLOAT, y FLOAT, z FLOAT
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(80),
    last_name VARCHAR(80),
    phone VARCHAR(80),
    state_province VARCHAR(80),
    address TEXT,
    days_of_experience INT,
    total_purchase_amount FLOAT
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    product_id INT REFERENCES products(id),
    quantity INT,
    value FLOAT,
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")
print("Tables created or verified.")

diamonds = sns.load_dataset("diamonds").head(1000)
diamonds = diamonds.rename(columns={"table": "table_val"})

cur.execute("SELECT COUNT(*) FROM products;")
if cur.fetchone()[0] == 0:
    print("Loading first 1000 product records (seaborn.diamonds)...")

    diamond_records = [
        tuple(row)
        for row in diamonds[['carat', 'cut', 'color', 'clarity', 'depth', 'table_val', 'price', 'x', 'y', 'z']].to_numpy()
    ]

    batch_size = 100
    insert_query = """
        INSERT INTO products (carat, cut, color, clarity, depth, table_val, price, x, y, z)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """

    for i in range(0, len(diamond_records), batch_size):
        batch = diamond_records[i:i + batch_size]
        execute_batch(cur, insert_query, batch, page_size=batch_size)
        print(f"Inserted batch {i//batch_size + 1}/{len(diamond_records)//batch_size + 1}")

    print(f"Inserted {len(diamond_records)} products total.")
else:
    print("Products table already contains data.")

cur.execute("SELECT COUNT(*) FROM users;")
if cur.fetchone()[0] == 0:
    print("👤 Creating dummy users...")
    for _ in range(500):
        cur.execute("""
            INSERT INTO users (first_name, last_name, phone, state_province, address, days_of_experience, total_purchase_amount)
            VALUES (%s,%s,%s,%s,%s,%s,%s);
        """, (
            fake.first_name(),
            fake.last_name(),
            fake.phone_number(),
            fake.state_abbr(),
            fake.address().replace("\n", ", "),
            int(abs(gauss(365, 180))),
            abs(gauss(20000, 5000))
        ))
    print("Created 500 users.")
else:
    print("Users table already contains data.")

cur.execute("SELECT MAX(id) FROM users;")
USER_COUNT = cur.fetchone()[0] or 0

cur.execute("SELECT MAX(id) FROM products;")
PRODUCT_COUNT = cur.fetchone()[0] or 0

print(f"Users: {USER_COUNT},  Products: {PRODUCT_COUNT}")

print("🚀 Starting transaction generation (every 0.01s)...")
cnt = 0
TRANSACTION_LIMIT = 40000 
while cnt < TRANSACTION_LIMIT:
    cnt = cnt + 1
    user_id = randint(1, USER_COUNT)
    prod_id = randint(1, PRODUCT_COUNT)

    cur.execute("SELECT price FROM products WHERE id = %s;", (prod_id,))
    result = cur.fetchone()
    if not result:
        continue  
    price = result[0]

    quantity = randint(1, 5)
    value = price * quantity

    cur.execute("""
        INSERT INTO transactions (user_id, product_id, quantity, value)
        VALUES (%s, %s, %s, %s);
    """, (user_id, prod_id, quantity, value))
    
    if cnt % 1000 == 0:
        print(f"Inserted {cnt} transactions...")

    time.sleep(0.01)

print(f"Finished inserting {cnt} transactions.")
cur.close()
conn.close()
print("Connection closed.")