import clickhouse_connect
from config import CLICKHOUSE_HOST, CLICKHOUSE_DB, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD

def get_clickhouse_client():
    """Get a ClickHouse client instance."""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        database=CLICKHOUSE_DB,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )

def create_tables():
    """Create tables in ClickHouse."""
    client = get_clickhouse_client()
    client.command('''
        CREATE TABLE IF NOT EXISTS transactions (
            id UInt32,
            user_id UInt32,
            product_id UInt32,
            quantity UInt8,
            value Float64,
            transaction_date UInt64
        ) ENGINE = MergeTree
        ORDER BY id;
    ''')
    client.command('''
        CREATE TABLE IF NOT EXISTS users (
            id UInt32,
            first_name String,
            days_of_experience Int32,
            total_purchase_amount Float64,
            state_province String
        ) ENGINE = MergeTree
        ORDER BY id;
    ''')
    client.command('''
        CREATE TABLE IF NOT EXISTS products (
            id UInt32,
            carat Float32,
            cut String,
            color String,
            clarity String,
            depth Float32,
            table_val Float32,
            price Float64
        ) ENGINE = MergeTree
        ORDER BY id;
    ''')

def insert_transaction(client, record):
    """Insert a transaction record into ClickHouse."""
    row = [[
        record['id'],
        record['user_id'],
        record['product_id'],
        record['quantity'],
        record['value'],
        record['transaction_date']
    ]]
    client.insert("transactions", row)

def insert_user(client, record):
    """Insert a user record into ClickHouse."""
    row = [[
        record['id'],
        record['first_name'],
        record['days_of_experience'],
        record['total_purchase_amount'],
        record['state_province']
    ]]
    client.insert("users", row)

def insert_product(client, record):
    """Insert a product record into ClickHouse."""
    row = [[
        record['id'],
        record['carat'],
        record['cut'],
        record['color'],
        record['clarity'],
        record['depth'],
        record['table_val'],
        record['price']
    ]]
    client.insert("products", row)