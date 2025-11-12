import os

KAFKA_BROKER = "broker-1:19092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
TOPICS = {
    "transactions": "store.public.transactions",
    "users": "store.public.users",
    "products": "store.public.products",
}
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "mydb")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")