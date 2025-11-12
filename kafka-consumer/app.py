import time
import threading
from fastapi import FastAPI
from config import TOPICS
from db import create_tables, insert_transaction, insert_user, insert_product
from kafka_consumer import kafka_consumer_worker

app = FastAPI()

@app.post("/create_table")
def create_table_endpoint():
    """
    Create tables in ClickHouse.
    """
    time.sleep(10)
    create_tables()
    return {"status": "OK", "message": "Tables created"}

@app.post("/consume_kafka")
def consume_kafka_endpoint():
    """
    Start Kafka consumers in threads.
    """
    threads = [
        threading.Thread(
            target=kafka_consumer_worker,
            args=(TOPICS["transactions"], "group-transactions", insert_transaction),
            daemon=True
        ),
        threading.Thread(
            target=kafka_consumer_worker,
            args=(TOPICS["users"], "group-users", insert_user),
            daemon=True
        ),
        threading.Thread(
            target=kafka_consumer_worker,
            args=(TOPICS["products"], "group-products", insert_product),
            daemon=True
        )
    ]
    for t in threads:
        t.start()
    return {"status": "OK", "message": "Kafka consumers started in 3 threads"}