from fastapi import FastAPI, HTTPException
import time
import os
import threading
from raport_generator_and_sender import create_report
from raport_sender import send_report
import clickhouse_connect
import pandas as pd
app = FastAPI()

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "mydb")

@app.post("/create_raport")
def create_table():
    """
    Create report
    """
    time.sleep(10)  

    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        database=CLICKHOUSE_DB,
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "")
    )
    try:
        result_fact_sales = client.query('SELECT * FROM mydb_marts.fact_sales;')
        rows_fact_sales = result_fact_sales.result_rows 
        columns_fact_sales = result_fact_sales.column_names 
        sales_df = pd.DataFrame(rows_fact_sales, columns=columns_fact_sales)
        create_report(sales_df)
        send_report()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    return {"status": "OK", "message": "DFs created"}


    """
    Uruchamia 3 konsumery (każdy w osobnym wątku).
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
