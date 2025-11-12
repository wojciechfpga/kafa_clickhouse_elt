from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor

from utils.config import DEFAULT_ARGS, KAFKA_CONFIG_ID, KAFKA_TOPICS
from utils.callables import (
    create_default_connections,
    configure_kafka_connect,
    check_response_clickhouse,
    check_response_kafka,
    decode_avro  
)

with DAG(
    dag_id="init",
    start_date=datetime(2023, 1, 1), 
    schedule='@once',
    catchup=False,
    default_args=DEFAULT_ARGS,
    description="Checking elements fo ELT pipeline"
) as dag:

    create_connections = PythonOperator(
        task_id='create_default_connections',
        python_callable=create_default_connections
    )

    wait_for_postgre = SqlSensor(
        task_id='check_postgresql',
        conn_id='application_postgres',
        sql="SELECT 1",
        fail_on_empty=True
    )

    wait_for_kafka_message = AwaitMessageSensor(
        task_id="awaiting_message",
        kafka_config_id=KAFKA_CONFIG_ID,
        topics=KAFKA_TOPICS,
        apply_function="utils.callables.decode_avro", 
        execution_timeout=timedelta(minutes=10)
    )

    configure_kafka_connect_by_http = PythonOperator(
        task_id="configure_kafka_connection",
        python_callable=configure_kafka_connect
    )

    clickhouse_table_creation = HttpSensor(
        task_id="clickhouse_table_creation",
        http_conn_id="kafka-to-clickhouse-consumer",
        endpoint="/create_table",
        poke_interval=10,
        timeout=600,
        method='POST',
        response_check=check_response_clickhouse 
    )

    kafka_subscribe = HttpSensor(
        task_id="kafka_subscribe",
        http_conn_id="kafka-to-clickhouse-consumer",
        endpoint="/consume_kafka",
        poke_interval=10,
        timeout=600,
        method='POST',
        response_check=check_response_kafka
    )

    (
        create_connections >>
        wait_for_postgre >>
        configure_kafka_connect_by_http >>
        clickhouse_table_creation >>
        [kafka_subscribe, wait_for_kafka_message]
    )