from datetime import timedelta

DEFAULT_ARGS = {
    'owner': 'Wojciech L.',
    'depends_on_past': False,
    'retries': 80,
    'retry_delay': timedelta(seconds=12)
}

KAFKA_CONNECT_URL = "http://kafka-connect:8083/connectors"
CONFIG_PATH = '/opt/airflow/dags/configuration.json'
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

KAFKA_CONFIG_ID = "t5"
KAFKA_TOPICS = ["store.public.transactions"]