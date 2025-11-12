import json
import requests
from airflow.models import Connection
from airflow.utils.session import provide_session
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer

from utils.config import SCHEMA_REGISTRY_URL, KAFKA_CONNECT_URL, CONFIG_PATH


@provide_session
def create_default_connections(session=None):
    """Tworzy połączenia do komponentów ELT jeśli jeszcze nie istnieją."""
    default_conns = [
        Connection(
            conn_id='application_postgres',
            conn_type='postgres',
            host='application-postgres',
            login='user',
            password='password',
            port=5432,
            schema='store'
        ),
        Connection(
            conn_id='clickhouse',
            conn_type='http',
            host='clickhouse',
            port=8123,
            schema='mydb'
        ),
        Connection(
            conn_id='dbt_api',
            conn_type='http',
            host='http://dbt_api',
            port=8000
        ),
        Connection(
            conn_id='raport-creator',
            conn_type='http',
            host='http://raport-creator',
            port=8000
        ),
        Connection(
            conn_id='kafka_connect',
            conn_type='http',
            host='http://kafka-connect',
            port=8083
        ),
        Connection(
            conn_id='kafka-to-clickhouse-consumer',
            conn_type='http',
            host='http://kafka-to-clickhouse-consumer',
            port=8000
        ),
        Connection(
            conn_id='t5',
            conn_type='kafka',
            host='broker-1',
            port=19092,
            extra={
                "security.protocol": "PLAINTEXT",
                "bootstrap.servers": "broker-1:19092",
                "group.id": "airflow-await-message-sensor",
                "auto.offset.reset": "earliest"
            }
        ),
        Connection(
            conn_id='schema_registry',
            conn_type='http',
            host='http://schema-registry',
            port=8081
        )
    ]

    for conn in default_conns:
        existing = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
        if existing:
            print(f"✅ Connection '{conn.conn_id}' already exists — skipping.")
        else:
            session.add(conn)
            session.commit()
            print(f"🆕 Created connection '{conn.conn_id}'.")

def configure_kafka_connect():
    """Wysyła konfigurację JSON do Kafka Connect API."""
    with open(CONFIG_PATH, 'r', encoding='utf-8') as conf:
        json_data = json.load(conf)

    try:
        r = requests.post(url=KAFKA_CONNECT_URL, json=json_data)
        r.raise_for_status() 
        print("Kafka Connect skonfigurowany pomyślnie.")
    except requests.exceptions.RequestException as e:
        print(f"Błąd podczas konfiguracji Kafka Connect: {e}")
        raise Exception(f"Failed to configure Kafka Connect: {e}")


def check_response_clickhouse(response):
    """Sprawdza odpowiedź z API dla tworzenia tabel ClickHouse."""
    try:
        data = response.json()
        return data.get("status") == "OK" and data.get("message") == "Tables created"
    except Exception:
        return False

def check_response_kafka(response):
    """Sprawdza odpowiedź z API dla uruchomienia konsumera Kafki."""
    try:
        data = response.json()
        return data.get("status") == "OK" and data.get("message") == "Kafka consumers started in 3 threads"
    except Exception:
        return False


_registry_client = None
_serializer = None

def get_serializer():
    """Init of serializer (lazy loading)."""
    global _registry_client, _serializer
    if _serializer is None:
        _registry_client = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        _serializer = MessageSerializer(_registry_client)
    return _serializer

def decode_avro(msg):
    """
    Printing message type
    """
    print(f"Odebrano wiadomość (typ: {type(msg)}), sensor może kontynuować.")
    return "Message"