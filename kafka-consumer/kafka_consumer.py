from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from config import KAFKA_BROKER, SCHEMA_REGISTRY_URL
from db import get_clickhouse_client

def kafka_consumer_worker(topic_name: str, group_id: str, insert_fn):
    """
    Run a Kafka consumer worker for a specific topic.
    """
    print(f"[START] Consumer for topic topiku: {topic_name}")
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': group_id,
        'schema.registry.url': SCHEMA_REGISTRY_URL,
        'auto.offset.reset': 'earliest'
    }
    consumer = AvroConsumer(consumer_config)
    consumer.subscribe([topic_name])
    client = get_clickhouse_client()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ Kafka error: {msg.error()}")
                continue
            value = msg.value()
            if not isinstance(value, dict) or "after" not in value:
                continue
            insert_fn(client, value["after"])
            print(f"[{topic_name}] Inserted: {value['after']}")
    except SerializerError as e:
        print(f"Error w {topic_name}: {e}")
    except KeyboardInterrupt:
        print(f"Stopped {topic_name}")
    finally:
        consumer.close()