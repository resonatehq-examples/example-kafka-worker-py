# config.py
from confluent_kafka import Producer, Consumer

BOOTSTRAP_SERVERS = "localhost:9092"

producer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
}

producer = Producer(producer_config)

consumer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "record_consumer_group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

consumer = Consumer(consumer_config)