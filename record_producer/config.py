# config.py
from confluent_kafka import Producer

producer_config = {
    "bootstrap.servers": "localhost:9092",
}

producer = Producer(producer_config)