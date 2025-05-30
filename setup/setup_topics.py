# setup_topics.py
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": "localhost:9092"})

def main():
    # Create topics
    futures = admin.create_topics([
        NewTopic("records_to_be_deleted", num_partitions=1, replication_factor=1),
        NewTopic("records_that_were_deleted", num_partitions=1, replication_factor=1),
    ])

    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic {topic} created.")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")