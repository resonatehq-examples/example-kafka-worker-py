import click
from kafka.admin import NewTopic
from .config import admin

@click.command()
def main():
    try:
        admin.create_topics([
            NewTopic(name="tombstones", num_partitions=1, replication_factor=1),
            NewTopic(name="processed", num_partitions=1, replication_factor=1),
        ])
    except Exception as e:
        click.echo(f"Error: {e}")
    finally:
        click.echo("Kafka topics created.")
        admin.close()