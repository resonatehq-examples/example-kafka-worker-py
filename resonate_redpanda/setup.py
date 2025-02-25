import click
from kafka.admin import NewTopic
from resonate_redpanda import config


@click.command()
def setup() -> None:
    try:
        config.admin.create_topics(
            [
                NewTopic(name="foo", num_partitions=1, replication_factor=1),
                NewTopic(name="bar", num_partitions=1, replication_factor=1),
            ]
        )
    except Exception as e:
        click.echo(f"Error: {e}")
    finally:
        click.echo("Topics created")
        config.admin.close()
