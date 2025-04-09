import click
import json
import uuid
from .config import producer


@click.command()
@click.option(
    "-n",
    type=click.IntRange(min=1),
    required=True,
    help="Number of tombstone records to produce",
)
def main(n: int) -> None:
    # We produce messages that contains an ID
    # The ID is meant to represent a tombstone record
    # The tombstone record represents an unknown number of rows to be deleted
    messages = [
        (
            uuid.uuid4().hex,
        )
        for _ in range(n)
    ]

    for msg in messages:
        producer.send("tombstones", value=json.dumps(msg))
    producer.flush()
    producer.close()
    click.echo(f"Produced {n} messages.")
