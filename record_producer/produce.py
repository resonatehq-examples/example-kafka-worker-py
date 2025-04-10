import click
import json
import uuid
from .config import producer


@click.command()
@click.option(
    "-n",
    type=click.IntRange(min=1),
    required=True,
    help="Number of records to produce",
)
def main(n: int) -> None:
    # We produce messages that contains an ID
    # The ID corresponds to a record
    # The record represents an unknown number of rows to be deleted
    messages = [
        (
            uuid.uuid4().hex,
        )
        for _ in range(n)
    ]

    for msg in messages:
        producer.send("records_to_be_deleted", value=json.dumps(msg))
    producer.flush()
    producer.close()
    click.echo(f"Produced {n} messages.")
