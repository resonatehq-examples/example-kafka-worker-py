# produce.py
import click
import json
import uuid
from .config import producer


def delivery_report(err, msg):
    if err is not None:
        click.echo(f"Delivery failed for record {msg.key()}: {err}")
    else:
        click.echo(f"Record produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


@click.command()
@click.option(
    "-n",
    type=click.IntRange(min=1),
    required=True,
    help="Number of records to produce",
)
def main(n: int) -> None:
    messages = [uuid.uuid4().hex for _ in range(n)]

    for msg in messages:
        producer.produce(
            topic="records_to_be_deleted",
            value=json.dumps(msg).encode("utf-8"),
            callback=delivery_report,
        )

    producer.flush()
    click.echo(f"Produced {n} messages.")