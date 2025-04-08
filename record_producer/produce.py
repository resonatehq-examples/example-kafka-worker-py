import click
import json
import uuid
import random
from .config import producer


@click.command()
@click.option("-n", type=click.IntRange(min=1), required=True, help="Number of records to produce")
def main(n: int) -> None:
    messages = [
        (
            uuid.uuid4().hex,
            i,
            random.randint(1, 90),
        )
        for i in range(n)
    ]

    for msg in messages:
        producer.send("tombstones", value=json.dumps(msg))
    producer.flush()
    producer.close()
    click.echo(f"Produced {n} messages.")