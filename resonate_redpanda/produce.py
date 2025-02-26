import json
import click

from resonate_redpanda.config import producer
import random

import uuid


@click.command()
@click.option("-n", "n", type=click.IntRange(min=0))
def produce(n: int) -> None:
    messages = [
        (
            uuid.uuid4().hex,
            random.choice(("sum", "mul", "sub")),
            random.randint(1, 5),
            random.randint(1, 5),
        )
        for _ in range(n)
    ]
    for msg in messages:
        producer.send("foo", value=json.dumps(msg))
    producer.flush()
    producer.close()
    click.echo(f"Produced {n} messages")
