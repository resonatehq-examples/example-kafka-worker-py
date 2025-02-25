import json
from typing import Any, Generator
import click
from kafka import KafkaConsumer
from resonate_redpanda.config import producer
from resonate.stores.local import LocalStore

from resonate_redpanda.config import (
    BOOTSTRAP_SERVERS,
    SASL_MECHANISM,
    SASL_PLAIN_PASSWORD,
    SASL_PLAIN_USERNAME,
    SECURITY_PROTOCOL,
)
from resonate import Context, Resonate
from resonate.typing import Yieldable

resonate = Resonate(store=LocalStore())


def sum(ctx: Context, a: int, b: int) -> int:
    return a + b


def sub(ctx: Context, a: int, b: int) -> int:
    return a - b


def mul(ctx: Context, a: int, b: int) -> int:
    return a * b


def enqueue(ctx: Context, op: str, a: int, b: int, v: int) -> None:
    producer.send("bar", value=json.dumps((op, a, b, v)))
    producer.flush()
    print("sent to next queue")


@resonate.register
def calc(ctx: Context, op: str, a: int, b: int) -> Generator[Yieldable, Any, None]:
    match op:
        case "sum":
            v: int = yield ctx.lfc(sum, a, b)
        case "sub":
            v: int = yield ctx.lfc(sub, a, b)
        case "mul":
            v: int = yield ctx.lfc(mul, a, b)
        case _:
            raise ValueError("Unexpected operation")

    yield ctx.lfc(enqueue, op, a, b, v)


@click.command()
def consume() -> None:
    consumer = KafkaConsumer(
        "foo",
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol=SECURITY_PROTOCOL,
        sasl_mechanism=SASL_MECHANISM,
        sasl_plain_username=SASL_PLAIN_USERNAME,
        sasl_plain_password=SASL_PLAIN_PASSWORD,
        group_id=None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda x: x.decode("utf-8"),
        max_poll_records=100,  # Control batch size
    )

    try:
        for message in consumer:
            try:
                op, a, b = json.loads(message.value)
                id = f"processing-{message.offset}"
                print(f"Processing: {id}")

                calc.run(id, op, a, b)

                # Commit after successful processing
                # consumer.commit()
                # print(f"Committed offset {message.offset}")

            except Exception as e:
                print(f"Failed to process message: {e}")
                # Handle error without committing offset
    finally:
        consumer.close()
