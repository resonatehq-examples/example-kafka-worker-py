import json
import time
from kafka import KafkaConsumer
from .config import producer, consumer
from resonate import Resonate, Context
from typing import Generator, Any
from resonate.typing import Yieldable

resonate = Resonate()

def enqueue(_: Context, msg_id:str, n:int, duration: int) -> None:
    producer.send("processed", value=json.dumps((msg_id, duration)))
    producer.flush()

@resonate.register
def process(ctx: Context, msg_id: str, n:int, duration: int) -> Generator[Yieldable, Any, None]:
    try:
        print(f"processing tombstone {msg_id} will take {duration} seconds, position {n}")
        yield ctx.sleep(duration)
        print(f"tombstone {msg_id} processed, was position {n}")
        yield ctx.lfc(enqueue, msg_id, n, duration)
    except Exception as e:
        print(e)

def consume() -> None:
    print("Starting consumer loop...")
    msg_consumer = KafkaConsumer("tombstones", **consumer)
    try:
        for message in msg_consumer:
            msg_id, n, duration = json.loads(message.value)
            process.run(msg_id, msg_id, n, duration)
    except Exception as e:
        print(f"Fatal consumer error: {e}")
    finally:
        msg_consumer.close()


def main() -> None:
    print("processor running")
    consume()


if __name__ == "__main__":
    main()