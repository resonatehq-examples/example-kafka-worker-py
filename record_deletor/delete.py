from .config import producer, consumer
from resonate import Resonate, Context
from resonate.typing import Yieldable
from typing import Generator, Any
from kafka import KafkaConsumer
from random import randint
import json

resonate = Resonate()


def delete_batch(_: Context, record_id: str, batch_size = 10) -> Generator[Yieldable, Any, bool]:
    print(f"deleting a batch of rows related to record {record_id}")
    # This function simulates batch deletions of rows associated with the record ID
    # We pretend to delete a maxium of 10 rows at a time
    # There is a 25% chance of an error occurring, to showcase automatic retries
    # If an error occurs, Resonate automatically retries the function
    # We have no way of knowing how many rows are associated with the record ID
    # So there is 25% chance that all rows have been deleted while executing the query
    # If all rows are deleted, we return False to stop the processing
    # Otherwise, we return True to continue processing
    if randint(1, 100) < 25:
        print(f"simulated error while processing record {record_id}")
        raise Exception(f"simulated error while processing record {record_id}")
    if randint(1, 100) < 25:
        return False
    return True


def enqueue(_: Context, msg_id: str, previous_offset: str) -> None:
    # Here we enqueue a message to another topic indicating that all data related to the record has been deleted
    # We include the message ID and the offset of the original message
    # We can then inspect this topic to see the order in which records were processed
    producer.send("records_that_were_deleted", value=json.dumps((msg_id, previous_offset)))
    producer.flush()


@resonate.register
def workflow(ctx: Context, record_id: str, offset: int) -> Generator[Yieldable, Any, None]:
    print(f"processing record {record_id} in position {offset}")
    # Simulate batch deletion of rows
    while (yield ctx.lfc(delete_batch, record_id)):
        print(f"record {record_id} still has rows to delete")
        # Sleep for 5 seconds to not overwhelm the database
        yield ctx.sleep(5)
    print(f"all rows deleted for record {record_id} in position {offset}")
    # Add a new message to another topic indicating that the record has been processed
    yield ctx.lfc(enqueue, record_id, offset)


def consume() -> None:
    # Here we connect to the topic and consume any messages that haven't been processed
    msg_consumer = KafkaConsumer("records_to_be_deleted", **consumer)
    try:
        for message in msg_consumer:
            # We grab the record ID from the message 
            record_id = json.loads(message.value)[0]
            # Invoke the process function with the record ID and the offset of the message
            workflow.run(record_id, record_id, message.offset)
    except Exception as e:
        print(f"Fatal consumer error: {e}")
    finally:
        msg_consumer.close()


def main() -> None:
    print("processor running")
    consume()


if __name__ == "__main__":
    main()
