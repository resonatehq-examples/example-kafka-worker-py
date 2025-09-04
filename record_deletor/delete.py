from .config import producer, consumer
from resonate import Resonate
from random import randint
import json

resonate = Resonate.remote()

def delete_batch(_, record_id, batch_size=10):
    print(f"deleting a batch of rows related to record {record_id}")
    if randint(1, 100) < 25:
        print(f"simulated error while processing record {record_id}")
        raise Exception(f"simulated error while processing record {record_id}")
    if randint(1, 100) < 25:
        return False
    return True

def enqueue(_, msg_id, previous_offset):
    payload = json.dumps((msg_id, previous_offset)).encode("utf-8")
    producer.produce("records_that_were_deleted", value=payload)
    producer.flush()

@resonate.register
def workflow(ctx, record_id, offset):
    print(f"processing record {record_id} in position {offset}")
    while (yield ctx.run(delete_batch, record_id)):
        print(f"record {record_id} still has rows to delete")
        yield ctx.sleep(5)
    print(f"all rows deleted for record {record_id} in position {offset}")
    yield ctx.run(enqueue, record_id, offset)

def consume():
    consumer.subscribe(["records_to_be_deleted"])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                record_id = json.loads(msg.value().decode("utf-8"))
                if isinstance(record_id, str):  # in case json.dumps(msg_id) was just a string
                    record_id = [record_id]
                _ = workflow.begin_run(record_id[0], record_id[0], msg.offset())
            except Exception as e:
                print(f"Error processing message: {e}")
    except KeyboardInterrupt:
        print("Shutting down consumer.")
    finally:
        consumer.close()

def main():
    resonate.start()
    print("processor running")
    consume()

if __name__ == "__main__":
    main()