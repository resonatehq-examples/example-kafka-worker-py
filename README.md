# Tombstone Cleanup | Resonate + Redpanda example application

This is an example app showcasing what a RedPanda + Resonate pipeline might look like in the context of a "tombstone record processing" use case.

## Use case

todo

## How to run the example

This application uses [uv]() as the python environment and package manager.

Install dependencies:

```shell
uv sync
```

Run the RedPanda in Docker:

```shell
cd redpanda && docker compose up
```

Set up the topics:

```shell
uv run setup-topics
```

Run the tombstone record processor:

```
uv run record-processor
```

Create records:

```shell
uv run record-producer -n 25
```

You can provide any value for n.

### How to view records

RedPand provides a UI in which you can view the records / messages in the topic queues.

url: http://localhost:8080
username: john
password: some-secret-password

The record-producer script adds records to the tombstone topic / queue.
The rcord-prodcessor pulls records from the tombstone topic / queue and adds them to the processed topic / queue.
