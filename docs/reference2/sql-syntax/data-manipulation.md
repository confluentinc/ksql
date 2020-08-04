- DML: how to change data

## Inserting data

- It's just a Kafka topic
- Using the insert statement
- Connector management
- Using the Kafka clients

... And any other way to populate a Kafka topic

- Make sure keys are correct and serialization/schema is right.
- Otherwise will go into processing log.

## Updating data

- Unlike standard SQL in this regard
- Streams are immutable, so there is no updating
- But tables are. This is how persistent queries work

## Deleting data

- No deletes on streams
- Tombstones for tables
