A table is a durable, partitioned collection used to model change over time. It is the mutable counterpart to the immutable stream. By contrast to streams, which represent a historical sequence of events, tables represent what is true as of “now”. As an example, you might model the locations that someone has lived at as a stream: first Miami, then New York, then London, and so forth. You would then use a table to roll this information up and tell you where they live right now. Tables can also be used to materialize a view by incrementally aggregating a stream of events.

Tables work by leveraging the keys of each event. Recall that keys are used to denote identity. If a sequence of events shares a key, clearly the last event for a given key represents the most up to date information. Under the hood, ksqlDB uses Kafka’s notion of a compacted topic to make this work. Compaction is a process that periodically deletes all but the newest events for each key.

A table can be created from scratch or declared on top of an existing Kafka topic. A variety of configuration options can be supplied. In either case, the table is not “materialized”, which limits its ability to be queried. Only tables which are derived from other collections are materialized.

Create a table from scratch
---------------------------

If a table is created from scratch, a backing compacted Kafka topic will be created automatically. To create a table from scratch, give it a name, schema, and configuration options:

```sql
CREATE TABLE movies (title VARCHAR, release_year INT)
    WITH (kafka_topic = 'movies',
          key = ‘title’
          partitions = 5,
          value_format = 'avro');
```

In this example, a new table named movies is created with two columns: title and release_yar. ksqlDB automatically created an underlying movies topic that can be freely accessed. The topic has 5 partitions, and any new events that are integrated into the table are hashed according to the value of the title column. Because Kafka can store data in a variety of formats, we let ksqlDB know that we want the value portion of each row stored in the Avro format. A variety of configuration options can be used in the final WITH clause.

If a table is created from scratch, you must supply the number of partitions.

Create a table over an existing Kafka topic
-------------------------------------------

A table can also be created on top of an existing Kafka topic. Internally, ksqlDB simply registers the topic with the provided schema and does not create anything new:

```sql
CREATE TABLE movies (title VARCHAR, release_year INT)
    WITH (kafka_topic = 'movies',
          value_format = 'avro');
```

Because the topic already exists, the number of partitions cannot be included. The key should not be set here either, because any data that already exists in the same has a given key.

If an underlying event in the Kafka topic doesn’t conform to the given stream schema, the event is discarded at read-time.
