A stream is a durable, partitioned sequence of immutable events. When a new event is added a stream, it is appended to the partition that its key belongs to. They’re useful for modeling a historical sequence of activity. For example, you might use them to model a series of customer purchases or a sequence of readings from a sensor. Under the hood, streams are simply stored as Kafka topics with an enforced schema. A stream can either be created from scratch or declared on top of an existing Kafka topic. In either case, a variety of configuration options can be supplied.

Create a stream from scratch
------------------------------

If a stream is created from scratch, a backing Kafka topic will be created automatically. To create a stream from scratch, give it a name, schema, and configuration options:

```sql
CREATE STREAM publications (author VARCHAR, title VARCHAR)
    WITH (kafka_topic = 'publication_events',
          partitions = 3,
          key = 'author',
          value_format = 'avro');
```

In this example, a new stream named `publications` is created with two columns: `author` and `title`. Both are of type `VARCHAR`. ksqlDB automatically created an underlying `publication_events` topic that can be freely accessed. The topic has 3 partitions, and any new events that are appended to the stream are hashed according to the value of the `author` column. Because Kafka can store data in a variety of formats, we let ksqlDB know that we want the value portion of each row stored in the Avro format. A variety of configuration options can be used in the final `WITH` clause.

If you create a stream from scratch, you must supply the number of partitions.

Create a stream over an existing Kafka topic
-----------------------------------------------

A stream can also be created on top of an existing Kafka topic. Internally, ksqlDB simply registers the topic with the provided schema and does not create anything new:

```sql
CREATE STREAM publications (author VARCHAR, title VARCHAR)
    WITH (kafka_topic = 'publication_events',
          value_format = 'avro');
```

Because the topic already exists, the number of partitions cannot be included. The key should not be set here either, because any data that already exists in the same has a given key.

If an underlying event in the Kafka topic doesn’t conform to the given stream schema, the event is discarded at read-time.