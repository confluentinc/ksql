---
layout: page
title: Partition Data to Enable Joins
tagline: Correct partitioning for joins
description: Learn how to partition topics correctly to enable join queries.
keywords: ksqldb, join, partition, key, schema 
---

When you use ksqlDB to join streaming data, you must ensure that your
streams and tables are *co-partitioned*, which means that input records
on both sides of the join have the same configuration settings for
partitions.

To join two data sources, streams or tables, ksqlDB needs to compare their
records based on the joining column. To ensure that records with the same
join column are co-located on the same stream task, the join column must
coincide with the column that the sources are partitioned by.

Primary key
-----------

A *primary key*, when present, defines the partitioning column. Tables are
always partitioned by their primary key, and ksqlDB doesn't allow repartitioning
of tables, so you can only use a table's primary key as a join column.

Streams, on the other hand, may not have a defined key or may have a key that
differs from the join column. In these cases, ksqlDB repartitions the stream,
which implicitly defines a primary key for it. The primary keys for streams
and tables are of data type `VARCHAR`. 

For primary keys to match, they must have the same serialization format. For
example, you can't join a `VARCHAR` key encoded as JSON with one encoded as AVRO.

!!! note
    ksqlDB requires that keys are encoded as UTF-8 strings.

Because you can only use the primary key of a table as a joining column, it's
important to understand how keys are defined. For both streams and tables, the
column that represents the primary key has the name `ROWKEY`.

When you create a table by using a CREATE TABLE statement, the key of the
table is the same as that of the records in the underlying Kafka topic.

When you create a table by using a CREATE TABLE AS SELECT statement, the key of
the resulting table is determined as follows:

- If the FROM clause is a single source, and the source is a stream, the
  statement must have a GROUP BY clause, where the grouping columns determine
  the key of the resulting table.
- If the single source is a table, the key is copied over from the key of the
  table in the FROM clause. If the FROM clause is a join, the primary key of the
  resulting table is the joining column, since joins are allowed only on keys.
- If the statement contains a GROUP BY, the key of the resulting table
  comprises the grouping columns.

When the primary key consists of multiple columns, like when it's created as
the result of a GROUP BY clause with multiple grouping columns, you must use
ROWKEY as the joining column. Even when the primary key consists of a single
column, we recommend using ROWKEY as the joining column to avoid confusion.

The following example shows a `users` table joined with a `clicks` stream 
on the `userId` column. The `users` table has the correct primary key
`userId` that coincides with the joining column. But the `clicks` stream
doesn't have a defined key, and ksqlDB must repartition it on the joining column,
(`userId`) and assign the primary key before performing the join.

```sql
    -- clicks stream, with an unknown key.
    -- the schema of stream clicks is: ROWTIME | ROWKEY | USERID | URL
    CREATE STREAM clicks (userId STRING, url STRING) WITH(kafka_topic='clickstream', value_format='json');

    -- the primary key of table users becomes userId that is the key of the records topic:
    -- the schema of table users is: ROWTIME | ROWKEY | USERID | URL
    CREATE TABLE  users  (userId STRING, fullName STRING) WITH(kafka_topic='users', value_format='json', key='userId');

    -- join using primary key of table users with newly assigned key of stream clicks
    -- join will automatically repartition clicks stream:
    SELECT clicks.url, users.fullName FROM clicks JOIN users ON clicks.ROWKEY = users.ROWKEY;
```

Co-partitioning Requirements
----------------------------

When you use ksqlDB to join streaming data, you must ensure that your streams
and tables are *co-partitioned*, which means that input records on both sides
of the join have the same configuration settings for partitions.

- The input records for the join must have the
  [same keying scheme](#records-have-the-same-keying-scheme).
- The input records must have the
  [same number of partitions](#records-have-the-same-number-of-partitions)
  on both sides.
- Both sides of the join must have the
  [same partitioning strategy](#records-have-the-same-partitioning-strategy).

When your inputs are co-partitioned, records with the same key, from
both sides of the join, are delivered to the same stream task during
processing.

### Records Have the Same Keying Scheme

For a join to work, the keys from both sides must have the same serialized
binary data.

For example, you can join a stream of user clicks that's keyed on a `VARCHAR`
user id with a table of user profiles that's also keyed on a `VARCHAR` user id.
Records with the exact same user id on both sides will be joined.

ksqlDB requires that keys are UTF-8 encoded strings.

!!! note
    A join depends on the key's underlying serialization format. For example,
    no join occurs on a VARCHAR column that's encoded as JSON with a VARCHAR
    column that's encoded as AVRO.

Tables created on top of existing Kafka topics, for example those created with
a `CREATE TABLE` statement, are keyed on the data held in the key of the records
in the Kafka topic. ksqlDB presents this data in the `ROWKEY` column and expects
the data to be a `VARCHAR`.

Tables created inside ksqlDB from other sources, for example those created with
a `CREATE TABLE AS SELECT` statement, will copy the key from their source(s)
unless there is an explicit `GROUP BY` clause, which can change what the table
is keyed on.

!!! note
    ksqlDB automatically repartitions a stream if a join requires it, but ksqlDB
    rejects any join on a table's column that is *not* the key. This is
    because ksqlDB doesn't support joins on foreign keys, and repartitioning a
    table's topic has the potential to reorder events and misinterpret
    tombstones, which can lead to unintended or undesired side effects.

If you are using the same sources in more than one join that requires the data
to be repartitioned, you may prefer to repartition manually to avoid ksqlDB
repartitioning multiple times.

To repartition a stream, use the PARTITION BY clause. Be aware that Kafka
guarantees the relative order of any two messages from one source partition
only if they are also both in the same partition after the repartition.
Otherwise, Kafka is likely to interleave messages. The use case will determine
if these ordering guarantees are acceptable.

For example, if you need to re-partition a stream to be keyed by a `product_id`
field, and keys need to be distributed over 6 partitions to make a join work,
use the following SQL statement:

```sql
CREATE STREAM products_rekeyed WITH (PARTITIONS=6) AS SELECT * FROM products PARTITION BY product_id;
```

For more information, see
[Inspecting and Changing Topic Keys](https://www.confluent.io/stream-processing-cookbook/ksql-recipes/inspecting-changing-topic-keys)
in the [Stream Processing Cookbook](https://www.confluent.io/product/ksql/stream-processing-cookbook).

### Records Have the Same Number of Partitions

The input records for the join must have the same number of partitions on both
sides.

ksqlDB checks this part of the co-partitioning requirement and rejects any join
where the partition counts differ.

Use the `DESCRIBE EXTENDED <source name>` command in the CLI to determine the
Kafka topic under a source, and use the SHOW TOPICS command in the CLI to list
topics and their partition counts.

If the sides of the join have different partition counts, you may want to change
the partition counts of the source topics, or repartition one side to match the
partition count of the other.

The following example creates a repartitioned stream, maintaining the existing
key, with the specified number of partitions.

```sql
CREATE STREAM products_rekeyed WITH (PARTITIONS=6) AS SELECT * FROM products PARTITION BY ROWKEY;
```

### Records Have the Same Partitioning Strategy

Records on both sides of the join must have the same partitioning
strategy. If you use the default partitioner settings across all
applications, and your producers don't specify an explicit partition,
you don't need to worry about the partitioning strategy.

But if the producer applications for your records have custom
partitioners specified in
[configuration](http://kafka.apache.org/documentation/#producerconfigs),
the same custom partitioner logic must be used for records on both sides
of the join. The applications that write to the join inputs must have
the same partitioning strategy, so that records with the same key are
delivered to same partition number.

This means that the input records must be in the same partition on both
sides of the join. For example, in a stream-table join, if a `userId`
key with the value `alice123` is in Partition 1 for the stream, but
`alice123` is in Partition 2 for the table, the join won't match, even
though both sides are keyed by `userId`.

ksqlDB can't verify whether the partitioning strategies are the same for
both join inputs, so you must ensure this.

The
[DefaultPartitioner class](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java)
implements the following partitioning strategy:

- If the producer specifies a partition in the record, use it.
- If the producer specifies a key instead of a partition, choose a
  partition based on a hash of the key.
- If the producer doesn't specify a partition or a key, choose a
  partition in a round-robin fashion.

Custom partitioner classes implement the
[Partitioner interface](https://kafka.apache.org/20/javadoc/org/apache/kafka/clients/producer/Partitioner.html)
and are assigned in the producer configuration property,
`partitioner.class`.

For example implementations of a custom partitioner, see
[Built for realtime: Big data messaging with Apache Kafka, Part2](https://www.javaworld.com/article/3066873/big-data/big-data-messaging-with-kafka-part-2.html)
and [Apache Kafka Foundation Course - Custom Partitioner](https://www.learningjournal.guru/courses/kafka/kafka-foundation-training/custom-partitioner/).

Page last revised on: {{ git_revision_date }}
