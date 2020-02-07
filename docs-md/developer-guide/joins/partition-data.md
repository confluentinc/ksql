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

Keys
----

A *key*, when present, defines the partitioning column. Tables are
always partitioned by their primary key, and ksqlDB doesn't allow repartitioning
of tables, so you can only use a table's primary key as a join column.

Streams, on the other hand, may not have a defined key or may have a key that
differs from the join column. In these cases, ksqlDB internally repartitions
the stream, which implicitly defines a key for it.

ksqlDB requires keys to use the `KAFKA` format. For more information, see
[Serialization Formats](serialization.md#serialization-formats). If internally
repartitioning, ksqlDB uses the correct format.

Because you can only use the primary key of a table as a joining column, it's
important to understand how keys are defined. For both streams and tables, the
column that represents the key has the name `ROWKEY`.

When you create a table by using a CREATE TABLE statement, the key of the
table is the same as that of the records in the underlying Kafka topic.
You must set the type of the `ROWKEY` column in the
CREATE TABLE statement to match the key data in the underlying {{ site.ak }} topic.

When you create a table by using a CREATE TABLE AS SELECT statement, the key of
the resulting table is determined as follows:

- If the FROM clause contains a stream, the statement must have a GROUP BY clause,
  and the grouping columns determine the key of the resulting table.
    - When grouping by a single column or expression, the type of `ROWKEY` in the
    resulting stream matches the type of the column or expression.
    - When grouping by multiple columns or expressions, the type of `ROWKEY` in the
    resulting stream is a `STRING`.
- If the FROM clause contains only tables and no GROUP BY clause, the key is
  copied over from the key of the table(s) in the FROM clause.
- If the FROM clause contains only tables and has a GROUP BY clause, the
  grouping columns determine the key of the resulting table.
    - When grouping by a single column or expression, the type of `ROWKEY` in the
    resulting stream matches the type of the column or expression.
    - When grouping by multiple columns or expressions, the type of `ROWKEY` in the
    resulting stream is a `STRING`.

The following example shows a `users` table joined with a `clicks` stream
on the `userId` column. The `users` table has the correct primary key
`userId` that coincides with the joining column. But the `clicks` stream
doesn't have a defined key, and ksqlDB must repartition it on the joining
column (`userId`) and assign the key before performing the join.

```sql
    -- clicks stream, with an unknown key.
    -- the schema of stream clicks is: ROWTIME BIGINT | ROWKEY STRING | USERID BIGINT | URL STRING
    CREATE STREAM clicks (userId BIGINT, url STRING) WITH(kafka_topic='clickstream', value_format='json');

    -- the primary key of table users is a BIGINT. 
    -- The userId column in the value matches the key, so can be used as an alias for ROWKEY in queries to make them more readable.
    -- the schema of table users is: ROWTIME BIGINT | ROWKEY BIGINT | USERID BIGINT | FULLNAME STRING
    CREATE TABLE  users  (ROWKEY BIGINT KEY, userId BIGINT, fullName STRING) WITH(kafka_topic='users', value_format='json', key='userId');

    -- join of users table with clicks stream, joining on the table's primary key alias and the stream's userId column: 
    -- join will automatically repartition clicks stream:
    SELECT clicks.url, users.fullName FROM clicks JOIN users ON clicks.userId = users.userId;
    
    -- The following is equivalent and does not rely on their being a copy of the tables key within the value schema:
    SELECT clicks.url, users.fullName FROM clicks JOIN users ON clicks.userId = users.ROWKEY;
```

Co-partitioning Requirements
----------------------------

When you use ksqlDB to join streaming data, you must ensure that your streams
and tables are *co-partitioned*, which means that input records on both sides
of the join have the same configuration settings for partitions.

- The input records for the join must have the
  [same keying schema](#records-have-the-same-keying-schema).
- The input records must have the
  [same number of partitions](#records-have-the-same-number-of-partitions)
  on both sides.
- Both sides of the join must have the
  [same partitioning strategy](#records-have-the-same-partitioning-strategy).

When your inputs are co-partitioned, records with the same key, from
both sides of the join, are delivered to the same stream task during
processing.

### Records Have the Same Keying Schema

For a join to work, the keys from both sides must have the same SQL type.

For example, you can join a stream of user clicks that's keyed on a `VARCHAR`
user id with a table of user profiles that's also keyed on a `VARCHAR` user id.
Records with the exact same user id on both sides will be joined.

If the schema of the columns you wish to join on don't match, it may be possible
to `CAST` one side to match the other. For example, if one side of the join
had a `INT` userId column, and the other a `LONG`, then you may choose to cast
the `INT` side to a `LONG`:

```sql
    -- stream with INT userId
    CREATE STREAM clicks (userId INT, url STRING) WITH(kafka_topic='clickstream', value_format='json');

    -- table with BIGINT userId stored in they key:
    CREATE TABLE  users  (ROWKEY BIGINT KEY, fullName STRING) WITH(kafka_topic='users', value_format='json');
    
   -- Join utilising a CAST to convert the left sides join column to match the rights type.
   SELECT clicks.url, users.fullName FROM clicks JOIN users ON CAST(clicks.userId AS BIGINT) = users.ROWKEY;
```


Tables created on top of existing Kafka topics, for example those created with
a `CREATE TABLE` statement, are keyed on the data held in the key of the records
in the Kafka topic. ksqlDB presents this data in the `ROWKEY` column and expects
the data to be in the `KAFKA` format.

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
