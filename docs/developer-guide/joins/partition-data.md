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
the stream, which implicitly defines the correct key for it.

!!! important
    Be aware that Kafka guarantees the relative order of any two messages from
    one source partition only if they are also both in the same partition
    after the repartition. Otherwise, {{ site.ak }} is likely to interleave messages.
    The use case will determine if these ordering guarantees are acceptable.

ksqlDB requires keys to use the `KAFKA` format. For more information, see
[Serialization Formats](../serialization.md#serialization-formats). If internally
repartitioning, ksqlDB uses the correct format.

Because you can only use the primary key of a table as a joining column, it's
important to understand how keys are defined.

When you create a table by using a CREATE TABLE statement you define the key in the schema and
it must be the same as that of the records in the underlying {{ site.ak }} topic. As the
KAFKA format does not support named fields that key has no implicit name, so the key can be
given any name in the schema definition.

When you create a table by using a CREATE TABLE AS SELECT statement, the key of
the resulting table is determined as follows:

- If the statement contains neither a JOIN or GROUP BY clause, the key type of the resulting
  table matches the key type of the source table, and the name matches the source unless the
  projection defines an alias for the column.
- If the statement contains a JOIN and no GROUP BY clause, the key type of the resulting table
  will match the type of the join columns and then key name will:
    - FULL OUTER joins and joins on expressions other than column references will have a system
    generated name in the form `KSQL_COL_n`, where `n` is a positive integer, unless the projection
    defines an alias for the column.
    - For other joins that contain at least one column reference in their join criteria, the name
    will match the left most column reference in the join criteria.
- If the statement contains a GROUP BY clause, the grouping columns determine the key
  of the resulting table.
    - When grouping by a single column or STRUCT field: the name of the key column in the
      resulting table matches the name of the column or field, unless the projection includes
      an alias for the column or field, and the type of the key column matches the column or field.
    - When grouping by a single expression that is not a column or STRUCT field: the resulting table
      will have a system generated key column name in the form `KSQL_COL_n`, where `n` is a positive
      integer, unless the projection contains an alias for the expression, and the type of the
      column will match the result of the expression.
    - When grouping by multiple expressions: the resulting table will have a system generated key
      name in the form `KSQL_COL_n`, where `n` is a positive integer, and the type of the column will
      be a [SQL `STRING`](../../concepts/schemas), containing the grouping expressions concatenated
      together.

The following example shows a `users` table joined with a `clicks` stream
on the `userId` column. The `users` table has the correct primary key
`userId` that coincides with the joining column. But the `clicks` stream
doesn't have a defined key, so ksqlDB must repartition it on the joining
column (`userId`) to assign the key before performing the join.

```sql
    -- clicks stream, with an unknown key.
    -- the schema of stream clicks is: ROWKEY STRING KEY | USERID BIGINT | URL STRING
    CREATE STREAM clicks (userId BIGINT, url STRING) WITH(kafka_topic='clickstream', value_format='json');

    -- the primary key of table users is a BIGINT. 
    -- the schema of table users is: USERID BIGINT KEY | FULLNAME STRING
    CREATE TABLE  users (userId BIGINT PRIMARY KEY, fullName STRING) WITH(kafka_topic='users', value_format='json');

    -- join of users table with clicks stream, joining on the table's primary key and the stream's userId column:
    -- join will automatically repartition clicks stream:
    SELECT clicks.url, users.fullName FROM clicks JOIN users ON clicks.userId = users.userId;
```

Co-partitioning Requirements
----------------------------

When you use ksqlDB to join streaming data, you must ensure that your streams
and tables are *co-partitioned*, which means that input records on both sides
of the join have the same configuration settings for partitions.

- The input records for the join must have the
  [same key schema](#records-have-the-same-key-schema).
- The input records must have the
  [same number of partitions](#records-have-the-same-number-of-partitions)
  on both sides.
- Both sides of the join must have the
  [same partitioning strategy](#records-have-the-same-partitioning-strategy).

When your inputs are co-partitioned, records with the same key, from
both sides of the join, are delivered to the same stream task during
processing.

### Records Have the Same Key Schema

For a join to work, the keys from both sides must have the same SQL type.

For example, you can join a stream of user clicks that's keyed on a `STRING`
user id with a table of user profiles that's also keyed on a `STRING` user id.
Records with the exact same user id on both sides will be joined.

If the schema of the columns you wish to join on don't match, it may be possible
to `CAST` one side to match the other. For example, if one side of the join
had a `INT` userId column, and the other a `LONG`, then you may choose to cast
the `INT` side to a `LONG`:

```sql
    -- stream with INT userId
    CREATE STREAM clicks (userId INT KEY, url STRING) WITH(kafka_topic='clickstream', value_format='json');

    -- table with BIGINT id stored in the key:
    CREATE TABLE  users (id BIGINT PRIMARY KEY, fullName STRING) WITH(kafka_topic='users', value_format='json');
    
   -- Join utilising a CAST to convert the left sides join column to match the rights type.
   SELECT clicks.url, users.fullName FROM clicks JOIN users ON CAST(clicks.userId AS BIGINT) = users.id;
```

Tables created on top of existing Kafka topics, for example those created with
a `CREATE TABLE` statement, are keyed on the data held in the key of the records
in the Kafka topic. ksqlDB presents this data in the `PRIMARY KEY` column and expects
the data to be in the `KAFKA` format.

Tables created inside ksqlDB from other sources, for example those created with
a `CREATE TABLE AS SELECT` statement, will copy the key from their source(s)
unless there is an explicit `GROUP BY` or `JOIN` clause, which can change what the table
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

!!! important
      If the PARTITION BY expression evaluates to NULL, the resulting row is produced to a
      random partition. You many want to use [COALESCE](../syntax-reference#coalesce) to wrap
      the expression and convert any NULL values to a default value, for example,
      `PARTITION BY COALESCE(MY_UDF_THAT_MAY_FAIL(Col0), 0)`.

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
