---
layout: page
title: Upgrade ksqlDB
tagline: Upgrade on-premises ksqlDB 
description: Learn how to upgrade your on-premises ksqlDB deployments.
keywords: ksqldb, install, upgrade
---

## About backward compatibility

Past releases of KSQL were backward compatible. But there was a cost to this backward compatibility:
progress was slower and the code base incurred increased complexity. ksqlDB is a young product and
we want to move fast, so we have decided to choose speed of development over strong backward
compatibility guarantees for a few releases.

Until version 1.0 of ksqlDB, each minor release will potentially have breaking changes in it,
which means that you can't simply update the ksqlDB binaries and restart the server(s).

The data models and binary formats used within ksqlDB are in flux. This means data local to each
ksqlDB node and stored centrally within internal {{ site.ak }} topics may not be compatible with
the new version you're trying to deploy.

## Should I upgrade?

It's great that you're interested in trying out the new features and fixes that new versions of
ksqlDB bring. But before rushing off to upgrade all your ksqlDB clusters, ask yourself,
"Do I need to upgrade *this* cluster"?

If you're running ksqlDB in production, and you don't yet need the features or fixes the new version
brings, consider delaying any upgrade until either another release has features or fixes you
need, or until ksqlDB reaches version 1.0 and promises backward compatibility.

## How to upgrade

Upgrading a cluster involves leaving the old cluster running on the old version, bringing up a new
cluster on the new version, porting across your database schema, and finally thinking about your data.

### Port the database schema

To port your database schema from one cluster to another you need to recreate all the streams,
tables and types in the source cluster.

The recommended process is to use the [commandTopicConsumer](commandTopicConsumer.py)
Python script to dump the ksqlDB command topic. 

If you prefer to recover the schema manually, use the following steps.

!!! tip

    You can use the [SPOOL](../../developer-guide/ksqldb-reference/spool.md)
    command to capture the output of the commands you run in the CLI to a file.

1. Capture streams SQL:
  1. Run `list streams extended;` to list all of the streams.
  2. Grab the SQL statement that created each stream from the output, ignoring `KSQL_PROCESSING_LOG`.
2. Capture tables SQL:
  3. Run `list tables extended;` to list all of the tables.
  4. Grab the SQL statement that created each table from the output.
3. Capture custom types SQL:
  5. Run `list types;` to list all of the custom types.
  6. Convert the output into `CREATE TYPE <name> AS <schema>` syntax by grabbing the name from the
     first column and the schema from the second column of the output.
4. Order by dependency:  you'll now have the list of SQL statements to rebuild the schema, but they
   are not yet ordered in terms of dependencies. You will need to reorder the statements to ensure
   each statement come after any other statements it depends on.
5. Update the script to take into account any changes in syntax or functionality between the old
   and new clusters. The release notes can help here.  It can also be useful to have a test ksqlDB
   cluster, pointing to a different test Kafka cluster, where you can try running the script to get
   feedback on any errors.  Note: you may want to temporarily add `PARTITIONS=1` to the `WITH`
   clause of any `CREATE TABLE` or `CREATE STREAM` command, so that the command will run without
   requiring you to first create the necessary topics in the test Kafka cluster.
6. Stop the old cluster: if you do not do so then both the old and new cluster will be publishing
   to sink topics, resulting in undefined behavior.
7. Build the schema in the new instance. Now you have the SQL file you can run this against the
   new cluster to build a copy of the schema.  This is best achieved with the
   [RUN SCRIPT](../../developer-guide/ksqldb-reference/run-script.md) command, which takes a SQL
   file as an input.

### Rebuild state

Porting the database schema to the new cluster will cause ksqlDB to start processing data. As this
is a new cluster it will start processing all data from the start, i.e. it will likely be
processing data the old cluster has already processed.

**IMPORTANT**: while ksqlDB is processing historic data it will output historic results to sink
topics. Such historic results _may_ cause issues with downstream consumers of these data. The
historic results will be correctly timestamped, allowing well behaved consumers to correctly
process or ignore the historic results.

**NOTE**: source data that the old cluster processed may not longer be available in Kafka for the
new cluster to process, e.g. topics with limited retention.  It is therefore possible for the new
cluster to have different results to the old.

It is possible to monitor how far behind the processing is through JMX. Monitor the
`kafka.consumer/consumer-fetch-manager-metrics/<consumer-name>/records-lag-max` metrics to observe
the new nodes processing the historic data.

### Destroy the old cluster

Once you're happy with your new cluster you can destroy the old one using the
[terminate endpoint](https://github.com/confluentinc/ksql/blob/master/docs/developer-guide/ksqldb-rest-api/terminate-endpoint.md).
This will stop all processing and delete any internal topics in Kafka.

## Upgrade notes

### Upgrading from ksqlDB 0.7.0+ to 0.9.0

!!! important
    ksqlDB 0.8.0 is not backward compatible. Do not upgrade in-place.

The following changes in SQL syntax and functionality may mean SQL statements
that ran previously no longer run.

### Table PRIMARY KEYs

Tables now use `PRIMARY KEY` to define their primary key column rather than `KEY`.
Update your `CREATE TABLE` statements as required. For example, statements like
the this:

```sql
CREATE TABLE OUTPUT (ROWKEY INT KEY, V0 STRING, V1 DOUBLE) WITH (...);
```

Must be updated to:

```sql
CREATE TABLE OUTPUT (ROWKEY INT PRIMARY KEY, V0 STRING, V1 DOUBLE) WITH (...);
```

### Upgrading from ksqlDB 0.6.0 to 0.7.0

!!! important
    ksqlDB 0.7.0 is not backward compatible. Do not upgrade in-place.

The following changes in SQL syntax and functionality may mean SQL statements
that ran  previously no longer run.

### `PARTITION BY` and `GROUP BY` result schema changes:

Materialized views created with `CREATE TABLE AS SELECT` or `CREATE STREAM AS SELECT` that include a
`PARTITION BY` or `GROUP BY` on a single column may fail or result in a different result schema.

The statement may fail if the type of the single column is not a supported primitive type: `INT`,
`BIGINT`, `DOUBLE` or `STRING`. For example:

```sql
CREATE STREAM input (col0 ARRAY<STRING>) WITH (kafka_topic='input', value_format='json');
-- following will fail due to grouping by non-primitive key:
CREATE TABLE output AS SELECT count(1) FROM input GROUP BY col0;
```

**Workaround:** Change the statement to `CAST` the `GROUP BY` or `PARTITION BY` column to a `STRING`, for example

```sql
CREATE TABLE output AS SELECT count(1) FROM input GROUP BY CAST(col0 AS STRING);
```

The statement will result in a stream or table with a different schema if the single column has
supported primitive key other than `STRING`.  The type of the `ROWKEY` system column in the resulting
table or stream will match the type of the single column.  This can cause downstream statements to
fail.

```sql
CREATE STREAM input (col0 INT) WITH (kafka_topic='input', value_format='json');
-- schema of output has changed!
-- was: ROWKEY STRING KEY, COUNT BIGINT
-- now: ROWKEY INT KEY, COUNT BIGINT
CREATE TABLE output AS SELECT count(1) AS COUNT FROM input GROUP BY col0;
```

**Workaround 1:** Fix the downstream queries to use the new SQL type.
**Workaround 2:** Change the statement to `CAST` the `GROUP BY` or `PARTITION BY` column to a `STRING`.
This allows the query to operate as it did previously, for example

```sql
-- schema: ROWKEY STRING KEY, COUNT BIGINT
CREATE TABLE output AS SELECT count(1) AS COUNT FROM input GROUP BY CAST(col0 AS STRING);
```

### Joins may result in different schema or may fail

Some existing joins may now fail and others may see the the type of `ROWKEY` in the result schema
may have changed, due to primitive key support.

The statement may fail if the two sides of the join have a different SQL Type. For example:

```sql
CREATE STREAM L (ID INT) WITH (...);
CREATE TABLE R (ID BIGINT) WITH (...);

-- previously the following statement would have succeeded, but will now fail:
SELECT * FROM L JOIN R ON L.ID = R.ID;
```

The join  now fails because joins require the join keys to exactly match, which can not be the case
if they are a different SQL type.


**workaround:**: ksqlDB now supports arbitrary expressions in the join criteria, allowing you to `CAST`
either, or both, sides to the same SQL type. For example,

```sql
SELECT * FROM L JOIN R ON CAST(L.ID AS BIGINT) = R.ID;
```

The statement will result in a stream or table with a different schema if the join column has
supported primitive key other than `STRING`.  The type of the `ROWKEY` system column in the resulting
table or stream will match the type of the join column.  This can cause downstream statements to
fail.

```sql
CREATE STREAM L (ID INT, COUNT BIGINT) WITH (...);
CREATE TABLE R (ID INT, NAME STRING) WITH (...);

-- schema of output has changed!
-- was: ROWKEY STRING KEY, COUNT BIGINT, NAME STRING
-- now: ROWKEY INT KEY, COUNT BIGINT, NAME STRING
SELECT COUNT, NAME FROM L JOIN R ON L.ID = R.ID;
```

**Workaround 1:** Fix the downstream queries to use the new SQL type.
**Workaround 2:** Change the statement to `CAST` the join criteria to a `STRING`.
This allows the query to operate as it did previously, for example

```sql
-- schema: ROWKEY STRING KEY, COUNT BIGINT, NAME STRING
SELECT COUNT, NAME FROM L JOIN R ON CAST(L.ID AS STRING) = CAST(R.ID AS STRING);
```

### Incompatibility between `ROWKEY` type and `WITH(KEY)` type

`CREATE TABLE` and `CREATE STREAM` statements accept an optional `KEY` property in their `WITH` clause.
ksqlDB now requires the column identified in the `KEY` property to have the same SQL type as key of the
Kafka message, i.e. same type as `ROWKEY`. This makes sense, given the column identified in the `KEY`
property is expected to hold the _exact_ same data as `ROWKEY` if things are to work correctly.

For example, the statement below would previously have executed, but will now fail:

```sql
-- fails with error due to SQL type of `col0` not matching the type of `ROWKEY` column.
CREATE STREAM input (col0 INT) WITH (key='col0', kafka_topic='input', value_format='json');
```

**Workaround 1:** Update the statement to explicitly set the type of `ROWKEY`

If the key type matches the column type, e.g. in the example above the key is
an `INT`, then the statement can be updated to explicitly set `ROWKEY` type:

```sql
CREATE STREAM input (ROWKEY INT KEY, col0 INT) WITH (key='col0', kafka_topic='input', value_format='json');
	```

Note: in version 0.7 the key column must be called `ROWKEY`. This requirement will be removed by
[#3536](https://github.com/confluentinc/ksql/issues/3536).

**Workaround 2:** remove the `KEY` property from the `WITH` clause. Though this may mean ksqlDB needs to repartition
the data where previously it did not.

### Removal of `WindowStart()` and `WindowEnd()` UDAFs

These UDAFs allowed access to the window bounds in the projection of `GROUP BY` queries, for example:

```sql
SELECT ROWKEY, WindowStart(), WindowEnd(), COUNT() from input WINDOW SESSION (30 SECONDS) GROUP BY ROWKEY;
```

These window bound UDAFs have been removed in ksqlDB 0.7.

**workaround**: Please use the `WindowStart` and `WindowEnd` system columns instead, for example:

```sql
SELECT ROWKEY, WindowStart, WindowEnd, COUNT() from input WINDOW SESSION (30 SECONDS) GROUP BY ROWKEY;
```

### Windowed `ROWKEY` data change

Any query of a windowed source that uses `ROWKEY` in the SELECT projection will see the contents of
`ROWKEY` change, for example:

```sql
-- assuming `input` is a windowed stream:
CREATE STREAM output AS SELECT ROWKEY as KEY_COPY, ID, NAME From input WHERE COUNT > 100;
```

For previous versions of ksqlDB `KEY_COPY` would be of type `STRING` and would contain data in the
format `<key> : Window{start=<window-start>, end=<window-end>}`. From v0.7 onwards the type of
`KEY_COPY` will match the type and contents of `ROWKEY`.

**workaround**: if required, the statement can be updated to reconstruct the old string value by accessing
the window bounds using the `WINDOWSTART` and `WINDOWEND` system columns, for example:

```sql
CREATE STREAM output AS SELECT
   (CAST(ROWKEY AS STRING) + " : Window{start=" + CAST(WINDOWSTART AS STRING) + ", end=-}" ) as KEY_COPY,
   ID,
   NAME
   From input WHERE COUNT > 100;
```

### Change in array base index

Previous versions of ksqlDB used base-0 indexing when accessing array elements. For example:

```sql
CREATE STREAM input (ids ARRAY<BIGINT>) WITH (...);
-- access array using base-zero indexing:
SELECT ids[0] AS firstId, ids[1] AS secondId from input;
```

Starting from v0.7 ksqlDB more correctly uses base-one indexing.

**Workaround:** update the statements to use base-one indexing. For example:

```sql
SELECT ids[1] AS firstId, ids[2] AS secondId from input;
```

### Change in required order for `EMIT CHANGES` and `PARTITION BY`

Previous releases of ksqlDB required the `EMIT CHANGES` _before_ the `PARTITION BY`. For example:

```sql
SELECT * FROM input EMIT CHANGES PARTITION BY col0;
```

Starting from v0.7, ksqlDB requires the `PARTITION BY` to come before the `EMIT CHANGES`

**Workaround**: update the statement to reflect the new required order. For example:

```sql
SELECT * FROM input PARTITION BY col0 EMIT CHANGES;
```

### `ALL`, `WINDOWSTART` and `WINDOWEND` are now reserved identifiers

Any query using these identifiers will need to be changed to either use some other identifier, or
to quote them.