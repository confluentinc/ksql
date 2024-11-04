---
layout: page
title: Upgrade ksqlDB
tagline: Upgrade on-premises ksqlDB 
description: Learn how to upgrade your on-premises ksqlDB deployments.
keywords: ksqldb, install, upgrade
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/operate-and-deploy/installation/upgrading.html';
</script>

## About backward compatibility

Most releases of ksqlDB are backward compatible. But backward compatibility comes at a cost:
progress is slower and the code base incurs increased complexity. ksqlDB is a young product and
we want to move fast, so we have decided to choose speed of development over strong backward
compatibility guarantees.

Until version 1.0 of ksqlDB, each minor release will potentially have breaking changes in it,
which may mean that you can't simply update the ksqlDB binaries and restart the server(s).

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

When possible, ksqlDB maintains runtime compatibility between versions, which means you can upgrade
in-place by stopping your ksqlDB servers and restarting them with the new version, and your existing
query pipelines will resume from where they left off. New query statements may need to be
adjusted for syntax changes required by the new version.

However, until ksqlDB 1.0, some ksqlDB versions may not support in-place upgrades. In these situations,
upgrading a cluster involves leaving the old cluster running on the old version, bringing up a new
cluster on the new version, porting across your database schema, and finally thinking about your data.
Read on for details.


### In-Place upgrade

Follow these steps to do an in-place upgrade of ksqlDB.

1. Download the [new ksqlDB version](https://docs.confluent.io/platform/current/installation/installing_cp/zip-tar.html#get-the-software) to upgrade to.
2. Set it up with all required configs. For the vast majority of upgrades, you may simply copy existing configs over to the new version.
3. Copy source UDFs and recompile against the new ksqlDB version, to accommodate any changes to the UDF framework.
4. Stop the ksqlDB process running the current version and start the newer version with the latest ksqlDB config file, which you created in Step 2.
5. ksqlDB starts executing queries and rebuilds all states, using the command topic, because the value of `ksql.service.id` is the same as the previous version. ksqlDB Server with the latest version is up and running.


### Troubleshooting an Upgrade

1. ksqlDB Server fails to start with an error message like "KsqlException: UdfFactory not compatible with existing factory. function: <FUNCTION_NAME> existing":  
This error occurs when the new ksqlDB version introduces a built-in function that has the same name as yours, creating a conflict with your UDF. Create a new UDF jar by renaming or eliminating the conflicting function(s), and restart ksqlDB with the new UDF jar.

2. DESCRIBE EXTENDED command doesn't work: 
Usually, this happens when the ksqlDB CLI and ksqlDB Server versions are different. You can create an alias for ksqlDB CLI to make sure to change it to a newer version. You should ensure that the ksqlDB CLI and Server versions are the same. In "<Home-Folder-Path>/.bashrc" file, you can create an alias for logging into ksqlDB cli. 
Eg : 
(i) Open .bashrc file
(ii) Create alias using command : "alias ksqldb='cd <ksqlDB bin folder location> && ./ksql http://<host-ip>:8088'" and save the changes. Now you can type "ksqldb" on shell and can login into ksqlDB.



### Steps to follow when "In-Place" upgrade is not supported

#### Port the database schema

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
4. Order by dependency: you'll now have the list of SQL statements to rebuild the schema, but they
   are not yet ordered in terms of dependencies. You will need to reorder the statements to ensure
   each statement comes after any other statements it depends on.
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

#### Rebuild state

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

#### Destroy the old cluster

Once you're happy with your new cluster you can destroy the old one using the
[terminate endpoint](https://github.com/confluentinc/ksql/blob/master/docs/developer-guide/ksqldb-rest-api/terminate-endpoint.md).
This will stop all processing and delete any internal topics in Kafka.

## Upgrade notes

### Upgrading from ksqlDB 0.14.0 to {{ site.ksqldbversion }}

In-place upgrades are supported from ksqlDB 0.14.0 to {{ site.ksqldbversion }}.
See the [changelog](https://github.com/confluentinc/ksql/blob/master/CHANGELOG.md)
for potential breaking changes that may affect the behavior or required syntax
for new queries.

#### Join statements

The GRACE PERIOD clause is now supported in stream-stream joins since 0.20.0. However, when set
explicitly, the grace period now defines when the left/outer non-joined results are emitted.
Contrary to the old syntax (without GRACE PERIOD) that emits left/outer non-joined results eagerly,
which may cause "spurious" result records.

The use of GRACE PERIOD is highly recommended now that it is supported. It helps to reduce high
disk usage when using small grace period values (default 24 hours if not set) and also provides
better semantics for left/outer joins.

### Upgrading from ksqlDB 0.10.0 to 0.14.0

In-place upgrades are supported from ksqlDB 0.10.0 to 0.14.0.
See the [changelog](https://github.com/confluentinc/ksql/blob/master/CHANGELOG.md)
for potential breaking changes that may affect the behavior or required syntax
for new queries.

### Upgrading from ksqlDB 0.9.0 to 0.10.0

In-place upgrades are supported from ksqlDB 0.9.0 to 0.10.0. However, in-place upgrades
from pre-0.7.0 versions to 0.10.0 are not supported, as ksqlDB 0.7.0 is not backward compatible.
Do not upgrade in place from a pre-0.7.0 version to 0.10.0.

The following changes in SQL syntax and functionality may mean SQL statements
that ran previously no longer run.

#### Any key name

Statements containing PARTITION BY, GROUP BY, or JOIN clauses now produce different output schemas.

For PARTITION BY and GROUP BY statements, the name of the key column in the result is determined by the PARTITION BY or GROUP BY clause:
1. Where the partitioning or grouping is a single column reference, then the key column has the same name as this column. For example:

```sql
-- OUTPUT will have a key column called X;
CREATE STREAM OUTPUT AS
  SELECT * 
  FROM INPUT 
  GROUP BY X;
```
2. Where the partitioning or grouping is a single struct field, then the key column has the same name as the field. For example:

```sql
-- OUTPUT will have a key column called FIELD1;
CREATE STREAM OUTPUT AS
  SELECT * 
  FROM INPUT 
  GROUP BY X->field1;
```
3. Otherwise, the key column name is system-generated and has the form `KSQL_COL_n`, where `n` is a positive integer.

In all cases, except where grouping by more than one column, you can set the new key column's name by defining an alias in the projection. For example:

```sql
-- OUTPUT will have a key column named ID.
CREATE TABLE OUTPUT AS
  SELECT 
    USERID AS ID, 
    COUNT(*) 
  FROM USERS 
  GROUP BY ID;
```
For groupings of multiple expressions, you can't provide a name for the system-generated key column.
However, a work around is to combine the grouping columns yourself, which does enable you to provide an alias:

```sql
-- products_by_sub_cat will have a key column named COMPOSITEKEY:
CREATE TABLE products_by_sub_cat AS
  SELECT 
    categoryId + ‘§’ + subCategoryId AS compositeKey
    SUM(quantity) as totalQty  
  FROM purchases
  GROUP BY CAST(categoryId AS STRING) + ‘§’ + CAST(subCategoryId AS STRING);

```

For JOIN statements, the name of the key column in the result is determined by the join criteria.
1. For INNER and LEFT OUTER joins where the join criteria contain at least one column reference, the key column is named based on the left-most source whose join criteria is a column reference. For example:

```sql
-- OUTPUT will have a key column named I2_ID.
CREATE TABLE OUTPUT AS
  SELECT * 
  FROM I1 
    JOIN I2 ON abs(I1.ID) = I2.ID JOIN I3 ON I2.ID = I3.ID;
```
The key column can be given a new name, if required, by defining an alias in the projection. For example:
```sql
-- OUTPUT will have a key column named ID.
CREATE TABLE OUTPUT AS
  SELECT 
    I2.ID AS ID, 
    I1.V0, 
    I2.V0, 
    I3.V0 
  FROM I1 
    JOIN I2 ON abs(I1.ID) = I2.ID 
    JOIN I3 ON I2.ID = I3.ID;
```
2. For FULL OUTER joins and other joins where the join criteria are not on column references, the key column in the output is not equivalent to any column from any source. The key column has a system-generated name in the form `KSQL_COL_n`, where `n` is a positive integer. For example:

```sql
-- OUTPUT will have a key column named KSQL_COL_0, or similar.
CREATE TABLE OUTPUT AS
  SELECT * 
  FROM I1 
    FULL OUTER JOIN I2 ON I1.ID = I2.ID;
```
The key column can be given a new name, if required, by defining an alias in the projection. A new UDF has been introduced to help define the alias called `JOINKEY`. It takes the join criteria as its parameters. For example:
```sql
-- OUTPUT will have a key column named ID.
CREATE TABLE OUTPUT AS
  SELECT 
    JOINKEY(I1.ID, I2.ID) AS ID, 
    I1.V0, 
    I2.V0 
  FROM I1 
    FULL OUTER JOIN I2 ON I1.ID = I2.ID;
```
`JOINKEY` will be deprecated in a future release of ksqlDB once multiple key columns are supported.

#### Explicit keys

`CREATE TABLE` statements will now fail if the `PRIMARY KEY` column is not provided.

For example, a statement such as:

```sql
CREATE TABLE FOO (
    name STRING
  ) WITH (
    kafka_topic='foo', 
    value_format='json'
  );
```

Will need to be updated to include the definition of the PRIMARY KEY, for example:

```sql
CREATE TABLE FOO (
    ID STRING PRIMARY KEY, 
    name STRING
  ) WITH (
    kafka_topic='foo', 
    value_format='json'
  );
```

If using schema inference, i.e. loading the value columns of the topic from the Schema Registry, the primary key can be provided as a partial schema, for example:

```sql
-- FOO will have value columns loaded from the Schema Registry
CREATE TABLE FOO (
    ID INT PRIMARY KEY
  ) WITH (
    kafka_topic='foo', 
    value_format='avro'
  );
```

`CREATE STREAM` statements that do not define a `KEY` column no longer have an implicit `ROWKEY` key column.

For example:

```sql
CREATE STREAM BAR (
    NAME STRING
  ) WITH (...);
```

Previously, the above statement would have resulted in a stream with two columns: `ROWKEY STRING KEY` and `NAME STRING`.

With this change, the above statement results in a stream with only the `NAME STRING` column.

Streams with no KEY column are serialized to Kafka topics with a `null` key.

#### Key columns required in projection

A statement that creates a materialized view must include the key columns in the projection. For example:

```sql
CREATE TABLE OUTPUT AS
   SELECT 
      productId,  // <-- key column in projection
      SUM(quantity) as unitsSold
   FROM sales
   GROUP BY productId;
```

The key column `productId` is required in the projection. In previous versions of ksqlDB, the presence

of `productId` in the projection would have placed a _copy_ of the data into the value of the underlying 
Kafka topic's record.  But starting in version 0.10.0, the projection must include the key columns, and ksqlDB stores these columns

in the _key_ of the underlying Kafka record.  Optionally, you may provide an alias for 

the key column(s). 

```sql
CREATE TABLE OUTPUT AS
   SELECT 
      productId as id,  // <-- aliased key column
      SUM(quantity) as unitsSold
   FROM sales
   GROUP BY productId;
```

If you need a copy of the key column in the Kafka record's value, use the 

[AS_VALUE](/developer-guide/ksqldb-reference/scalar-functions#as_value) function to indicate this
to ksqlDB. For example, the following statement produces an output inline with the previous version of ksqlDB

for the above example materialized view:

```sql
CREATE TABLE OUTPUT AS
   SELECT 
      productId as ROWKEY,              // <-- key column named ROWKEY
      AS_VALUE(productId) as productId, // <-- productId copied into value
      SUM(quantity) as unitsSold
   FROM sales
   GROUP BY productId;
```

### WITH(KEY) syntax removed

In previous versions, all key columns were called `ROWKEY`. To enable using a more

user-friendly name for the key column in queries, it was possible

to supply an alias for the key column in the WITH clause, for example:

```sql
CREATE TABLE INPUT (
    ROWKEY INT PRIMARY KEY, 
    ID INT, 
    V0 STRING
  ) WITH (
    key='ID', 
    ...
  );
```

With the previous query, the `ID` column can be used as an alias for `ROWKEY`.
This approach required the Kafka message value to contain an exact copy of the key.

[KLIP-24](https://github.com/confluentinc/ksql/blob/master/design-proposals/klip-24-key-column-semantics-in-queries.md)
removed the restriction that key columns must be named `ROWKEY`, negating the need for the `WITH(KEY)`
syntax, which has been removed. Also, this change removed the requirement for
the Kafka message value to contain an exact copy of the key.

Update your queries by removing the `KEY` from the `WITH` clause and naming

your `KEY` and `PRIMARY KEY` columns appropriately. For example, the previous
CREATE TABLE statement can now be rewritten as:

```sql
CREATE TABLE INPUT (
    ID INT PRIMARY KEY, 
    V0 STRING
  ) WITH (...);
```

Unless the value format is `DELIMITED`, which means the value columns are
*order dependent*, so dropping the `ID` value column would result in a

deserialization error or the wrong values being loaded. If you're using
`DELIMITED`, consider rewriting as:

```sql
CREATE TABLE INPUT (
    ID INT PRIMARY KEY, 
    ignoreMe INT, 
    V0 STRING
  ) WITH (...);
```

### Upgrading from ksqlDB 0.8.0 to 0.9.0

In-place upgrades are supported from ksqlDB 0.8.0 to 0.9.0. However, in-place upgrades
from pre-0.7.0 versions to 0.9.0 are not supported, as ksqlDB 0.7.0 is not backward compatible.
Do not upgrade in place from a pre-0.7.0 version to 0.9.0.

The following changes in SQL syntax and functionality may mean SQL statements
that ran previously no longer run.

### Table PRIMARY KEYs

Tables now use `PRIMARY KEY` to define their primary key column rather than `KEY`.
Update your `CREATE TABLE` statements as required. For example, statements like
this:

```sql
CREATE TABLE OUTPUT (ROWKEY INT KEY, V0 STRING, V1 DOUBLE) WITH (...);
```

Must be updated to:

```sql
CREATE TABLE OUTPUT (ROWKEY INT PRIMARY KEY, V0 STRING, V1 DOUBLE) WITH (...);
```

### Upgrading from ksqlDB 0.7.0 to 0.8.0

In-place upgrades are supported from ksqlDB 0.7.0 to 0.8.0.
See the [changelog](https://github.com/confluentinc/ksql/blob/master/CHANGELOG.md)
for bug fixes and other changes.

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
a supported primitive key other than `STRING`.  The type of the `ROWKEY` system column in the resulting
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

Some existing joins may now fail and others may see that the type of `ROWKEY` in the result schema
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
a supported primitive key other than `STRING`.  The type of the `ROWKEY` system column in the resulting
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
