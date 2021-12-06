---
layout: page
title: How to convert a changelog to a table 
tagline: Materialize a changelog stream into a table by using ksqlDB
description: Use ksqlDB to create a view of a changelog that reflects only the last change for each key
keywords: changelog, table, materialize
---

# How to convert a changelog to a table

## Context

You have a stream of events that represent a series of changes, known as a changelog. You want a view of the data that reflects only the last change for each key. Because ksqlDB represents change over time using tables, you need a way to convert your changelog into a table. This is broadly called *materializing* a changelog into a table.

## In action

```sql
CREATE TABLE t1 AS
    SELECT *
    FROM changelog
    EMIT CHANGES;
```

## Materializing a changelog

In ksqlDB, you derive new tables by aggregating other streams and tables. To create a table that reflects the latest values for each key, declare the `changelog` as a table.

Begin by telling ksqlDB to start all queries from the earliest point in each topic.

```sql
SET 'auto.offset.reset' = 'earliest';
```

Make a table `changelog` with four columns. `k` represents the key of the table. Rows with the same key represent information about the same entity. `v1`, `v2`, and `v3` are various value columns.

```sql
CREATE TABLE changelog (
    k VARCHAR PRIMARY KEY,
    v1 INT,
    v2 VARCHAR,
    v3 BOOLEAN
) WITH (
    kafka_topic = 'changelog',
    partitions = 1,
    value_format = 'JSON'
);
```

Insert some rows into the `changelog` table, repeating entries for some of the keys.

```sql
INSERT INTO changelog (
    k, v1, v2, v3
) VALUES (
    'k1', 0, 'a', true
);

INSERT INTO changelog (
    k, v1, v2, v3
) VALUES (
    'k2', 1, 'b', false
);

INSERT INTO changelog (
    k, v1, v2, v3
) VALUES (
    'k1', 2, 'c', false
);

INSERT INTO changelog (
    k, v1, v2, v3
) VALUES (
    'k3', 3, 'd', true
);

INSERT INTO changelog (
    k, v1, v2, v3
) VALUES (
    'k2', 4, 'e', true
);
```

Derive a table, `t1`, from table `changelog`.

```sql
CREATE TABLE t1 AS
    SELECT *
    FROM changelog
    EMIT CHANGES;
```

Run a pull query against each key. Notice how for each key, the columns reflect the last set of values that were inserted.

```sql
SELECT k, v1, v2, v3 FROM t1 WHERE k='k1';
+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+
|K                                                      |V1                                                     |V2                                                     |V3                                                     |
+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+
|k1                                                     |2                                                      |c                                                      |false                                                  |


SELECT k, v1, v2, v3 FROM t1 WHERE k='k2';
+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+
|K                                                      |V1                                                     |V2                                                     |V3                                                     |
+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+
|k2                                                     |4                                                      |e                                                      |true                                                   |


SELECT k, v1, v2, v3 FROM t1 WHERE k='k3';
+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+
|K                                                      |V1                                                     |V2                                                     |V3                                                     |
+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+
|k3                                                     |3                                                      |d                                                      |true                                                   |
```
You can also run a pull query to scan the entire table to view the last set of values that were inserted for every key.
```sql
SELECT * FROM t1;
+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+
|K                                                      |V1                                                     |V2                                                     |V3                                                     |
+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+-------------------------------------------------------+
|k1                                                     |2                                                      |c                                                      |false                                                  |
|k2                                                     |4                                                      |e                                                      |true                                                   |
|k3                                                     |3                                                      |d                                                      |true                                                   |
```
