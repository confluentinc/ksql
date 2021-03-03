---
layout: page
title: How to convert a changelog to a table 
tagline: Materialize a changelog stream into a table by using ksqlDB
description: Use ksqlDB to create a view of a changelog that reflects only the last change for each key
keywords: changelog, table, materialize
---

# How to convert a changelog to a table

## Context

You have a stream of events that represent a series of changes, known as a changelog. You want a view of the data that reflects only the last change for each key. Because ksqlDB represents change over time using tables, you need a way to convert your changelog stream into a table. This is broadly called *materializing* a changelog stream into a table.

## In action

```sql
CREATE TABLE t1 AS
    SELECT k,
           LATEST_BY_OFFSET(v1) AS v1,
           LATEST_BY_OFFSET(v2) AS v2,
           LATEST_BY_OFFSET(v3) AS v3
    FROM s1
    GROUP BY k
    EMIT CHANGES;
```

## Materializing a changelog stream

In ksqlDB, you derive new tables by aggregating other streams and tables. To create a table that reflects the latest values for each key, use the `LATEST_BY_OFFSET` aggregation.

Begin by telling ksqlDB to start all queries from the earliest point in each topic.

```sql
SET 'auto.offset.reset' = 'earliest';
```

Make a stream `s1` with four columns. `k` represents the key of the table. Rows with the same key represent information about the same entity. `v1`, `v2`, and `v3` are various value columns.

```sql
CREATE STREAM s1 (
    k VARCHAR KEY,
    v1 INT,
    v2 VARCHAR,
    v3 BOOLEAN
) WITH (
    kafka_topic = 's1',
    partitions = 1,
    value_format = 'avro'
);
```

Insert some rows into `s1`, repeating entries for some of the keys.

```sql
INSERT INTO s1 (
    k, v1, v2, v3
) VALUES (
    'k1', 0, 'a', true
);

INSERT INTO s1 (
    k, v1, v2, v3
) VALUES (
    'k2', 1, 'b', false
);

INSERT INTO s1 (
    k, v1, v2, v3
) VALUES (
    'k1', 2, 'c', false
);

INSERT INTO s1 (
    k, v1, v2, v3
) VALUES (
    'k3', 3, 'd', true
);

INSERT INTO s1 (
    k, v1, v2, v3
) VALUES (
    'k2', 4, 'e', true
);
```

Derive a table, `t1`, from stream `s1`. When you create a table, the columns in the `SELECT` clause must either be the column that you're grouping by or columns with an aggregation function applied. The `LATEST_BY_OFFSET` aggregation allows you to select any column and retains only the last value it receives, where "last" is in terms of offsets.

```sql
CREATE TABLE t1 AS
    SELECT k,
           LATEST_BY_OFFSET(v1) AS v1,
           LATEST_BY_OFFSET(v2) AS v2,
           LATEST_BY_OFFSET(v3) AS v3
    FROM s1
    GROUP BY k
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
