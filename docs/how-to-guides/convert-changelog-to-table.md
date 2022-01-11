---
layout: page
title: How to convert a changelog to a table 
tagline: Materialize a changelog topic or a stream into a table by using ksqlDB
description: Use ksqlDB to create a view of a changelog that reflects only the last change for each key
keywords: changelog, table, materialize
---

# How to convert a changelog to a table

## Context

You have a topic or a stream of events that represent a series of changes, known as a changelog. You want a view of the data that reflects only the last change for each key. Because ksqlDB represents change over time using tables, you need a way to convert your changelog into a table. This is broadly called *materializing* a changelog into a table.

## Materializing a changelog topic

If you have a `changelog` topic, and you want view of the data that reflects the latest values for each key then simply create a table with the `changelog` topic using the `CREATE SOURCE TABLE` statement.

Let's say that you have the following data in your `changelog` topic where the first row is the record that has the earliest offset and the last row is the record that has the latest offset:
```sql
+------+------+------+------+
|K     |V1    |V2    |V3    |
+------+------+------+------+
|k1    |0     |a     |true  |
|k2    |1     |b     |false |
|k1    |2     |c     |false |
|k3    |3     |d     |true  |
|k2    |4     |e     |true  |
```
Note that there are 2 records for the keys `k1` and `k2` in the `changelog` topic.

Begin by telling ksqlDB to start all queries from the earliest point in each topic:

```sql
SET 'auto.offset.reset' = 'earliest';
```

Make a table `latest_view` with four columns. `k` represents the key of the table. Rows with the same key represent information about the same entity. `v1`, `v2`, and `v3` are various value columns.

```sql
CREATE SOURCE TABLE latest_view (
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

Now, you can view the latest values for each key in your `changelog` topic by issuing a pull query against the `latest_view` table that we just created above:
```sql
SELECT * FROM latest_view;
+------+------+------+------+
|K     |V1    |V2    |V3    |
+------+------+------+------+
|k1    |2     |c     |false |
|k2    |4     |e     |true  |
|k3    |3     |d     |true  |
```

Notice how for each key, the columns reflect the latest set of values.

If you just want to lookup the latest value for a particular key (let's assume `k2` in this case), then you can simply issue a pull query for that particular key:
```sql
SELECT * FROM latest WHERE K = 'k2';
+------+------+------+------+
|K     |V1    |V2    |V3    |
+------+------+------+------+
|k2    |4     |e     |true  |
```
You can learn more about pull queries here.
