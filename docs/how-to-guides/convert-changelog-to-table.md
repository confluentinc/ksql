---
layout: page
title: How to convert a changelog to a table 
tagline: Materialize a changelog topic or a stream into a table by using ksqlDB
description: Use ksqlDB to create a view of a changelog that reflects only the last change for each key
keywords: changelog, table, materialize
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/how-to-guides/convert-changelog-to-table.html';
</script>

# How to convert a changelog to a table

## Context

You have a topic or a stream of events that represent a series of changes, known as a changelog. You want a view of the data that reflects only the last change for each key. Because ksqlDB represents change over time using tables, you need a way to convert your changelog into a table. This is broadly called *materializing* a changelog into a table.

## Materializing a changelog topic

If you have a `changelog` topic, and you want a view of the data that reflects the latest values for each key then simply create a table with the `changelog` topic using the `CREATE SOURCE TABLE` statement.

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

Now, you can view the latest values for each key in your `changelog` topic by issuing a pull query against the `latest_view` table that you created above:
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

If you want to look up the latest value for a particular key, for example, `k2`, issue a pull query for that key:
```sql
SELECT * FROM latest WHERE k = 'k2';
+------+------+------+------+
|K     |V1    |V2    |V3    |
+------+------+------+------+
|k2    |4     |e     |true  |
```


## Materializing a changelog `STREAM`

The best way to materialize an input topic as a table is the `CREATE SOURCE TABLE` statement. If you already have a `STREAM`, you can also materialize it as a table with the `CREATE TABLE ... AS ...` statement. This is useful if you have a `STREAM` of events in ksqlDB that represents a series of changes called `changelog_stream`, and you want a view of the data that reflects the latest values for each key. 

Begin by telling ksqlDB to start all queries from the earliest point in each topic:
```sql
SET 'auto.offset.reset' = 'earliest';
```

Let's mimic adding a few records to the `changelog_stream` by using the `INSERT INTO` statement:
```sql
INSERT INTO changelog_stream (
    k, v1, v2, v3
) VALUES (
    'k1', 0, 'a', true
);

INSERT INTO changelog_stream (
    k, v1, v2, v3
) VALUES (
    'k2', 1, 'b', false
);

INSERT INTO changelog_stream (
    k, v1, v2, v3
) VALUES (
    'k1', 2, 'c', false
);

INSERT INTO changelog_stream (
    k, v1, v2, v3
) VALUES (
    'k3', 3, 'd', true
);

INSERT INTO changelog_stream (
    k, v1, v2, v3
) VALUES (
    'k2', 4, 'e', true
);
```

You can view the data in the `changelog_stream` by issuing a pull query:
```sql
SELECT * FROM changelog_stream;
+------+------+------+------+
|K     |V1    |V2    |V3    |
+------+------+------+------+
|k1    |0     |a     |true  |
|k2    |1     |b     |false |
|k1    |2     |c     |false |
|k3    |3     |d     |true  |
|k2    |4     |e     |true  |
Query terminated
```

To view the data that reflects only the last change for each key in `changelog_stream`, you can derive a table called `latest_events` with a `CREATE TABLE AS SELECT` statement in conjunction with the `LATEST_BY_OFFSET` aggregation:
```sql
CREATE TABLE latest_events AS
    SELECT k,
           LATEST_BY_OFFSET(v1) AS v1,
           LATEST_BY_OFFSET(v2) AS v2,
           LATEST_BY_OFFSET(v3) AS v3
    FROM changelog_stream
    GROUP BY k
    EMIT CHANGES;
```

Now, you can view the latest values for each key in your `changelog_stream` by issuing a pull query against the `latest_events` table that you created above:
```sql
SELECT * FROM latest_events;
+------+------+------+------+
|K     |V1    |V2    |V3    |
+------+------+------+------+
|k1    |2     |c     |false |
|k2    |4     |e     |true  |
|k3    |3     |d     |true  |
```

Notice how for each key, the columns reflect the latest set of values.
