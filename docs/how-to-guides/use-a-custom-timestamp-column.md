---
layout: page
title: How to use a custom timestamp column
tagline: Tell ksqlDB where to find the timestamp attribute within records
description: Use the timestamp attribute within events to do time-related processing with ksqlDB
keywords: timestamp, event time
---

# How to use a custom timestamp column

## Context

You have events that have a timestamp attribute. You want to do time-related processing over them and want ksqlDB to use those timestamps during processing. Because ksqlDB defaults to using the timestamp metadata of the underlying Kafka records, you need to tell ksqlDB where to find the timestamp attribute within the events. This is called using *event-time*.

## In action

```sql
CREATE STREAM s1 (
    k VARCHAR KEY,
    ts VARCHAR,
    v1 INT,
    v2 VARCHAR
) WITH (
    kafka_topic = 's1',
    partitions = 1,
    value_format = 'avro',
    timestamp = 'ts',                        -- the column to use as a timestamp
    timestamp_format = 'yyyy-MM-dd HH:mm:ss' -- the format to parse the timestamp
);
```

## Using event-time

Using event-time allows ksqlDB to handle out-of-order events during time-related processing. Set the `timestamp` property when creating a stream or table to denote which column to use as the timestamp. If the timestamp column is a string, also set the `timestamp_format` property to tell ksqlDB how to parse it.

Begin by telling ksqlDB to start all queries from the earliest point in each topic.

```sql
SET 'auto.offset.reset' = 'earliest';
```

Create a stream `s1` that has a timestamp column, `ts`. Notice that the `timestamp` property hasn't been set yet. This will make it easier to see how the functionality behaves later in this guide.

```sql
CREATE STREAM s1 (
    k VARCHAR KEY,
    ts VARCHAR,
    v1 INT,
    v2 VARCHAR
) WITH (
    kafka_topic = 's1',
    partitions = 1,
    value_format = 'avro'
);
```

Insert some rows into `s1`, setting the `ts` column to dates that are not "now".

```sql
INSERT INTO s1 (
    k, ts, v1, v2
) VALUES (
    'k1', '2020-05-04 01:00:00', 0, 'a'
);

INSERT INTO s1 (
    k, ts, v1, v2
) VALUES (
    'k2', '2020-05-04 02:00:00', 1, 'b'
);
```

Query the stream for its columns, including `ROWTIME`. `ROWTIME` is a system-column that ksqlDB reserves to track the timestamp of the event.

```sql
SELECT k,
       ROWTIME,
       TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss.SSS') AS rowtime_formatted,
       ts,
       v1,
       v2
FROM s1
EMIT CHANGES;
```

Your results should look similar to what is below with the exception of `ROWTIME` and `ROWTIME_FORMATTED`, which will mirror your wall clock. Because you didn't yet instruct ksqlDB to use event-time, `ROWTIME` is inherited from the underlying Kafka record. Kafka's default is to set the timestamp at which the record was produced to the topic.

```
+------------------------------------+------------------------------------+------------------------------------+------------------------------------+------------------------------------+------------------------------------+
|K                                   |ROWTIME                             |ROWTIME_FORMATTED                   |TS                                  |V1                                  |V2                                  |
+------------------------------------+------------------------------------+------------------------------------+------------------------------------+------------------------------------+------------------------------------+
|k1                                  |1589564380616                       |2020-05-15 17:39:40.616             |2020-05-04 01:00:00                 |0                                   |a                                   |
|k2                                  |1589564380731                       |2020-05-15 17:39:40.731             |2020-05-04 02:00:00                 |1                                   |b                                   |
```

Derive a new stream, `s2`, from `s1` and tell ksqlDB to use event-time. Set the `timestamp` property to the `ts` column.

```sql
CREATE STREAM S2 WITH (
    timestamp = 'ts',
    timestamp_format = 'yyyy-MM-dd HH:mm:ss'
)   AS
    SELECT *
    FROM s1
    EMIT CHANGES;
```

Now compare the timestamps again. This time, notice that `ROWTIME` has been set to the same value as `ts`. `s2` is now using event-time.

```sql
SELECT k,
       ROWTIME,
       TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss.SSS') AS rowtime_formatted,
       ts,
       v1,
       v2
FROM s2
EMIT CHANGES;
```

The query should return the following results.

```
+------------------------------------+------------------------------------+------------------------------------+------------------------------------+------------------------------------+------------------------------------+
|K                                   |ROWTIME                             |ROWTIME_FORMATTED                   |TS                                  |V1                                  |V2                                  |
+------------------------------------+------------------------------------+------------------------------------+------------------------------------+------------------------------------+------------------------------------+
|k1                                  |1588554000000                       |2020-05-04 01:00:00.000             |2020-05-04 01:00:00                 |0                                   |a                                   |
|k2                                  |1588557600000                       |2020-05-04 02:00:00.000             |2020-05-04 02:00:00                 |1                                   |b                                   |
```

Any new streams or tables derived from `s2` will continue to have their timestamp set to `ts` unless an operation instructs otherwise.


## Timestamps on base streams/tables

Not only can you change the timestamp to use as you derive new streams and tables, you can also set it on base ones, too. Simply set the `timestamp` and `timestamp_format` properties on the `WITH` clause.

```sql
CREATE STREAM s3 (
    k VARCHAR KEY,
    ts VARCHAR,
    v1 INT
) WITH (
    kafka_topic = 's3',
    partitions = 1,
    value_format = 'avro',
    timestamp = 'ts',
    timestamp_format = 'yyyy-MM-dd HH:mm:ss'
);
```

Note that the underlying timestamp metadata for the Kafka records in topic `s3` are **not** modified. ksqlDB has merely marked that any derived streams or tables from `s3` should use the value of `ts` for `ROWTIME`.

## Timestamps as long values

You can use timestamps that are represented as milliseconds since the Unix epoch, too.

Create a stream `s4` with a timestamp column of type `BIGINT`. Because the timestamp is a number, ksqlDB doesn't need to know how to parse its timestamp format — it can interpret it directly as milliseconds since the Unix epoch. This means you can omit the `timestamp_format` property.

```sql
CREATE STREAM s4 (
    k VARCHAR KEY,
    ts BIGINT,
    v1 INT,
    v2 VARCHAR
) WITH (
    kafka_topic = 's4',
    partitions = 1,
    value_format = 'avro',
    timestamp = 'ts'
);
```

Insert some rows with millisecond timestamps.

```sql
INSERT INTO s4 (
    k, ts, v1, v2
) VALUES (
    'k1', 1562634000000, 0, 'a'
);

INSERT INTO s4 (
    k, ts, v1, v2
) VALUES (
    'k2', 1588509000000, 1, 'b'
);

INSERT INTO s4 (
    k, ts, v1, v2
) VALUES (
    'k3', 1588736700000, 2, 'c'
);
```

And run a similar query as above. Remember to set `auto.offset.reset` to `earliest` if you haven't yet.

```sql
SELECT k,
       ROWTIME,
       ts,
       TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss.SSS') AS rowtime_formatted,
       TIMESTAMPTOSTRING(ts, 'yyyy-MM-dd HH:mm:ss.SSS') AS ts_formatted,
       v1,
       v2
FROM s4
EMIT CHANGES;
```

The query should return the following results.

```
+------------------------------+------------------------------+------------------------------+------------------------------+------------------------------+------------------------------+------------------------------+
|K                             |ROWTIME                       |TS                            |ROWTIME_FORMATTED             |TS_FORMATTED                  |V1                            |V2                            |
+------------------------------+------------------------------+------------------------------+------------------------------+------------------------------+------------------------------+------------------------------+
|k1                            |1562634000000                 |1562634000000                 |2019-07-09 01:00:00.000       |2019-07-09 01:00:00.000       |0                             |a                             |
|k2                            |1588509000000                 |1588509000000                 |2020-05-03 12:30:00.000       |2020-05-03 12:30:00.000       |1                             |b                             |
|k3                            |1588736700000                 |1588736700000                 |2020-05-06 03:45:00.000       |2020-05-06 03:45:00.000       |2                             |c                             |
```


## Timestamps represented by TIMESTAMP columns

Similar to the previous section, you can also use columns of type `TIMESTAMP`. These columns require data to be in `yyyy-mm-ddThh:mm:ss[.S]` format, so there is no need to provide the `timestamp_format` property.

Create a stream `s5` with a column `ts` of type `TIMESTAMP`.

```sql
CREATE STREAM s5 (
    k VARCHAR KEY,
    ts TIMESTAMP,
    v1 INT,
    v2 VARCHAR
) WITH (
    kafka_topic = 's5',
    partitions = 1,
    value_format = 'avro',
    timestamp = 'ts'
);
```

Insert some rows with timestamps.


```sql
INSERT INTO s5 (
    k, ts, v1, v2
) VALUES (
    'k1', '2019-07-09T01:00', 0, 'a'
);

INSERT INTO s5 (
    k, ts, v1, v2
) VALUES (
    'k2', '2020-05-03T12:30:00', 1, 'b'
);

INSERT INTO s5 (
    k, ts, v1, v2
) VALUES (
    'k3', '2020-05-06T03:45', 2, 'c'
);
```

And run the following query. Remember to set `auto.offset.reset` to `earliest` if you haven't yet.


```sql
SELECT k,
       ROWTIME,
       ts,
       v1,
       v2
FROM s5
EMIT CHANGES;
```

The query should return the following results.

```sql
+------------------------------+------------------------------+------------------------------+------------------------------+------------------------------+
|K                             |ROWTIME                       |TS                            |V1                            |V2                            |
+------------------------------+------------------------------+------------------------------+------------------------------+------------------------------+
|k1                            |1562634000000                 |2019-07-09T01:00:00.000       |0                             |a                             |
|k2                            |1588509000000                 |2020-05-03T12:30:00.000       |1                             |b                             |
|k3                            |1588736700000                 |2020-05-06T03:45:00.000       |2                             |c                             |
```
