---
layout: page
title: Time units and formats
tagline: Valid time units and formats in ksqlDB
description: Use built-in formats for time-based operations, like windowing, in ksqlDB
keywords: time, datetime, timestamp, format, window
---

### Time units

The following list shows valid time units for the `SIZE`, `ADVANCE BY`,
`SESSION`, and `WITHIN` clauses, or to pass as time unit parameters in functions.

-   `DAYS`
-   `HOURS`
-   `MINUTES`
-   `SECONDS`
-   `MILLISECONDS`

For more information, see
[Windows in SQL Queries](/concepts/time-and-windows-in-ksqldb-queries#windows-in-sql-queries).

### Timestamp formats

!!! important
      This section refers to timestamps as a field in records. For information
      on the TIMESTAMP data type, see [Timestamp types](data-types.md).

Time-based operations, like windowing, process records according to the
timestamp in `ROWTIME`. By default, the implicit `ROWTIME` pseudo column is the
timestamp of a message in a Kafka topic. Timestamps have an accuracy of
one millisecond.

Use the TIMESTAMP property to override `ROWTIME` with the contents of
the specified column. Define the format of a record's timestamp by
using the TIMESTAMP_FORMAT property.

If you use the TIMESTAMP property but don't set TIMESTAMP_FORMAT, ksqlDB
assumes that the timestamp field is either a `bigint` or a `timestamp`.
If you set TIMESTAMP_FORMAT, the TIMESTAMP field must be of type `varchar`
and have a format that the `DateTimeFormatter` Java class can parse.

If your timestamp format has embedded single quotes, you can escape them
by using two successive single quotes, `''`. For example, to escape
`'T'`, write `''T''`. The following examples show how to escape the `'`
character in SQL statements.

```sql
-- Example timestamp format: yyyy-MM-dd'T'HH:mm:ssX
CREATE STREAM TEST (id BIGINT KEY, event_timestamp VARCHAR)
  WITH (
    kafka_topic='test_topic',
    value_format='JSON',
    timestamp='event_timestamp',
    timestamp_format='yyyy-MM-dd''T''HH:mm:ssX'
  );

-- Example timestamp format: yyyy.MM.dd G 'at' HH:mm:ss z
CREATE STREAM TEST (id BIGINT KEY, event_timestamp VARCHAR)
  WITH (
    kafka_topic='test_topic',
    value_format='JSON',
    timestamp='event_timestamp',
    timestamp_format='yyyy.MM.dd G ''at'' HH:mm:ss z'
  );

-- Example timestamp format: hh 'o'clock' a, zzzz
CREATE STREAM TEST (id BIGINT KEY, event_timestamp VARCHAR)
  WITH (
    kafka_topic='test_topic',
    value_format='JSON',
    timestamp='event_timestamp',
    timestamp_format='hh ''o''clock'' a, zzzz'
  );
```

For more information on timestamp formats, see
[DateTimeFormatter](https://cnfl.io/java-dtf).
