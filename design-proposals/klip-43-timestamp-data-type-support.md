# KLIP-43: TIMESTAMP Data Type Support

**Author**: @jzaralim | 
**Release Target**: 0.15 | 
**Status**: In Discussion | 
**Discussion**: https://github.com/confluentinc/ksql/pull/6649

**tl;dr:** _KSQL currently converts time data into long values. This KLIP introduces a TIMESTAMP
data type to handle time data instead._

## Motivation and background

With Connect integration, KSQL processes data from various sources and sinks. Most other databases
have a TIMESTAMP or DATETIME type, which KSQL currently converts into long values. This sometimes
causes problems when sinking into a database. Supporting TIMESTAMP types would make moving time data
between KSQL and other data sources/sinks smoother and less error-prone.

## What is in scope
### Required

* Add TIMESTAMP type to KSQL
* Serialization and de-serialization of TIMESTAMPs to Avro, JSON and Delimited formats
* Built-in functions to support TIMESTAMP usage

### Future enhancements

* Give window units (HOUR, DAY etc) a TIMESTAMP value
* Add DATE and TIME types
* UDFs such as DAY and HOUR

## What is not in scope
Changing the ROWTIME data type. We will eventually want this to happen, but that is a separate discussion.

## Public APIS

The TIMESTAMP data type will store a time without timezone information. It will be useable in any
place STRING or INTEGER could be used:
```roomsql
CREATE STREAM stream_name (col1 TIMESTAMP, COL2 STRING) AS ...
CREATE TABLE table_name (col1 STRUCT<field TIMESTAMP>) AS ...
```

## Design

### Serialization/Deserialization

TIMESTAMPs will be handled by the Java Date type within KSQL. The corresponding Kafka Connect type is
[org.apache.kafka.connect.data.Timestamp](https://kafka.apache.org/0100/javadoc/org/apache/kafka/connect/data/Timestamp.html).
They are represented as long types in Schema Registry, but also come with a tag indicating that it is a timestamp, so they should be distinguishable from long types when handling serialized values in KSQL.

#### Avro

Avro schemas represent timestamps as
```json
{
  "type": "long",
  "logicalType": "timestamp-millis"
}
```
The Avro deserializer KSQL uses supports this.

#### JSON/Delimited

Timestamps will get stored in JSON and CSV files as long values. The KSQL JSON and delimited deserializers
will be updated to parse timestamps.

### UDFs

The following UDFs should be updated to use the TIMESTAMP type instead of BIGINT:

* TIMESTAMPTOSTRING
* STRINGTOTIMESTAMP
* UNIX_TIMESTAMP

The following UDFs should also be added to support conversions between TIMESTAMP and BIGINT representations of time:

* TIMESTAMPTOBIGINT
* BIGINTTOTIMESTAMP

There are a few existing UDFs that deal with dates. These should be left as is until a DATE type is implemented:

* UNIX_DATE
* DATETOSTRING
* STRINGTODATE

### Casting/Converting

Since the above UDFs already handle conversions, casting between TIMESTAMP and STRING or BIGINT is
unnecessary. If DATE and TIME types are implemented, then the conversions between them and TIMESTAMP
will be as follows:

| From      | To        | Result                                                |
|---------- |:----------|:------------------------------------------------------|
| TIMESTAMP | DATE      | DATE type representing date portion of the TIMESTAMP. |
| TIMESTAMP | TIME      | TIME type representing time portion of the TIMESTAMP. |
| DATE      | TIMESTAMP | TIMESTAMP with time portion set to 00:00:00.          |
| TIME      | TIMESTAMP | TIMESTAMP with date portion set to Unix epoch.        |

### Arithmetic operations and comparisons

At the very least, we would want KSQL to be able to compare, add and subtract TIMESTAMPS.

[MySQL](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html) and [MariaDB](https://mariadb.com/kb/en/date-time-functions/)
have very complete lists of built-in arithmetic functions, while [PostgreSQL](https://www.postgresql.org/docs/9.0/functions-datetime.html)
defines behaviors for arithmetic operators.

Because functions are more well-defined, we should opt for built-in arithmetic functions
instead of using operators. The following functions are necessary:

* ADDTIME(timestamp1, timestamp2)
* SUBTIME(timestamp1, timestamp2)

As for comparisons, the following expressions should be supported:
```
time_stamp1 < time_stamp2
time_stamp1 > time_stamp2
time_stamp1 = time_stamp2
time_stamp1 BETWEEN time_stamp2 AND time_stamp3
```

Comparisons between TIMESTAMPS and other data types should not be allowed.

### Window units

It might make sense to give window units a timestamp representation so that they could be used as time intervals in TIMESTAMP arithmetic. For example,

```roomsql
SELECT ADDTIME(TIME, 1 DAY) FROM FOO;
```

Because windowing is handled separately from expression evaluation, doing this will not have any
impact on windows.

## Test plan

There should be an integration test with Kafka Connect and Schema Registry.

## LOEs and Delivery Milestones

This feature can be broken down into two milestones: implementing the TIMESTAMP type (1-2 weeks) and
adding all of the supporting UDFs (~1 week).

## Documentation Updates

There will need to be documentation on the following:

* Description of the TIMESTAMP data type
* TIMESTAMP usage in WHERE/GROUP/PARTITION clauses
* New and updated UDFs
* We might want to add this into one of the quick-starts

## Compatibility Implications

If a user creates a stream or table with a TIMESTAMP typed column, then that column will not be
(de)serializable in earlier versions. Besides that, there should be no other compatibility issues. 

## Security Implications

None
