# KLIP-43: TIMESTAMP Data Type Support

**Author**: @jzaralim | 
**Release Target**: 0.17,0; 6.1.0 | 
**Status**: _Merged_ | 
**Discussion**: https://github.com/confluentinc/ksql/pull/6649

**tl;dr:** _Add support for TIMESTAMP column types in ksqlDB. This will allow users to easily migrate
time data from other databases without having to convert column types, as well as simplify time data
manipulation_

## Motivation and background

With Connect integration, KSQL processes data from various databases. Most other databases
have a TIMESTAMP or DATETIME type, which KSQL currently converts to BIGINT. This makes sinking to
databases that don't implicitly cast long values to timestamps much more complicated - the user would
have to set up their connector to convert these columns back to TIMESTAMP. Supporting TIMESTAMP types
would make moving time data between KSQL and other data sources/sinks smoother and less error-prone.

Adding a TIMESTAMP type also simplifies time data manipulation. For example, currently
if a user wants to extract the month of a timestamp, they would either have to parse a string or do a
lot of math. Having time data in a dedicated data type allows for a lot of new UDFs.

## What is in scope

* Add TIMESTAMP type to KSQL
* Support TIMESTAMP arithmetic and comparisons
* Allow window units (HOUR, DAY etc) to be used in timestamp arithmetic
* Support TIMESTAMP usage in STRUCT, MAP and ARRAY
* Serialization and de-serialization of TIMESTAMPs to Avro, JSON, Protobuf and Delimited formats
* New UDFs to support the TIMESTAMP type, as well as deprecating old TIMESTAMP functions
* Casting TIMESTAMP to and from STRING

## What is not in scope
* Changing the ROWTIME data type. We will eventually want this to happen, but that is a separate
discussion.
* Support for dates before Unix epoch - this is not supported by Kafka. There is a [KIP](https://cwiki.apache.org/confluence/display/KAFKA/KIP-228+Negative+record+timestamp+support)
for negative timestamps, but until that is implemented, KSQL can only support positive timestamps.
* DATE and TIME types - because TIMESTAMPs represent a point in time, DATE and TIME types
would be useful if a user wants to represent a less specific time. These would be added after
TIMESTAMP is implemented though.
* TIMESTAMP types that store timezone information (TIMESTAMP_TZ) - Several other databases support
this. This is something that's useful to add in the future. 

## Public APIS

The TIMESTAMP data type will store a point in time after Unix epoch without timezone information.
The syntax is as follows:

```roomsql
CREATE STREAM stream_name (time TIMESTAMP, COL2 STRING) AS ...
CREATE TABLE table_name (col1 STRUCT<field TIMESTAMP>) AS ...
```

TIMESTAMPS will be displayed in console as strings in ISO-8601 form with millisecond precision:

```roomsql
> SELECT time FROM stream_name EMIT CHANGES;

+------------------------+
|time                    |
+------------------------+
|1994-11-05T13:15:30.112 |
```

TIMESTAMPS can be represented by ISO-8601 date strings:

```roomsql
INSERT INTO stream_name VALUES ("1994-11-05T13:15:30");
```

Date strings with time zones will be converted to UTC.

## Design

### Serialization/Deserialization

TIMESTAMPs will be handled by the `java.sql.Timestamp` class in UTC within KSQL. The corresponding Kafka Connect type is
[org.apache.kafka.connect.data.Timestamp](https://kafka.apache.org/0100/javadoc/org/apache/kafka/connect/data/Timestamp.html).
They are represented as long types in Schema Registry, but also come with a tag indicating that it
is a timestamp, so they should be distinguishable from long types when handling serialized values in KSQL.

#### Avro

Avro schemas represent timestamps as
```json
{
  "type": "long",
  "logicalType": "timestamp-millis"
}
```
The Avro deserializer KSQL uses supports this.

#### Protobuf
Protobuf 3 has a Timestamp type. The Protouf deserializer KSQL uses supports this.

#### JSON/Delimited

Timestamps will get stored in JSON and CSV files in milliseconds as long values. The KSQL JSON and delimited deserializers
will be updated to parse timestamps.

### UDFs

The following UDFs should be deprecated:

* TIMESTAMPTOSTRING
* STRINGTOTIMESTAMP

These functions will still be available so that existing queries using these functions won't break,
but the documentation will state that they are deprecated and will direct users towards using the
TIMESTAMP type and the new functions.

The following functions will be added:
* `PARSE_TIMESTAMP(format, timestamp_string)` - converts a string into a TIMESTAMP 
* `FORMAT_TIMESTAMP(format, timestamp)` - returns a string in the specified format
* `NOW()` - returns the timestamp after the logical plan is built. All calls of now() within the same query return the same value.
* `CONVERT_TZ(timestamp, from_tz ,to_tz)` - converts a timestamp from one timezone to another

There are a few existing UDFs that deal with dates. These should be left as is until a DATE type is implemented:

* UNIX_DATE
* DATETOSTRING
* STRINGTODATE

### Casting

Casting from TIMESTAMP to STRING will return the timestamp in ISO-8601 form with millisecond
precision (yyyy-mm-ddTHH:mm:ss:fff), and  casting from STRING to TIMESTAMP will attempt to parse the
string into a TIMESTAMP.

```roomsql
 > SELECT CAST("1994-11-05T13:15.30" AS TIMESTAMP) FROM stream_name EMIT CHANGES;
"1994-11-05T13:15.30"
 > SELECT CAST(time AS STRING) FROM stream_name EMIT CHANGES;
"1994-11-05T13:15.30"
```

### Arithmetic operations and comparisons

At the very least, we would want KSQL to be able to compare, add and subtract TIMESTAMPS.

[MySQL](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html) and [MariaDB](https://mariadb.com/kb/en/date-time-functions/)
have very complete lists of built-in arithmetic functions, while [PostgreSQL](https://www.postgresql.org/docs/9.0/functions-datetime.html)
defines behaviors for arithmetic operators.

Because functions are more well-defined, we should opt for built-in arithmetic functions
instead of using operators. The following functions are necessary:

* `TIMESTAMP_ADD(time_stamp, duration)` - adds a duration to `time_stamp` and returns the result as a
TIMESTAMP
* `TIMESTAMP_SUB(time_stamp1, duration)` - subtracts a duration from `time_stamp` and returns the
result as a TIMESTAMP

Durations will be expressed in the form, `<integer_value> <unit>`. This is discussed further in the
next section.

As for comparisons, the following expressions should be supported:
```
time_stamp1 < time_stamp2
time_stamp1 <= time_stamp2
time_stamp1 > time_stamp2
time_stamp1 >= time_stamp2
time_stamp1 = time_stamp2
time_stamp1 <> time_stamp2
time_stamp1 BETWEEN time_stamp2 AND time_stamp3
```

Comparisons between TIMESTAMPs and other data types should not be allowed, though a ISO-8601 formatted datestring can be used to represent TIMESTAMPs
([This is already supported for system time columns](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/select-push-query/#example)).

### Durations / Time units

KSQL currently recognizes the following time units for defining windows:
* DAY, DAYS
* HOUR, HOURS
* MINUTE, MINUTES
* SECOND, SECONDS
* MILLISECOND, MILLISECONDS

These could be repurposed to enable timestamp arithmetic. For example,

```roomsql
SELECT TIMESTAMP_ADD(TIME, 1 DAY) FROM FOO;
```

Because windowing is handled separately from expression evaluation, doing this will not have any
impact on windows.

## Test plan

The following will need to be tested:
* Integration with Kafka Connect and Schema Registry
* Different serialization formats
* QTTs with all of the new and updated UDFs

## LOEs and Delivery Milestones

This feature can be broken down into three milestones:

1. Implementing (including testing and documentation)
the TIMESTAMP type (1-2 weeks)
2. Adding and updating supporting UDFs (~1 week)
3. Implementing duration and arithmetic (~1-2 weeks) - this will be in 0.16

## Documentation Updates

There will need to be documentation on the following:

* Description of the TIMESTAMP data type - lack of timezone should be clear
* TIMESTAMP usage in WHERE/GROUP/PARTITION clauses
* Arithmetic and duration
* New and updated UDFs
* The deprecation of TIMESTAMPTOSTRING and STRINGTOTIMESTAMP should be stated and users should be
directed to the TIMESTAMP type and the new set of UDFs.
* We might want to add this into one of the quick-starts

## Compatibility Implications

If a user issues a command that includes the TIMESTAMP type, then previous versions of KSQL will not
recognize the TIMESTAMP type, and the server will enter a DEGRADED state.

## Security Implications

None
