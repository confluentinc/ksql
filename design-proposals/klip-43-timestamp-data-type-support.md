# KLIP-43: TIMESTAMP Data Type Support

**Author**: @jzaralim | 
**Release Target**: 0.15 | 
**Status**: In Discussion | 
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
* Support TIMESTAMP usage in STRUCT, MAP and ARRAY
* Serialization and de-serialization of TIMESTAMPs to Avro, JSON, Protobuf and Delimited formats
* Update existing built-in functions to use the TIMESTAMP data type
* Casting between TIMESTAMP and BIGINT

## What is not in scope
* Changing the ROWTIME data type. We will eventually want this to happen, but that is a separate
discussion.
* Support for dates before Unix epoch - this is not supported by Kafka. There is a [KIP](https://cwiki.apache.org/confluence/display/KAFKA/KIP-228+Negative+record+timestamp+support)
for negative timestamps, but until that is implemented, KSQL can only support positive timestamps.
* DATE and TIME types - because TIMESTAMPs represent a point in time, DATE and TIME types
would be useful if a user wants to represent a less specific time. These would be added after
TIMESTAMP is implemented though.
* Allow window units (HOUR, DAY etc) to be used in timestamp functions.

## Public APIS

The TIMESTAMP data type will store a point in time after Unix epoch without timezone information.
The syntax is as follows:

```roomsql
CREATE STREAM stream_name (time TIMESTAMP, COL2 STRING) AS ...
CREATE TABLE table_name (col1 STRUCT<field TIMESTAMP>) AS ...
```

TIMESTAMPS will be displayed in console as strings in ISO-8601 format:

```roomsql
> SELECT time FROM stream_name EMIT CHANGES;

+------------------------+
|time                    |
+------------------------+
|1994-11-05T13:15:30Z    |
```

TIMESTAMPS can be represented by milliseconds from Unix epoch or by using conversion functions:

```roomsql
INSERT INTO stream_name VALUES (1605927509166);
INSERT INTO stream_name VALUES (STRINGTOTIMESTAMP("2020-11-20", "YYYY-MM-DD"));
```

## Design

### Serialization/Deserialization

TIMESTAMPs will be handled by the `java.time.Instant` class within KSQL. The corresponding Kafka Connect type is
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

Timestamps will get stored in JSON and CSV files as long values. The KSQL JSON and delimited deserializers
will be updated to parse timestamps.

### UDFs

The following UDFs should be updated to use the TIMESTAMP type instead of BIGINT:

* TIMESTAMPTOSTRING
* STRINGTOTIMESTAMP

Because BIGINTs will be implicitly cast into TIMESTAMPs and vice versa, queries using these functions
with BIGINT will still work when TIMESTAMP is introduced.

There will also be a new function, `NOW()` that returns the current timestamp.

There are a few existing UDFs that deal with dates. These should be left as is until a DATE type is implemented:

* UNIX_DATE
* DATETOSTRING
* STRINGTODATE

### Casting

Casting from TIMESTAMP to BIGINT will return the millisecond representation of the timestamp, and
casting from BIGINT to TIMESTAMP will return a TIMESTAMP that is the BIGINT number of milliseconds
from Unix epoch. 

If a user attempts to cast a negative number or a timestamp before Unix epoch, the CAST will throw
an error.

### Arithmetic operations and comparisons

At the very least, we would want KSQL to be able to compare, add and subtract TIMESTAMPS.

[MySQL](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html) and [MariaDB](https://mariadb.com/kb/en/date-time-functions/)
have very complete lists of built-in arithmetic functions, while [PostgreSQL](https://www.postgresql.org/docs/9.0/functions-datetime.html)
defines behaviors for arithmetic operators.

Because functions are more well-defined, we should opt for built-in arithmetic functions
instead of using operators. The following functions are necessary:

* `TIMESTAMPADD(time_stamp, big_int)` - adds `big_int` milliseconds to `time_stamp` and returns the result as a
TIMESTAMP
* `TIMESTAMPDIFF(time_stamp1, time_stamp2)` - returns a BIGINT representing the number of milliseconds
between `timestamp1` and `timestamp2`
* `TIMESTAMPSUB(time_stamp1, big_int)` - subtracts `big_int` milliseconds from `time_stamp` and returns the
result as a TIMESTAMP

As for comparisons, the following expressions should be supported:
```
time_stamp1 < time_stamp2
time_stamp1 > time_stamp2
time_stamp1 = time_stamp2
time_stamp1 BETWEEN time_stamp2 AND time_stamp3
```

Comparisons between TIMESTAMPs and other data types should not be allowed.

### Window units (not in scope)

It might make sense to give window units a time representation so that they could be used as
time intervals in TIMESTAMP arithmetic. For example,

```roomsql
SELECT ADDTIME(TIME, 1 DAY) FROM FOO;
```

Because windowing is handled separately from expression evaluation, doing this will not have any
impact on windows.

## Test plan

The following will need to be tested:
* Integration with Kafka Connect and Schema Registry
* Different serialization formats
* QTTs with all of the new and updated UDFs

## LOEs and Delivery Milestones

This feature can be broken down into two milestones: implementing (including testing and documentation)
the TIMESTAMP type (1-2 weeks) and adding all of the supporting UDFs (~1 week).

## Documentation Updates

There will need to be documentation on the following:

* Description of the TIMESTAMP data type
* TIMESTAMP usage in WHERE/GROUP/PARTITION clauses
* New and updated UDFs
* We might want to add this into one of the quick-starts

## Compatibility Implications

If a user issues a command that includes the TIMESTAMP type, then previous versions of KSQL will not
recognize the TIMESTAMP type, and the server will enter a DEGRADED state.

## Security Implications

None
