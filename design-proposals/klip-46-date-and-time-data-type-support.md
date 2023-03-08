# KLIP 46 - DATE and TIME Data Type Support

**Author**: Zara Lim (@jzaralim) | 
**Release Target**: 0.20.0; 7.0.0 | 
**Status**: _Merged_ | 
**Discussion**: https://github.com/confluentinc/ksql/pull/7417

**tl;dr:** _Add support for DATE and TIME types. This will give users a more complete set of types
to represent time data with, and makes it easier to handle date/time data from other data sources._
           
## Motivation and background

The TIMESTAMP data type was introduced in ksqlDB 0.17. It was an improvement on time data
representation in ksqlDB, and we would like to expand on this by introducing DATE and TIME.
DATE and TIME are useful to have because:

* They store a different kind of time data. A TIMESTAMP stores some specific point in time, whereas
DATE and TIME store a date on a calendar or a time on a clock. For example, if we wanted to represent
a daily alarm time in ksqlDB, it would not make sense to store it as a TIMESTAMP.
* Clear representation of the underlying Kafka types. Currently, ksqlDB converts Date and Time data
from Kafka to BIGINT. This is inconsistent with how ksqlDB converts Timestamp data, and is a messy
user experience especially when moving data to/from different sources. For example, if a user wants
to use ksqlDB to manipulate their Kafka Data and Time data using the built-in time functions and then write
their results to an external database, they would first have to convert their BIGINT data to TIMESTAMP
and then set up their Sink connector to convert their data back to Data/Time.

## What is in scope
* Add TIME and DATE types to KSQL
* Support comparisons with the TIME and DATE types
* Support TIME and DATE usage in STRUCT, MAP and ARRAY
* Serialization and de-serialization of TIME and DATE to Avro, JSON, Protobuf and Delimited formats
* New UDFs to support the TIME and DATE types, as well as deprecating old DATE functions
* Casting between TIMESTAMP, STRING and DATE/TIME

## What is not in scope
* A DATETIME type - there is no underlying Kafka type for this
* Allowing TIME values to be defined with timezones (13:32:52+0100) - most other systems don't support this

## Public APIS

### DATE
The DATE data type will store a calendar date independent of time zone.
The syntax is as follows:

```roomsql
CREATE STREAM stream_name (birthday DATE, COL2 STRING) AS ...
CREATE TABLE table_name (col1 STRUCT<field DATE>) AS ...
```

DATEs will be displayed in console as strings in the form `yyyy-MM-dd`:

```roomsql
> SELECT date FROM stream_name EMIT CHANGES;

+------------------------+
|date                    |
+------------------------+
|1994-11-05              |
```

DATEs can be represented by date strings:

```roomsql
INSERT INTO stream_name VALUES ("1994-11-05");
```

The format `yyyy-MM-dd` can also represent a TIMESTAMP, so if a DATE or TIMESTAMP value are both
acceptable in the context that a date string is used, then TIMESTAMP will take precedence.
For example, because the function `FORMAT_DATE` can only accept DATE parameters, the date string will
be converted to a DATE.

```roomsql
SELECT DATE_ADD(DAYS, 2, '1994-11-05') FROM stream_name;
+------------------------+
|date                    |
+------------------------+
|1994-11-07              |
```

In the following example, the query makes sense as both a DATE and a TIMESTAMP comparison,
so ksqlDB will read them as TIMESTAMP.

```roomsql
SELECT '1994-11-07' > '2020-03-31' FROM stream_name;
```

### TIME 
The TIME data type will store a time without timezone information.
The syntax is as follows:

```roomsql
CREATE STREAM stream_name (alarm_time TIME, COL2 STRING) AS ...
CREATE TABLE table_name (col1 STRUCT<field TIME>) AS ...
```

TIMEs will be displayed in console as strings in the form `hh:mm:ss.SSS`:

```roomsql
> SELECT time FROM stream_name EMIT CHANGES;

+------------------------+
|time                    |
+------------------------+
|00:40:53.222            |
```

TIMEs can be represented by time strings:

```roomsql
INSERT INTO stream_name VALUES ("00:40:53.222");
```

Time zones will not be recognized in time strings.

### UDFs
The following UDFs should be deprecated:
* `DATETOSTRING`
* `STRINGTODATE`

These functions will still be available so that existing queries using these functions won't break,
but the documentation will state that they are deprecated and will direct users towards using the
DATE type and the new functions.

The following functions will be added/updated:
* `FORMAT_DATE(format, date)` - converts a date to a string in the specified format
* `FORMAT_TIME(format, time)` - converts a time to a string in the specified format
* `PARSE_DATE(format, date_string)` - converts a date string in the specified format to a DATE
* `PARSE_TIME(format, time_string)` - converts a time string in the specified format to a TIME
* `UNIX_DATE(date)` - returns an INTEGER number of days that have passed between Unix epoch and the specified date
* `FROM_DAYS(int)` -  convert epoch days to a DATE value
* `DATEADD(time unit, integer, date)` - Adds an interval to the date. The time unit must be `DAYS` or `YEARS`. If it is not,
then the function will throw an error
* `DATESUB(time unit, integer, date)` - Subtracts an interval from the date. The time unit must be `DAYS` or `YEARS`. If it is not,
then the function will throw an error
* `TIMEADD(time unit, integer, time)` - Adds an interval to the time. If the result falls outside of
the `00:00:00.000` to `23:59:59.999` range, then the result will be adjusted.
* `TIMESUB(time unit, integer, time)` - Subtracts an interval from the time. If the result falls outside of
the `00:00:00.000` to `23:59:59.999` range, then the result will be adjusted.

## Design
### Serialization/Deserialization

The DATE type will be handled by the `java.sql.Date` class within KSQL and the TIME type will be
handled by `java.sql.Time`. The corresponding Kafka Connect types are [org.apache.kafka.connect.data.Date](https://kafka.apache.org/0100/javadoc/org/apache/kafka/connect/data/Date.html)
and [org.apache.kafka.connect.data.Time](https://kafka.apache.org/0100/javadoc/org/apache/kafka/connect/data/Time.html).
Both are represented as integers in Schema Registry with tags to identify their logical types.

#### Avro
Avro schemas represent dates as

```json
{
  "type": "int",
  "logicalType": "date"
}
```

And time as
```json
{
  "type": "int",
  "logicalType": "time-millis"
}
```

The Avro deserializer ksqlDB uses support these.

#### JSON/Delimited

Dates will get stored in JSON and CSV files as an int number of days since Unix Epoch. Times will
get stored as an int number of milliseconds since 00:00:00.000

The KSQL JSON and delimited deserializers will be updated to parse dates and times.

#### Protobuf
Protobuf has a `google.type.TimeOfDay` type for time and a `google.type.Date` type for dates.
The Protouf deserializer KSQL uses supports this.

### Casting
#### TIMESTAMP as DATE
Casting from TIMESTAMP to DATE discards the time portion of the TIMESTAMP:
```roomsql
-- ts is the timestamp, '2007-01-01T03:20:45'
SELECT CAST(ts AS DATE) FROM s;
> 2007-01-1
```
#### TIMESTAMP as TIME
Casting from TIMESTAMP to DATE discards the date portion of the TIMESTAMP:
```roomsql
-- ts is the timestamp, '2007-01-01T03:20:45'
SELECT CAST(ts AS TIME) FROM s;
> 03:20:45
```
#### DATE AS TIMESTAMP
Casting from DATE to TIMESTAMP adds `00:00:00.000` as the time portion to the DATE:
```roomsql
-- d is the date, '2007-01-01'
SELECT CAST(d AS TIMESTAMP) FROM s;
> 2007-01-1T00:00:00
```

#### TIME AS STRING
Casting from TIME to STRING will convert the TIME value to a STRING of the form `hh:mm:ss`

```roomsql
-- t is the time, '03:20:45'
SELECT CAST(t AS STRING) FROM s;
> 03:20:45
```

#### DATE AS STRING
Casting from DATE to STRING will convert the DATE value to a STRING of the form `YYYY-MM-DD`

```roomsql
-- d is the date, '2007-01-01'
SELECT CAST(d AS STRING) FROM s;
> 2007-01-01
```

#### STRING AS TIME
Casting from STRING to TIME will attempt to parse the string to a TIME value in `hh:mm:ss[.sss]` format.

```roomsql
SELECT CAST('03:20:45' AS TIME) FROM s;
> 03:20:45
```

#### STRING AS DATE

Casting from STRING to DATE will attempt to parse the string to a DATE value in `YYYY-MM-DD` format.

```roomsql
SELECT CAST('2007-01-01' AS DATE) FROM s;
> 2007-01-01
```

### Comparisons

The result of a DATE/DATE or TIME/TIME comparison will be the same as the comparison of the integer
values of them (number of days since Unix epoch for DATE and number of milliseconds since the
beginning of the day for TIME).

Any comparison with a TIMESTAMP will convert the other value to TIMESTAMP.

A STRING/DATE comparison will convert both the STRING and the DATE to TIMESTAMP.

A STRING/TIME comparison will convert the STRING to TIME if the format is `hh:mm:ss[.sss]`, otherwise
it will convert the STRING to TIMEZONE.

Comparisons between DATE and TIME will not be allowed.

## Test plan
There will need to be tests for the following:
* Integration with Kafka Connect and Schema Registry
* All serialization formats
* QTTs with all of the new and updated UDFs

## LOEs and Delivery Milestones
DATE and TIME are each a milestone. They can both be broken up as follows:
* Adding the types to the syntax - 2 days
* Serialization/deserialization - 3 days
* Documentation - 3 days
* Adding and updating UDFs + documentation - 1 week
* Add to Connect integration test - 1 day

DATE and TIME should be implemented together for the following componenets:
* Casting - 4 days
* Comparisons - 3 days

## Documentation Updates
* The deprecation of DATETOSTRING and STRINGTODATE should be stated and users should be
directed to the DATE type and the new set of UDFs.
* Add and update UDFs to `docs/developer-guide/ksqldb-reference/scalar-functions.md`
* Serialization/deserialization information in `docs/reference/serialization.md`
* Section on casting in `docs/developer-guide/ksqldb-reference/type-coercion.md`
* Detailed description of all the time types in `docs/reference/sql/data-types.md`
* New section in `docs/developer-guide/ksqldb-reference/operations.md` for comparisons

## Compatibility Implications
If a user issues a command that includes the DATE or TIME type, then previous versions of KSQL will not
recognize the TIMESTAMP type, and the server will enter a DEGRADED state.

## Security Implications

None
