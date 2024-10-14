# KLIP 57 - Reading Kafka headers in ksqlDB
**Author**: Zara Lim (@jzaralim) | 
**Release Target**: 0.24 | 
**Status**: _Merged_ | 
**Discussion**: https://github.com/confluentinc/ksql/pull/8293

**tl;dr:** _Expose Kafka header data in ksqlDB. Currently, headers are the only metadata in a Kafka record that is invisible to users._

## Motivation and background

So far, ksqlDB exposes the timestamp, partition and offset of Kafka records as pseudocolumns. The only metadata left that is invisible to ksqlDB users are [headers](https://kafka.apache.org/28/javadoc/org/apache/kafka/common/header/Headers.html). Kafka headers are a list of zero or more key-value pairs, where the keys are (not necessarily unique) strings and the values are byte arrays. Headers typically contain metadata about records, which can then be used for routing or processing (e.g. the header could store information about how to deserialize strings in the value). If ksqlDB exposes headers to the user, then these steps could be done in ksqlDB.

## What is in scope
* Syntax for accessing headers
* Support for all query types except for `INSERT` queries
* Including header columns in a data source's schema
* Functions to help decode byte strings to primitive types (BYTES to STRING already exists, new functions would be conversions to INT, BIGINT and DOUBLE)

## What is not in scope
* Writing headers to output topics - we might do this in the future, but that is a separate discussion
* Advanced functions to help decode byte strings (e.g. functions to deserialize complex serialized data.) - if a user wants to convert something that we don't have a function for, then they can either create their own UDF or use `CREATE FUNCTION` when that becomes available.

## Public APIS

### Syntax

Unlike other pseudocolumns, if a user wants to access headers, they will create a column with the `HEADERS` keyword. This column will be populated with the full list of header keys and values. There can only be one HEADERS column.
```
ksql> CREATE STREAM A (
    my_headers ARRAY<STRUCT<key STRING, value BYTES>> HEADERS);
```
If there are multiple HEADERS columns, then an error will be thrown. The type of a header backed column must be exactly `ARRAY<STRUCT<key STRING, value BYTES>>`. An error will be thrown if that is not the case.
```
ksql> CREATE STREAM B (my_headers ARRAY<BYTES> HEADERS);
Error: Columns specified with the HEADERS keyword must be typed as ARRAY<STRUCT<key STRING, value BYTES>>.
```
Users can also mark a column with `HEADER(<key>)`, which will populate the column with the last header that matches the key. The data type must be `BYTES`
```
ksql> CREATE STREAM B (value BYTES HEADER(<key>), value_2 BYTES HEADER(<key_2>));
```
If there is a column labelled `HEADERS`, then no there can be no `HEADER(<key>)` columns. Otherwise, there can be multiple `HEADER(<key>)` columns, but no two can share the same key. If the key is not present in the header, then the value in the column will be `null`.

Sources created with schema inferencing can have header columns, they would just have to be specified. For example, the statement `CREATE STREAM A (my_headers ARRAY<STRUCT<key STRING, value BYTES>> HEADERS) WITH (kafka_topic='a', value_format='avro');` will create a stream with columns from Schema Registry, as well as one header column.

### New byte-conversion functions

There will also be new byte conversion functions to help decode header data:

* `INT_FROM_BYTES(bytes, [byte_order])` - The input byte array must be exactly 4 bytes long, otherwise an error will be thrown. The conversion will by handled by `ByteBuffer`'s `getInt` method. `byte_order` is a string that can either be `BIG_ENDIAN` or `LITTLE_ENDIAN`. If `byte_value` is not supplied, then the function will assume `BIG_ENDIAN`.
* `BIGINT_FROM_BYTES(bytes, [byte_order])` - The input byte array must be exactly 8 bytes long, otherwise an error will be thrown. The conversion will by handled by `ByteBuffer`'s `getLong` method. If `byte_value` is not supplied, then the function will assume `BIG_ENDIAN`.
* `DOUBLE_FROM_BYTES(bytes, [byte_order])` - The input byte array must be exactly 8 bytes long, otherwise an error will be thrown. The conversion will by handled by `ByteBuffer`'s `getDouble` method. If `byte_value` is not supplied, then the function will assume `BIG_ENDIAN`.

In all of these cases, when an error is thrown, then the error gets written to the processing log. If the function is a part of the projection, then `null` will be writtin to the output column. If it's a part of the predicate, then the whole record will be skipped. This is consistent with how errors are handled when functions with value column argments throw errors.

### Queries

The headers columns will be usable as selection items, function arguments or predicates in any query like any other column, except for `INSERT SELECT` and `INSERT VALUES` statements. They will fail if trying to insert into a headers column. However, when persistent queries project header-backed columns, the header values are copied into the output row’s value (or key), not to headers in the output record.

### Schema

Header columns will fully be a part of the stream/table's schema. When running `SELECT *`, header columns will be included in the projection, and they will also appear when describing a source. However, headers will not be copied to the header fields of sink topics.

## Design
Columns in the logical schema will be represented by a new type of column `HeaderColumn` which will extend `Column` and contain an optional String named `key` representing the key in `HEADER(key)`. It will store `Optional.empty()` for columns defined with `HEADERS`. They will be in a new `HEADER` namespace, which functions identically to the `VALUE` namespace, except that the `StreamBuilder` will populate those columns with header values. 

## Test plan

The following test cases will be added:
* Creating `STREAM`/`TABLE` with header columns
* `SELECT * FROM ...` queries include header columns in the output
* `DESCRIBE SOURCE ...` includes any header columns
* Push and pull queries may select headers
* Queries can use header columns in joins, UDFs, filters, group by and partition by clauses
* Tests for new functions
* Inserting values into header columns is blocked
* Persistent queries that were created in a previous version can still work
* Sink topic headers are unaffected by these changes
* Header columns can be used with the Java client, migrations tool and the CLI

## LOEs and Delivery Milestones

Development of the feature will be done behind a feature flag, `ksql.headers.enabled`. When the flag is off, creating new streams and tables with headers will be disabled, but sources with headers will continue to work as usual.

* Create feature flag
* Add `HEADERS` to the ksqlDB syntax
* Add header columns to the `LogicalSchema`
* Update `SourceBuilder` to add headers
* Update anything else that would need to accommodate headers and the new `LogicalSchema`
* Disallow header columns in `INSERT` statements
* Add `HEADER(<key>)` to the ksqlDB syntax
* Update everything header related in the code to enable key-extracted headers
* Disable feature flag
* Implement new byte conversion functions

## Documentation Updates
* The synopses for `CREATE STREAM|TABLE` will be updated to include header columns
* A new section in the [docs page on pseudocolumns](https://docs.ksqldb.io/en/latest/reference/sql/data-definition/#pseudocolumns) dedicated to describe how to access headers.
* Add docs on each of the new functions

## Compatibility Implications
All queries created before the introduction of headers should still work. No compatibility implications are expected.

## Security Implications
N/A

## Alternatives Considered
### Add ROWHEADERS as a pseudocolumn, similar to timestamp/offset/partition
In this approach. `ROWHEADERS` works exactly like `ROWTIME`, `ROWOFFSET` and `ROWPARTITION`. This is the lowest LOE to implement, but it was rejected for the following reasons:
1. When performaing joins, ksqlDB includes all columns in the repartition and changelog topics (https://github.com/confluentinc/ksql/issues/7489), including pseudocolumns. `ROWHEADERS`, being a list of structs, would increase the size of the state store much more than the other pseudocolumns. The accepted approach makes headers opt-in, so at least the extra overhead is not forced on every join.
2. The `ROWHEADERS` column would not be included in the schema (`SELECT * ` and `DESCRIBE` won't include the column). The data in Kafka headers is populated by Kafka clients rather than by Kafka itself, so it makes sense to distinguish it from other system columns .

### Use a property in the WITH clause to identify the column to be interpreted as headers
Example: 
```
-- populate `my_headers` from each row's record headers
CREATE STREAM FOO (my_headers ARRAY<STRUCT<key VARCHAR, value VARCHAR>>)
  WITH (headers='my_headers');
```
* Supporting multiple header-backed columns would likely be awkward in WITH-based approach.
* `HEADER(<key>)` would not work well in this approach.
* Aligns to existing syntax of `KEY` and `PRIMARY KEY` that tell ksqlDB to populate a row from the record key, instead the record value (default).

### Automatically convert KSQL types (like VARCHAR or INT) from the headers' byte-array values, based on the KSQL type assigned to the column.
```
CREATE STREAM FOO (my_headers STRUCT<version ARRAY<INT>> HEADERS);
CREATE STREAM FOO2 (latest_version INT HEADER('version'));
```
* No schema information is carried in the headers when added with the core Headers interface.
* The Connect headers implementation does support schemas and Schema Registry interop, but  ksqlDB’s support cannot assume all headers are written in this fashion.
* Converting bytes based on the structure of the column type could only handle certain byte encodings and byte order. For example the common integer-to-bytes codings differ between Java and Python. These differences are better covered by explicit converter functions.
* Automatic conversion implies that the entire list of headers would be eagerly converted at processing time, even when the user may desire only to pluck and convert the latest value for a header key.

### Specify the serialization format of the headers so that the bytes can be deserialized against a known format.
```
CREATE STREAM FOO (my_headers STRUCT<version ARRAY<INT>> HEADERS)
  WITH (headers_format='kafka');
CREATE STREAM FOO2 (latest_version INT HEADER('version'))
  WITH (headers_format='avro');
```
* There is no clear convention for using a single serializer to write header values. We should expect it’s common to simply call the equivalent of getBytes on an integer instance, for example.
* Unlike for keys and values, there is no serde configuration for the whole set of headers. Users can mix the serialization approach between different headers added to the same record.
