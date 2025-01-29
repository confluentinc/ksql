# KLIP 52 - BYTES Data Type Support

**Author**: Zara Lim (@jzaralim) | 
**Release Target**: 0.21.0; 7.0.0 | 
**Status**: _Merged_ | 
**Discussion**: https://github.com/confluentinc/ksql/pull/7764

**tl;dr:** _Add support for the BYTES data type. This will allow users to work with BLOBs of data that don't fit into any other data type._
           
## Motivation and background

Currently, ksqlDB can only handle a set of primitive types and combinations of them.
A BYTES data type would allow users to work with data that does not fit into any of
the primitive types such as images, as well as BLOB/binary data from other databases.

## What is in scope
* Add BYTES type to KSQL
* Support BYTE comparisons
* Support BYTES usage in STRUCT, MAP and ARRAY
* Serialization and de-serialization of BYTES to Avro, JSON, Protobuf and Delimited formats
* Adding/updating UDFs to support the BYTES type

## What is not in scope
* Fixed sized BYTES (`BYTES(3)` representing 3 bytes, for example) - This is supported by Kafka Connect by adding the `connect.fixed.size`
key in a bytes schema, but this will not be included in this KLIP.

## Public APIS

### BYTES

The BYTES data type will store an array of raw bytes of an unspecified length. The maximum size of
the array is limited by the maximum size of a Kafka message, as well as possibly by the serialization format being used.
The syntax is as follows:

```roomsql
CREATE STREAM stream_name (b BYTES, COL2 STRING) AS ...
CREATE TABLE table_name (col1 STRUCT<field BYTES>) AS ...
```

By default, BYTES will be displayed in the CLI as HEX strings, where each byte is represented by two characters.
For example, the byte array `[91, 67]` will be displayed as:

```roomsql
ksql> SELECT b from STREAM;
0x5B43
```

API response objects will store BYTES data as base64 strings. The Java client's `Row` class will include a new function,
`getBytes` that returns the value of a column as a `ByteBuffer` object. It will expect the raw value to be a Base64 string,
and if it's not then the function will throw an error.

Implicit conversions to BYTES will not be supported.

### UDF

The following UDFs will be added:

* `to_bytes(string, encoding)` - this will convert a STRING value in the specified encoding to BYTES.
The accepted encoders are `hex`, `utf8`, `ascii`, and `base64`.
* `from_bytes(bytes, encoding)` - this will convert a BYTES value to STRING in the specified encoding.
The accepted encoders are `hex`, `utf8`, `ascii`, and `base64`.

We will also update some of the existing STRING functions to accept BYTES as a parameter. In general, if a function works on ASCII characters for a STRING parameter,
then it will work on bytes for a BYTES parameter.

* `len(bytes)` - This will return the length of the stored ByteArray.
* `concat(bytes...)` - Concatenate an arbitrary number of byte fields
* `r/lpad(bytes, target_length, padding_bytes)` - pads input BYTES beginning from the left/right with the specified padding BYTES until the target length is reached.
* `replace(bytes, old_bytes, new_bytes)` - returns the given BYTES value with all occurrences of `old_bytes` with `new_bytes`
* `split(bytes, delimiter)` - splits a BYTES value into an array of BYTES based on a delimiter
* `splittomap(bytes, entryDelimiter, kvDelimiter)` - splits a BYTES value into key-value pairs based on a delimiter and creates a MAP from them
* `substring(bytes, to, from)` - returns the section of the BYTES from the byte at position `to` to `from`

## Design
### Serialization/Deserialization

BYTES will be handled by [`java.nio.ByteBuffer`](https://docs.oracle.com/javase/7/docs/api/java/nio/ByteBuffer.html) within ksqlDB.
The underlying Kafka Connect type is the primitive `bytes` type. 

#### Avro

`bytes` is a primitive Avro type. When converting to/from Connect data, the Avro converter ksqlDB
uses converts byte arrays to ByteBuffer.

#### Protobuf

`bytes` is a primitive Protobuf type. The maximum number of bytes in a byte array is 2<sup>32</sup>.
When converting to/from Connect data, the Avro converter ksqlDB uses converts byte arrays to ByteBuffer.

#### JSON/Delimited

Byte arrays will be stored in JSON and CSV files as [Base64 MIME](https://docs.oracle.com/javase/8/docs/api/java/util/Base64.html#mime) encoded binary values.
This is because ksqlDB and Schema Registry both use Jackson to serialize and deserialize JSON,
and Jackson serializes binaries to Base64 strings.

The ksqlDB JSON and delimited deserializers will be updated to convert Base64 strings to ByteBuffer.

### Casting

Casting between BYTES and other data types will not be supported. Users can use `to_bytes` and `from_bytes` if they would like to convert to/from STRING.

### Comparisons

Comparisons will only be allowed between two BYTES. They will be compared lexicographically by
unsigned 8-bit values. For example, the following comparisons evaluate to `TRUE`:

```
[10, 11] > [10]
[12] > [10, 11]
```

## Test plan

There will need to be tests for the following:
* Integration with Kafka Connect and Schema Registry
* All serialization formats
* Different types of byte data
* QTTs with all of the new and updated UDFs

## LOEs and Delivery Milestones

The implementation can both be broken up as follows:
* Adding the BYTES type to ksqlDB - 2 days
* Serialization/deserialization - 4 days
* Add BYTES to the Java client - 2 days
* Documentation - 2 days
* Add to Connect integration test - 1 day
* Comparisons - 2 days
* Adding UDFs + documentation - 1 week
* Buffer time and manual testing - 3 days

## Documentation Updates

* Add and update UDFs to `docs/developer-guide/ksqldb-reference/scalar-functions.md`
* Serialization/deserialization information in `docs/reference/serialization.md`
* Section on casting in `docs/developer-guide/ksqldb-reference/type-coercion.md`
* Detailed description of `BYTES` in `docs/reference/sql/data-types.md`
* New section in `docs/developer-guide/ksqldb-reference/operations.md` for comparisons

## Compatibility Implications

If a user issues a command that includes the BYTES type, then previous versions of KSQL will not
recognize the BYTES type, and the server will enter a DEGRADED state.

## Security Implications

None
