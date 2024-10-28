---
layout: page
title: Data Types Overview
tagline: SQL data types in ksqlDB
description: Overview of data types in ksqlDB
keywords: ksqldb, sql, syntax, data type
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/reference/sql/data-types.html';
</script>

## Boolean types

| name      | description                          | backing Java type
|-----------|--------------------------------------|------------------
| `boolean` | value representing `true` or `false` | [`java.lang.Boolean`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Boolean.html)

## Character types

| name                | description            | backing Java type
|---------------------|------------------------|------------------
| `varchar`, `string` | variable-length string | [`java.lang.String`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/String.html)
| `bytes`             | variable-length byte array | [byte []](https://docs.oracle.com/javase/8/docs/api/java/lang/Byte.html)

The `varchar` type represents a string in UTF-16 format.

Comparisons between `varchar` instances don't account for locale.

The `bytes` type represents an array of raw bytes.

## Numeric types

| name      | storage size    | range (min value to max value value)        | description                     | backing Java type
|-----------|-----------------|---------------------------------------------|---------------------------------|------------------
| `int`     | 4 bytes         | -2<sup>31</sup> to 2<sup>31</sup>-1         | typical choice for integer      | [`Integer`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Integer.html)
| `bigint`  | 8 bytes         | -2<sup>63</sup> to 2<sup>63</sup>-1         | large-range integer             | [`Long`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Long.html)
| `double`  | 8 bytes         | 2<sup>-1074</sup>&#8224; to (2-2<sup>-52</sup>)·2<sup>1023</sup> | variable-precision, inexact     | [`Double`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Double.html)
| `decimal` | value dependent | n/a                                         | user-specified precision, exact | [`BigDecimal`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/math/BigDecimal.html)
&#8224; Smallest positive nonzero value

### Integer types

The `int` and `bigint` types store integers, which are numbers without
decimals. Storing values outside of the supported range results in an error.

If your values are in its range, the `int` type is a good choice, because
its implementation has minimal overhead. If your values may be of
a larger size, use `bigint`.

### Floating-point types

The `double` data type is an inexact, variable-precision numeric type. The term
"inexact" means an approximate value is stored. Storing values outside of its
bounds of capacity will result in an error.

### Valid ranges

Numeric data types have the same valid minimum and maximum values as their
corresponding Java types. 

### Arbitrary precision types

The `decimal` type can be used to store fractional numbers with exact precision.
This is useful for modeling money or other values that don't tolerate
approximate storage representations.

`decimal` types take two parameters: precision and scale. *Precision* is the
maximum total number of decimal digits to be stored, including values to the
left and right of the decimal point. The precision must be greater than 1.
There is no default precision.

*Scale* is the number of decimal digits to the right of the decimal point. This
number must be greater than _0_ and less than or equal to the value for precision.

To declare a column of the `decimal` type, use the syntax:

```sql
DECIMAL(precision, scale)
```

Mathematical operations between `double` and `decimal` cause the decimal to be
converted to a double value automatically. Converting from the `decimal` data
type to any floating point type (`double`) may cause loss of precision.

- Upcasting an `int` to a `decimal` produces a `decimal` with a precision of _10_
and a scale of _0_.
- Upcasting a `bigint` to a `decimal` produces a `decimal` with a precision of _19_
and a scale of _0_.

## Time types

| name      | description                                                     | backing Java type
|-----------|-----------------------------------------------------------------|------------------
|`time`     | value representing a time of day in millisecond precision.      | [`java.sql.Time`](https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/Time.html)
|`date`     | value representing a calendar date independent of time zone.    | [`java.sql.Date`](https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/Date.html)
|`timestamp`| value representing a point in time in millisecond precision without timezone information | [`java.sql.Timestamp`](https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/Timestamp.html)

## Compound types

!!! note
    The `DELIMITED` serialization format doesn't support compound types.

| name     | description                              | backing Java type
|----------|------------------------------------------|------------------
| `array`  | sequence of values of a single type      | [Java native array](https://docs.oracle.com/javase/specs/jls/se11/html/jls-10.html)
| `struct` | a strongly typed structured data type    | [`org.apache.kafka.connect.data.Struct`](https://kafka.apache.org/27/javadoc/index.html?org/apache/kafka/connect/data/Struct.html)
| `map`    | a mapping of keys to values              | [`java.util.map`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Map.html)


### Array

`ARRAY<ElementType>`

ksqlDB supports fields that are arrays of another type. All of the elements
in the array must be of the same type. The element type can be any valid
SQL type.

The elements of an array are one-indexed and can be accessed by using
the `[]` operator passing in the index. For example, `SOME_ARRAY[1]`
retrieves the first element from the array. For more information, see
[Operators](/developer-guide/ksqldb-reference/operators).

You can define arrays within a `CREATE TABLE` or `CREATE STREAM`
statement by using the syntax `ARRAY<ElementType>`. For example,
`ARRAY<INT>` defines an array of integers.

Also, you can output an array from a query by using a SELECT statement.
The following example creates an array from a stream named `s1`. 

```sql
SELECT ARRAY[1, 2] FROM s1 EMIT CHANGES;
```

Starting in version 0.7.1, the built-in AS_ARRAY function syntax for
creating arrays doesn't work. Replace AS_ARRAY with the ARRAY constructor
syntax. For example, replace this legacy query:

```sql
CREATE STREAM OUTPUT AS SELECT cube_explode(as_array(col1, col2)) VAL1, ABS(col3) VAL2 FROM TEST;
```

With this query:

```sql
CREATE STREAM OUTPUT AS SELECT cube_explode(array[col1, col2]) VAL1, ABS(col3) VAL2 FROM TEST;
```

### Struct

`STRUCT<FieldName FieldType, ...>`

ksqlDB supports fields that are structs. A struct represents strongly
typed structured data. A struct is an ordered collection of named fields
that have a specific type. The field types can be any valid SQL type.

Access the fields of a struct by using the `->` operator. For example,
`SOME_STRUCT->ID` retrieves the value of the struct's `ID` field; 
and `SOME_STRUCT->*` retrieves the values of all fields of `SOME_STRUCT`. For
more information, see [Operators](/developer-guide/ksqldb-reference/operators).

You can define structs within a `CREATE TABLE` or `CREATE STREAM`
statement by using the syntax `STRUCT<FieldName FieldType, ...>`. For
example, `STRUCT<ID BIGINT, NAME STRING, AGE INT>` defines a struct with
three fields, with the supplied name and type.

Also, you can output a struct from a query by using a SELECT statement.
The following example creates a struct from a stream named `s1`.

```sql
SELECT STRUCT(f1 := v1, f2 := v2) FROM s1 EMIT CHANGES;
```

### Map

`MAP<KeyType, ValueType>`

ksqlDB supports fields that are maps. A map has a key and value type. All
of the keys must be of the same type, and all of the values must also
be of the same type. Currently only `STRING` keys are supported. The
value type can be any valid SQL type.

Access the values of a map by using the `[]` operator and passing in the
key. For example, `SOME_MAP['cost']` retrieves the value for the entry
with key `cost`, or `null` For more information, see
[Operators](/developer-guide/ksqldb-reference/operators).

You can define maps within a `CREATE TABLE` or `CREATE STREAM` statement
by using the syntax `MAP<KeyType, ValueType>`. For example,
`MAP<STRING, INT>` defines a map with string keys and integer values.

Also, you can output a map from a query by using a SELECT statement.
The following example creates a map from a stream named `s1`.

```sql
SELECT MAP(k1:=v1, k2:=v1*2) FROM s1 EMIT CHANGES;
```

## Custom types

ksqlDB supports custom types using the `CREATE TYPE` statements.
See the [`CREATE TYPE` docs](/developer-guide/ksqldb-reference/create-type) for more information.
