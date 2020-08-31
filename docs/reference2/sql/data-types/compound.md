---
layout: page
title: Compound Data Types
tagline: Array, map, and struct data types
description: Syntax Reference for array, map, and struct data types in ksqlDB
keywords: ksqldb, sql, syntax, array, map, struct, data type
---

| name     | description                              | backing Java type
|----------|------------------------------------------|------------------
| `array`  | sequence of values of a single type      | [Java native array](https://docs.oracle.com/javase/specs/jls/se11/html/jls-10.html)
| `struct` | map of string keys to values of any type | [`org.apache.kafka.connect.data.Struct`](https://downloads.apache.org/kafka/2.5.0/javadoc/index.html?org/apache/kafka/connect/data/Struct.html)
| `map`    | map of varying typed keys and values     | [`java.util.map`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Map.html)


## Array

`ARRAY<ElementType>`

ksqlDB supports fields that are arrays of another type. All of the elements
in the array must be of the same type. The element type can be any valid
SQL type.

The elements of an array are one-indexed and can be accessed by using
the `[]` operator passing in the index. For example, `SOME_ARRAY[1]`
retrieves the first element from the array. For more information, see
[Operators](ksqldb-reference/operators.md).

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

!!! note
		The `DELIMITED` format doesn't support arrays.

## Struct

`STRUCT<FieldName FieldType, ...>`

ksqlDB supports fields that are structs. A struct represents strongly
typed structured data. A struct is an ordered collection of named fields
that have a specific type. The field types can be any valid SQL type.

Access the fields of a struct by using the `->` operator. For example,
`SOME_STRUCT->ID` retrieves the value of the struct's `ID` field. For
more information, see [Operators](ksqldb-reference/operators.md).

You can define a structs within a `CREATE TABLE` or `CREATE STREAM`
statement by using the syntax `STRUCT<FieldName FieldType, ...>`. For
example, `STRUCT<ID BIGINT, NAME STRING, AGE INT>` defines a struct with
three fields, with the supplied name and type.

Also, you can output a struct from a query by using a SELECT statement.
The following example creates a struct from a stream named `s1`.

```sql
SELECT STRUCT(f1 := v1, f2 := v2) FROM s1 EMIT CHANGES;
```

!!! note
		The `DELIMITED` format doesn't support structs.

## Map

`MAP<KeyType, ValueType>`

ksqlDB supports fields that are maps. A map has a key and value type. All
of the keys must be of the same type, and all of the values must be also
be of the same type. Currently only `STRING` keys are supported. The
value type can be any valid SQL type.

Access the values of a map by using the `[]` operator and passing in the
key. For example, `SOME_MAP['cost']` retrieves the value for the entry
with key `cost`, or `null` For more information, see
[Operators](ksqldb-reference/operators.md).

You can define maps within a `CREATE TABLE` or `CREATE STREAM` statement
by using the syntax `MAP<KeyType, ValueType>`. For example,
`MAP<STRING, INT>` defines a map with string keys and integer values.

Also, you can output a map from a query by using a SELECT statement.
The following example creates a map from a stream named `s1`.

```sql
SELECT MAP(k1:=v1, k2:=v1*2) FROM s1 EMIT CHANGES;
```

!!! note
		The `DELIMITED` format doesn't support maps.
