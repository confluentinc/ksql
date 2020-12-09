---
layout: page
title: Schemas in ksqlDB
tagline: Defining the structure of your data
description: Learn how schemas work with ksqlDB
keywords: ksqldb, schema, evolution, avro, protobuf, json, csv
---

## SQL data types

The following SQL types are supported by ksqlDB:

* [Primitive types](#primitive-types)
* [Decimal type](#decimal-type)
* [Array type](#array-type)
* [Map type](#map-type)
* [Struct type](#struct-type)
* [Custom types](#custom-types)

### Primitive types

Supported primitive types are:

* `BOOLEAN`: a binary value
* `INT`: 32-bit signed integer
* `BIGINT`: 64-bit signed integer
* `DOUBLE`: double precision (64-bit) IEEE 754 floating-point number
* `STRING`: a unicode character sequence (UTF8)

### Decimal type

The `DECIMAL` type can store numbers with a very large number of digits and perform calculations exactly.
It is recommended for storing monetary amounts and other quantities where exactness is required.
However, arithmetic on decimals is slow compared to integer and floating point types.

`DECIMAL` types have a _precision_ and _scale_.
The scale is the number of digits in the fractional part, to the right of the decimal point.
The precision is the total number of significant digits in the whole number, that is,
the number of digits on both sides of the decimal point.
For example, the number `765.937500` has a precision of 9 and a scale of 6.

To declare a column of type `DECIMAL` use the syntax:

```sql
DECIMAL(precision, scale)
```

The precision must be positive, the scale zero or positive.

### Array type

The `ARRAY` type defines a variable-length array of elements. All elements in the array must be of
the same type.

To declare an `ARRAY` use the syntax:

```
ARRAY<element-type>
```

The _element-type_ of an another [SQL data type](#sql-data-types).

For example, the following creates an array of `STRING`s:

```sql
ARRAY<STRING>
```

Instances of an array can be created using the syntax:

```
ARRAY[value [, value]*]
```

For example, the following creates an array with three `INT` elements:

```sql
ARRAY[2, 4, 6]
```

### Map type

The `MAP` type defines a variable-length collection of key-value pairs. All keys in the map must be
of the same type. All values in the map must be of the same type. Currently only `STRING` keys are
supported. The value type can be any valid SQL type.

!!! note
    The `MAP` type is only supported for value columns, not key columns, as map keys
    may lead to unexpected behavior due to inconsistent serialization.
    This restriction includes nested types containing maps as well.

To declare a `MAP` use the syntax:

```
MAP<key-type, element-type>
```

The _key-type_ must currently be `STRING` while the _value-type_ can an any other [SQL data type](#sql-data-types).

For example, the following creates a map with `STRING` keys and values:

```sql
MAP<STRING, STRING>
```

Instances of a map can be created using the syntax:

```
MAP(key := value [, key := value]*)
```

For example, the following creates a map with three key-value pairs:

```sql
MAP('a' := 1, 'b' := 2, 'c' := 3)
```

### Struct type

The `STRUCT` type defines a list of named fields, where each field can have any [SQL data type](#sql-data-types).

To declare a `STRUCT` use the syntax:

```
STRUCT<field-name field-type [, field-name field-type]*>
```

The _field-name_ can be any [valid identifier](#valid-identifiers). The _field-type_ can be any
valid [SQL data type](#sql-data-types).

For example, the following creates a struct with an `INT` field called `FOO` and a `BOOLEAN` field
call `BAR`:

```sql
STRUCT<FOO INT, BAR BOOLEAN>
```

Instances of a struct can be created using the syntax:

```
STRUCT(field-name := field-value [, field-name := field-value]*)
```

For example, the following creates a struct with fields called `FOO` and `BAR` and sets their values
to `10` and `true`, respectively:

```sql
STRUCT('FOO' := 10, 'BAR' := true)
```

### Custom types

KsqlDB supports custom types using the `CREATE TYPE` statements.
See the [`CREATE TYPE` docs](../developer-guide/ksqldb-reference/create-type) for more information.