---
layout: page
title: Type Coercion and Casting
tagline: Converting between ksqlDB types
description: Understanding implicit and explicit type conversion.
keywords: ksqldb, coercion, cast, types, type conversion
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/type-coercion.html';
</script>

ksqlDB supports both implicit and explicit conversion between [SQL types](/reference/sql/data-types).
Explicit conversion is supported by using the [`CAST` function](scalar-functions.md#cast), which
supports a superset of the conversions that ksqlDB performs using implicit type
coercion on your behalf.

## Implicit type coercion

ksqlDB supports implicit type coercion that converts between related types. 

### General rules

* Numeric types can be coerced to a wider numeric type. For example, an
  `INTEGER` expression can be coerced to a `BIGINT`, which can be coerced to a
  `DECIMAL`, which can be coerced to a `DOUBLE`.
* `ARRAY` types can be coerced to any other `ARRAY` type where the source
  array's element type can be coerced to the target's type.
* `MAP` types can be coerced to any other `MAP` type where both the source
  map's key and value types can be coerced to the target's types. 
* `STRUCT` types can be coerced to any other `STRUCT` type where the types of
  any field that exists in both can be coerced.

For example, in the following query, the `A` in the expression `WHERE A > B`
is coerced implicitly to a `BIGINT`, to enable the comparison with `B`.

```sql
CREATE STREAM FOO (
    A INT,
    B BIGINT
 ) WITH (...);

SELECT * FROM FOO WHERE A > B; 
```

### Literal rules 

Literal values support more open coercion rules than other expression types.

* All of the above general rules apply, and:
* Any literal can be coerced to a `STRING`.
* A `STRING` literal containing a number can be coerced to a numeric type wide
  enough to store the number. If the type is a `DOUBLE`, the result may be
  inexact, due to rounding.
* A `STRING` literal containing a boolean value can be coerced to a `BOOLEAN`.
  Valid boolean values are `true`, `false`, `yes`, `no`, or any substring of
  these values that starts with the first character, for example, `fal`, `y`.
  Comparison is case-insensitive.
* A `STRING` literal containing an ISO-8601 formatted date or time string can be coerced
  to `TIMESTAMP`, `DATE` or `TIME`. A datestring containing a timezone is converted to UTC.

## Expression lists

Where an operator or function takes multiple expressions of the same type, for
example, the `ARRAY` or other structured type constructors or the `IN` operator,
the previous coercion rules are applied to ensure that all expressions can be
coerced to a common type.

The common type is determined by inspecting each expression in order. The
behavior depends on the type of the first non-null element.

 * `STRING`: all other expressions must be coercible to `STRING`.
 * Numeric: all other expressions must be coercible to a number. The common
   type is a numeric type that's wide enough to hold all numbers found in the
   list.
 * `BOOLEAN`: all other expressions must be coercible to `BOOLEAN`.
 * `BYTES`: all other expressions must be coercible to `BYTES`.
 * `TIMESTAMP`: all other expressions must be coercible to `TIMESTAMP`.
 * `TIME`: all other expressions must be coercible to `TIME`.
 * `DATE`: all other expressions must be coercible to `DATE`.
 * `ARRAY`: all other expressions must be `ARRAY`s and have element types that
   can be coerced to a common element type.
 * `MAP`: all other expressions must be `MAP`s and have key and value types
   that can be coerced to a common key and value type. 
 * `STRUCT`: all other expressions must be `STRUCT`s. Common field names must
   have types that can be coerced to a common type. The common type is a `STRUCT`
   with the superset of all fields.
 
For example, the `IN` operator uses a single expression to compare to a list
of other expressions. All expressions must be coercible to a single common type,
which is used to perform the comparison.

```sql
CREATE STREAM FOO (
    A INT,
    B BIGINT
 ) WITH (...);

SELECT * FROM FOO WHERE A IN [B, 10, '100', '3e2']; 
```
