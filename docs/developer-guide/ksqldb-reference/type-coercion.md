---
layout: page
title: Type Coercion and Casting
tagline: Converting between ksqlDB types
description: Understanding implicit and explicit type conversion.
keywords: ksqldb, coercion, cast, types
---

ksqlDB supports both implicit and explicit conversion between [Sql types][1]. Explicit conversion
is supported via the [`CAST` function][2], which supports a super-set of the conversions that 
ksqlDB will perform using implicit type coercion on your behalf.

## Implicit type coercion

ksqlDB supports implicit type coercion that converts between related types. 

### General rules

* numeric types can be coerced to a wider numeric type. For example, an `INTEGER` expression can
be coerced to a `BIGINT`, which can be coerced to a `DECIMAL`, which can be coerced to a `DOUBLE`.
* `ARRAY` types can be coerced to any other `ARRAY` type where the source array's element type can
be coerced to the target's.
* `MAP` types can be coerced to any other `MAP` type where both the source map's key and value types
can be coerced to the target's. 
* `STRUCT` types can be coerced to any other `STRUCT` type where the types of any field that exists
in both can be coerced.

For example, given:

```sql
CREATE STREAM FOO (
    A INT,
    B BIGINT
 ) WITH (...);

SELECT * FROM FOO WHERE A > B; 
```

...the `A` in the expression `WHERE A > B` will be implicitly coerced to a `BIGINT` to allow the
comparison with `B`.

### Literal rules 

Literal values support more lax and open coercion rules than other expression types:

* All of the above general rules, plus:
* any literal can be coerced to a `STRING`.
* a `STRING` literal containing a number can be coerced to a numeric type wide enough to store the
number. Where that type is a `DOUBLE` the result be inexact due to rounding.
* a `STRING` literal containing a boolean value can be coerced to a `BOOLEAN`. Valid boolean values
are `true`, `false`, `yes`, `no`, or any substring of these values that starts with the first 
character. For example, `fal`, `y`. Comparison is case-insensitive.

## Expression lists

Where an operator or function takes multiple expressions of the same type, for example, the 
`ARRAY` or other structured type constructors or the `IN` operator, the above coercion rules are 
applied to ensure all expressions can be coerced to a common type.

The common type is determined by inspecting each expression in order. If the first non-null element
is a:
 * `STRING`: then all other expressions must be coercible to `STRING`.
 * Numeric: then all other expressions must be coercible to a number. The common type being the numeric
 type wide enough to hold all numbers found in the list.
 * `BOOLEAN`: then all other expressions must be coercible to `BOOLEAN`.
 * `ARRAY`: then all other expressions must be `ARRAY`s and have element types that can be coerced 
 to a common element type.
 * `MAP`: then all other expressions must be `MAP`s and have key and value types that can be coerced
 to a common key and value type. 
 * `STRUCT`: then all other expressions must be `STRUCT`s. Common field names must have types that 
 can be coerced to a common type. The common type will be a `STRUCT` with the super set of all 
 fields.
 
For example, the `IN` operator a single expression to compare to a list of other expressions. All
expressions must be coercible to a single common type, which is used to perform the comparison: 

```sql
CREATE STREAM FOO (
    A INT,
    B BIGINT
 ) WITH (...);

SELECT * FROM FOO WHERE A IN [B, 10, '100', '3e2']; 
```

[1]: ../../concepts/schemas.md#sql-data-types
[2]: scalar-functions.md#cast