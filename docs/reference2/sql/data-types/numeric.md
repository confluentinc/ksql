---
layout: page
title: Numeric Data Types
tagline: Numeric data types in ksqlDB SQL
description: Syntax Reference for integer, double, and decimal data types in ksqlDB SQL
keywords: ksqldb, sql, syntax, int, integer, double, decimal, data type
---

| name      | storage size    | range (min value to max value value)        | description                     | backing Java type
|-----------|-----------------|---------------------------------------------|---------------------------------|------------------
| `int`     | 4 bytes         | -2<sup>31</sup> to 2<sup>31</sup>-1         | typical choice for integer      | [`Integer`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Integer.html)
| `bigint`  | 8 bytes         | -2<sup>63</sup> to 2<sup>63</sup>-1         | large-range integer             | [`Long`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Long.html)
| `double`  | 8 bytes         | 2<sup>-1074</sup>&#8224; to (2-2<sup>-52</sup>)·2<sup>1023</sup> | variable-precision, inexact     | [`Double`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Double.html)
| `decimal` | value dependent | n/a                                         | user-specified precision, exact | [`BigDecimal`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/math/BigDecimal.html)
&#8224; Smallest positive nonzero value

## Integer types

The `int` and `bigint` types store integers, which are numbers without
decimals. Storing values outside of the supported range results in an error.

If your values are in its range, the `int` type is a good choice, because
its implementation has minimal overhead. If your values may be of
a larger size, use `bigint`.

## Floating-point types

The `double` data type is an inexact, variable-precision numeric type. The term
"inexact" means an approximate value is stored. Storing values outside of its
bounds of capacity will result in an error.

## Valid ranges

Numeric data types have the same valid minimum and maximum values as their
corresponding Java types. 

## Arbitrary precision types

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