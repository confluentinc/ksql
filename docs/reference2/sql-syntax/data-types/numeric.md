| name      | storage size    | description                     | backing Java type
|-----------|-----------------|---------------------------------|------------------
| `int`     | 4 bytes         | typical choice for integer      | [`java.lang.Integer`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Integer.html)
| `bigint`  | value dependent | large-range integer             | [`java.math.BigInteger`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/math/BigInteger.html)
| `double`  | 8 bytes         | variable-precision, inexact     | [`java.lang.Double`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Double.html)
| `decimal` | value dependent | user-specified precision, exact | [`java.math.BigDecimal`](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/math/BigDecimal.html)

TODO: what are the range of allowable values for each of these types?

## Integer types

The types `int` and `bigint` store integers, which are numbers without decimals. Storing values outside their bounds of capacity will result in an error.

If your values fit within its bounds, the `int` type is a good choice because its implementation has minimal overhead. Use `bigint` if your values may be of a larger size.

## Floating-point types

The `double` data type is an inexact, variable-precision numeric type. The term "inexact" means an approximate value is stored. Storing values outside of its bounds of capacity will result in an error.

## Arbitrary precision types

The `decimal` type can be used to store fractional numbers with exact precision. This is useful for modeling money or other values that do not tolerate approximate storage representations.

`decimal` types take two parameters: precision and scale. *Precision* is the maximum total number of decimal digits to be stored, including values to the left and right of the decimal point. The precision must be greater than 1. There is no default precision.

*Scale* is the number of decimal digits to the right of the decimal point. This number must be greater than `0` and less than or equal to the value for precision.

To declare a column of the `decimal` type, use the syntax:

```sql
DECIMAL(precision, scale)
```

Mathematical operations between `double` and `decimal` cause the decimal to be
converted to a double value automatically. Converting from the `decimal` data type
to any floating point type (`double`) may cause loss of precision.
