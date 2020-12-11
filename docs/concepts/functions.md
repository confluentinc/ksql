---
layout: page
title: ksqlDB - User-defined functions
tagline: Program user-defined functions in ksqlDB
description: Learn how to create customer functions that run in your ksqlDB queries 
---

# User-defined functions



##### Null Handling

If a UDF uses primitive types in its signature it is indicating that the
parameter should never be `null`. Conversely, using boxed types indicates
the function can accept `null` values for the parameter. It's up to the
implementor of the UDF to chose which is the more appropriate. A common
pattern is to return `null` if the input is `null`, though generally
this is only for parameters that are expected to be supplied from the
source row being processed.

For example, a `substring(String str, int pos)` UDF might return `null`
if `str` is `null`, but a `null` value for the `pos` parameter would be
treated as an error, and so should be a primitive. In fact, the built-in
substring is more lenient and would return `null` if `pos` is `null`).

The return type of a UDF can also be a primitive or boxed type. A
primitive return type indicates the function will never return `null`,
whereas a boxed type indicates that it may return `null`.

The ksqlDB Server checks the value that's passed to each parameter and
reports an error to the server log for any `null` values being passed to a
primitive type. The associated column in the output row will be `null`.

##### Dynamic return type

UDFs support dynamic return types that are resolved at runtime. This is
useful if you want to implement a UDF with a non-deterministic return
type, like `DECIMAL` or `STRUCT`. For example, a UDF that returns
`BigDecimal`, which maps to the SQL `DECIMAL` type, may vary the
precision and scale of the output based on the input schema.

To use this functionality, you need to specify a method with signature
`public SqlType <your-method-name>(final List<SqlType> params)` and
annotate it with `@UdfSchemaProvider`. Also, you need to link it to the
corresponding UDF by using the `schemaProvider=<your-method-name>`
parameter of the `@Udf` annotation.

##### Generics in UDFs

A UDF declaration can utilize generics if they match the following
conditions:

-   Any generic in the return value of a method must appear in at least
    one of the method parameters
-   The generic must not adhere to any interface. For example,
    `<T extends Number>` is not valid).
-   The generic does not support type coercion or inheritance. For
    example, `add(T a, T b)` will accept `BIGINT, BIGINT` but not
    `INT, BIGINT`.




#### UDAFs

To create a UDAF you need to create a class that's annotated with
`@UdafDescription`. Each method in the class that's used as a factory
for creating an aggregation must be `public static`, be annotated with
`@UdafFactory`, and must return either `Udaf` or `TableUdaf`. The class
you create represents a collection of UDAFs all with the same name but
may have different arguments and return types.

Both `Udaf` and `TableUdaf` are parameterized by three types: `I` is the
input type of the UDAF. `A` is the data type of the intermediate storage
used to keep track of the state of the UDAF. `O` is the data type of the
return value. Decoupling the data types of the state and return value
enables you to define UDAFs like `average`, as shown in the following example.

When you create a UDAF, use the `map` method to provide the logic that
transforms an intermediate aggregate value to the returned value.

The `merge` method is only called when merging sessions when session windowing is used.



#### UDTFs

To create a UDTF you need to create a class that is annotated with
`@UdtfDescription`. Each method in the class that represents a UDTF must be
public and annotated with `@Udtf`. The class you create represents a collection
of UDTFs all with the same name but may have different arguments and return
types.

`@UdfParameter` annotations can be added to method parameters to provide users
with richer information, including the parameter schema. This annotation is
required if the SQL type can't be inferred from the Java type, like `STRUCT`.

For an example UDTF implementation, see
[Data Enrichment in ksqlDB Using UDTFs](https://www.confluent.io/blog/infrastructure-monitoring-with-ksqldb-udtf/).

##### Null Handling

If a UDTF uses primitive types in its signature, this indicates that the
parameter should never be `null`. Conversely, using boxed types indicates that
the function can accept `null` values for the parameter. It's up to the
implementer of the UDTF to chose which is the most appropriate. A common
pattern is to return `null` if the input is `null`, though generally this is
only for parameters that are expected to be supplied from the source row being
processed.

For example, a `substring(String str, int pos)` UDF might return null if `str`
is null, but a null `pos` parameter would be treated as an error and so should
be a primitive. The built-in `substring` function is more lenient and return
`null` if pos is `null`.

The return type of a UDTF can also be a primitive or boxed type. A primitive
return type indicates that the function never returns `null`, and a boxed type
indicates that it may return `null`.

ksqlDB server checks the value being passed to each parameter and reports an
error to the server log for any `null` values being passed to a primitive type.
The associated column in the output row will be `null`.

##### Dynamic return type

UDTFs support dynamic return types that are resolved at runtime. This is useful
if you want to implement a UDTF with a non-deterministic return type. For
example, a UDTF that returns a `BigDecimal` may vary the precision and scale
of the output based on the input schema.

To use this functionality, specify a method with signature
`public SqlType <your-method-name>(final List<SqlType> params)` and annotate it
with `@UdfSchemaProvider`. Also, you need to link it to the corresponding UDF by
using the `schemaProvider=<your-method-name>` parameter of the `@Udtf`
annotation.

If your UDTF method returns a value of type `List<T>`, the type referred to
by the schema provider method is the type `T`, not the type `List<T>`.





