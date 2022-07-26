---
layout: page
title: User-defined functions (UDFs) 
tagline: Custom ksqlDB functions
description: Extend ksqlDB's suite of built-in functions using Java hooks
keywords: function, aggregation, table 
---

[User-defined functions](/concepts/functions) enable you to extend ksqlDB's
suite of built-in functions using Java hooks. This section is a reference for
how they work. Use [the how-to guide](/how-to-guides/create-a-user-defined-function)
to learn how to use them.

## Data type mapping

Because SQL has a type system that is independent from Java’s, user-defined
functions (UDFs) need to use specific Java types so that ksqlDB can manage
the correspondence from SQL to Java. Below is the mapping to use for all UDF
parameters and return types. Use boxed types when you want to tolerate null
values.

| SQL Type  | Java Type                             |
|-----------|---------------------------------------|
| `INT`     | `int`, `java.lang.Integer`            |
| `BOOLEAN` | `boolean`, `java.lang.Boolean`        |
| `BIGINT`  | `long`, `java.lang.Long`              |
| `DOUBLE`  | `double`, `java.lang.Double`          |
| `DECIMAL` | `java.math.BigDecimal`                |
| `VARCHAR` | `java.lang.String`                    |
| `BYTES`   | `java.nio.ByteBuffer`                 |
| `TIME`    | `java.sql.Time`                       |
| `DATE`    | `java.sql.Date`                       |
|`TIMESTAMP`| `java.sql.Timestamp`                  |
| `ARRAY`   | `java.util.List`                      |
| `MAP`     | `java.util.Map`                       |
| `STRUCT`  | `org.apache.kafka.connect.data.Struct`|
| `BYTES`   | `java.nio.ByteBuffer`                 |

!!! note
    Using `Struct` or `BigDecimal` in your functions requires specifying the
    schema by using `paramSchema`, `returnSchema`, `aggregateSchema`, or a
    schema provider.

## Classloading

How does ksqlDB choose which classes to load as user-defined functions?
At start up time, ksqlDB scans the jars in its extensions directory looking
for classes with UDF [annotated](#annotations). Each function that is found is
parsed and, if successful, loaded into ksqlDB.

Each function instance has its own child-first `ClassLoader` that is
isolated from other functions. If you need to use any third-party
libraries with your functions, they should also be part of your jar,
which means that you should create an uberjar. The classes in your
uberjar are loaded in preference to any classes on the ksqlDB classpath,
excluding anything vital to the running of ksqlDB, i.e., classes that are
part of `org.apache.kafka` and `io.confluent`.

## Annotations

[Annotations](https://docs.oracle.com/javase/tutorial/java/annotations/basics.html)
not only help ksqlDB figure out which classes will be used as UDFs,
they also help commands like `DESCRIBE FUNCTION` display helpful metadata.

### Scalar functions

When a class is annotated with `@UdfDescription`, it's scanned for any public methods
that are annotated with `@Udf`. If it matches, the class is loaded as a scalar function.
Each method's parameters may optionally be annotated with `@UdfParameter`. Here is what
each of these annotations can be parameterized with.

#### `@UdfDescription`

The `@UdfDescription` annotation is applied at the class level.

| Field       | Description                                                          | Required |
|-------------|----------------------------------------------------------------------|----------|
| name        | The case-insensitive name of the UDF(s) represented by this class.   | Yes      |
| description | A string describing generally what the function(s) in this class do. | Yes      |
| category    | For grouping similar functions in the output of `SHOW FUNCTIONS.`    | No       |
| author      | The author of the UDF.                                               | No       |
| version     | The version of the UDF.                                              | No       |

#### `@Udf`

The `@Udf` annotation is applied to public methods of a class annotated
with `@UdfDescription`. Each annotated method will become an invocable function
in SQL.

| Field         | Description                  | Required               |
|---------------|------------------------------|------------------------|
| description   | A string describing generally what a particular version of the UDF does. | No |
| schema        | The ksqlDB schema for the return type of this UDF.  | For complex types such as `STRUCT` if `schemaProvider` is not passed in.   |
| schemaProvider| A reference to a method that computes the return schema of this UDF (e.g. dynamic return type). | For complex types,  like `STRUCT`, if `schema` is not provided.  |

#### `@UdfParameter`

The `@UdfParameter` annotation is applied to parameters of methods
annotated with `@Udf`. ksqlDB uses the information in the
`@UdfParameter` annotation to specify the parameter schema (if it can't
be inferred from the Java type) and to convey metadata.

| Field       | Description                                                 | Required                                                                        |
|-------------|-------------------------------------------------------------|---------------------------------------------------------------------------------|
| value       | The case-insensitive name of the parameter                  | Required if the UDF JAR was not compiled with the `-parameters` javac argument. |
| description | A string describing generally what the parameter represents | No                                                                              |
| schema      | The ksqlDB schema for the parameter.                          | For complex types, like `STRUCT`                                              |

!!! note
      If `schema` is supplied in the `@UdfParameter` annotation for a `STRUCT`
      it is considered "strict" - any inputs must match exactly, including
      order and names of the fields.

If your Java 8 class is compiled with the `-parameters` compiler flag,
the name of the parameter will be inferred from the method declaration.

### Tabular functions

When a class is annotated with `@UdtfDescription`, it's scanned for any public methods
that are annotated with `@Udtf`. If it matches, the class is loaded as a tabular function.
Each method's parameters may optionally be annotated with `@UdfParameter`. Here is what
each of these annotations can be parameterized with.

#### `@UdtfDescription`

The `@UdtfDescription` annotation is applied at the class level.

|    Field    |                             Description                              | Required |
| ----------- | -------------------------------------------------------------------- | -------- |
| name        | The case-insensitive name of the UDTF(s) represented by this class.  | Yes      |
| description | A string describing generally what the function(s) in this class do. | Yes      |
| author      | The author of the UDTF.                                              | No       |
| version     | The version of the UDTF.                                             | No       |

#### `@Udtf`

The `@Udtf` annotation is applied to public methods of a class annotated with
`@UdtfDescription`. Each annotated method becomes an invocable function in
SQL. This annotation supports the following fields:

|     Field      |                                                   Description                                                   |                                Required                                 |
| -------------- | --------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| description    | A string describing generally what a particular version of the UDTF does.                         | No                                                                      |
| schema         | The ksqlDB schema for the return type of this UDTF.                                                               | For complex types such as `STRUCT` if `schemaProvider` is  not passed in. |
| schemaProvider | A reference to a method that computes the return schema of this UDTF. (e.g. dynamic return type). | For complex types such as `STRUCT` if ``schema`` is not passed in.        |

#### `@UdfParameter`

You can use the `@UdfParameter` annotation to provide extra information for
UDTF parameters. This is the same annotation as used for UDFs. Please see the
earlier documentation on this for further information.

### Aggregation functions

When a class is annotated with `@UdafDescription`, it's scanned for any public static methods that are annotated with `@UdafFactory` that return either `Udaf` or `TableUdaf`. If it matches, the class is loaded as an aggregation function. The factory function represents a collection of UDAFs all with the same name but
may have different arguments and return types. Here is what each of these annotations can be parameterized with.

Both `Udaf` and `TableUdaf` are parameterized by three generic types: `I` is the
input type of the UDAF. `A` is the data type of the intermediate storage
used to keep track of the state of the UDAF. `O` is the data type of the
return value. Decoupling the data types of the state and return value
enables you to define UDAFs like `average`, as shown in the following example.

When you create a UDAF, you can use the `map` method to provide the logic that
transforms an intermediate aggregate value to the returned value.

The `merge` method is only called when merging sessions when session windowing is used.

#### `@UdafDescription`

The `@UdafDescription` annotation is applied at the class level.

| Field       | Description                                                          | Required |
|-------------|----------------------------------------------------------------------|----------|
| name        | The case-insensitive name of the UDAF(s) represented by this class.  | Yes      |
| description | A string describing generally what the function(s) in this class do. | Yes      |
| author      | The author of the UDF.                                               | No       |
| version     | The version of the UDF.                                              | No       |

#### `@UdafFactory`

The `@UdafFactory` annotation is applied to public static methods of a
class annotated with `@UdafDescription`. The method must return either
`Udaf`, or, if it supports table aggregations, `TableUdaf`. Each
annotated method is a factory for an invocable aggregate function in
SQL. The annotation supports the following fields:

| Field           | Description                                                    | Required                       |
|-----------------|----------------------------------------------------------------|--------------------------------|
| description     | A string describing generally what the function(s) in this class do. | Yes                            |
| paramSchema     | The ksqlDB schema for the input parameter.                       | For complex types, like `STRUCT` |
| aggregateSchema | The ksqlDB schema for the intermediate state.                    | For complex types, like `STRUCT` |
| returnSchema    | The ksqlDB schema for the return value.                          | For complex types, like `STRUCT` |

!!! note
      If `paramSchema` , `aggregateSchema` or `returnSchema` is supplied in
      the `@UdfParameter` annotation for a `STRUCT`, it's considered
      "strict" - any inputs must match exactly, including order and names of
      the fields.

## Null values

If a user defined function uses primitive types in its signature it is indicating that the
parameter should never be `null`. Conversely, using boxed types indicates
the function can accept `null` values for the parameter. It's up to the
implementer of the UDF to choose which is the more appropriate. A common
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

ksqlDB checks the value that's passed to each parameter and
reports an error to the server log for any `null` values being passed to a
primitive type. The associated column in the output row will be `null`.

## Dynamic types

UDFs support dynamic return types that are resolved at runtime. This is
useful if you want to implement a UDF with a non-deterministic return
type, like `DECIMAL` or `STRUCT`. For example, a UDF that returns
`BigDecimal`, which maps to the SQL `DECIMAL` type, may vary the
precision and scale of the output based on the input schema.

To use this functionality, you need to specify a method with signature
`public SqlType <your-method-name>(final List<SqlArgument> params)` and
annotate it with `@UdfSchemaProvider`. Also, you need to link it to the
corresponding UDF by using the `schemaProvider=<your-method-name>`
parameter of the `@Udf` annotation.

When implementing dynamic returns for a UDTF function, if your method returns
a value of type `List<T>`, the type referred to by the schema provider method
is the type `T`, not the type `List<T>`.

For dynamic UDAFs, the `aggregate` or `map` methods may depend on the input SQL type, so
implementations of the `Udaf` interface override some of the following three methods:
`initializeTypeArguments(List<SqlArgument> argTypeList)`, `getAggregateSqlType()`, and 
`getReturnSqlType()`.

## Generics

A UDF declaration can utilize generics if they match the following
conditions:

1. Any generic in the return value of a method must appear in at least
  one of the method parameters

1. The generic must not adhere to any interface. For example,
  `<T extends Number>` is not valid.

1. The generic does not support type coercion or inheritance. For
  example, `add(T a, T b)` will accept `BIGINT, BIGINT` but not
  `INT, BIGINT`.

## External parameters

If the UDF class needs access to the ksqlDB Server configuration, it can
implement `org.apache.kafka.common.Configurable`. `configure()` will be
invoked with the map of server parameters. This can be useful for parameterizing
a function on a per-deployment basis.

For security reasons, only settings whose name is prefixed with
`ksql.functions.<lowercase-udfname>.` or `ksql.functions._global_.` are
propagated to the UDF.

## Security

### Blacklisting

In some deployment environments, it may be necessary to restrict the
classes that UDFs have access to, as they may represent a security
risk. To reduce the attack surface of ksqlDB user defined functions you can
optionally blacklist classes and packages so that they can't be used from a
UDF. An example blacklist is in a file named `resource-blacklist.txt`
in the extensions directory. All of the entries in the default version of the
file are commented out, but it shows how you can use the blacklist.

This file contains one entry per line, where each line is a class or
package that should be blacklisted. The matching of the names is based
on a regular expression, so if you have an entry, `java.lang.Process` like
this:

```
java.lang.Process
```

This matches any paths that begin with `java.lang.Process`, like
`java.lang.Process`, `java.lang.ProcessBuilder`, etc.

If you want to blacklist a single class, for example,
`java.lang.Compiler`, then you would add:

```
java.lang.Compiler$
```

Any blank lines or lines beginning with `#` are ignored. If the file is
not present, or is empty, then no classes are blacklisted.

### Security Manager

By default, ksqlDB installs a simple Java security manager for executing
user defined functions. The security manager blocks attempts by any functions
to fork processes from the ksqlDB Server. It also prevents them from calling
`System.exit(..)`.

You can disable the security manager by setting
`ksql.udf.enable.security.manager` to `false`.

### Disabling ksqlDB Custom Functions

You can disable the loading of all UDFs in the extensions directory by
setting `ksql.udfs.enabled` to `false`. By default, they are enabled.

## Metrics

Metric collection can be enabled by setting the config
`ksql.udf.collect.metrics` to `true`. This defaults to `false` and is
generally not recommended for production usage, as metrics are
collected on each invocation and introduce some overhead to
processing time. See more details in the
[UDF metrics reference section](../../reference/metrics/#user-defined-functions).
