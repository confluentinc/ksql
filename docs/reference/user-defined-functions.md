ksqlDB has many built-in functions that help with processing records in
streaming data, like ABS and SUM. Functions are used within a SQL query
to filter, transform, or aggregate data.

With the ksqlDB API, you can implement custom functions that go beyond the
built-in functions. For example, you can create a custom function that
applies a pre-trained machine learning model to a stream.

ksqlDB supports these kinds of functions:

Stateless scalar function (UDF)
:   A scalar function that takes one input row and returns one output
    value. No state is retained between function calls. When you
    implement a custom scalar function, it's called a *User-Defined
    Function (UDF)*. For more information, see
    [Scalar Function](../developer-guide/ksqldb-reference/scalar-functions.md).

Stateful aggregate function (UDAF)
:   An aggregate function that takes *N* input rows and returns one
    output value. During the function call, state is retained for all
    input records, which enables aggregating results. When you implement
    a custom aggregate function, it's called a *User-Defined Aggregate
    Function (UDAF)*. For more information, see
    [Aggregate Function](../developer-guide/ksqldb-reference/aggregate-functions.md).

Table function (UDTF)
:   A table function that takes one input row and returns zero or more
    output rows. No state is retained between function calls. When you
    implement a custom table function, it's called a *User-Defined Table
    Function (UDTF)*. For more information, see
    [Table Function](../developer-guide/ksqldb-reference/table-functions.md).

## Data type mapping

ksqlDB supports the following Java types for UDFs, UDAFs, and UDTFs.

| Java Type                             | SQL Type  |
|---------------------------------------|-----------|
| `int`                                 | `INT` |
| `java.lang.Integer`                   | `INT` |
| `boolean`                             | `BOOLEAN` |
| `java.lang.Boolean`                   | `BOOLEAN` |
| `long`                                | `BIGINT`  |
| `java.lang.Long`                      | `BIGINT`  |
| `double`                              | `DOUBLE`  |
| `java.lang.Double`                    | `DOUBLE`  |
| `java.math.BigDecimal`                | `DECIMAL` |
| `java.lang.String`                    | `VARCHAR` |
| `java.util.List`                      | `ARRAY`   |
| `java.util.Map`                       | `MAP`     |
| `org.apache.kafka.connect.data.Struct`| `STRUCT`  |

!!! note
    Using `Struct` or `BigDecimal` in your functions requires specifying the
    schema by using `paramSchema`, `returnSchema`, `aggregateSchema`, or a
    schema provider.

## Classloading

At start up time, ksqlDB scans the jars in the directory looking
for any classes that annotated with `@UdfDescription` (UDF), `@UdafDescription`
(UDAF), or `@UdtfDescription` (UDTF).

- Classes annotated with `@UdfDescription` are scanned for any public methods
  that are annotated with `@Udf`.
- Classes annotated with `@UdafDescription` are scanned for any public static
  methods that are annotated with `@UdafFactory`.
- Classes annotated with ``@UdtfDescription`` are scanned for any public
  methods that are annotated with ``@Udtf``.
  
Each function that is found is parsed and, if successful, loaded into ksqlDB.

Each function instance has its own child-first `ClassLoader` that is
isolated from other functions. If you need to use any third-party
libraries with your functions, they should also be part of your jar,
which means that you should create an "uber-jar". The classes in your
uber-jar are loaded in preference to any classes on the ksqlDB classpath,
excluding anything vital to the running of ksqlDB, i.e., classes that are
part of `org.apache.kafka` and `io.confluent`. Further, the
`ClassLoader` can restrict access to other classes via a blacklist. The
blacklist file is `resource-blacklist.txt`. You can add any classes or
packages that you want blacklisted from UDF use. For example you may not
want a UDF to be able to fork processes. Further details on how to
blacklist are available below.

## Annotations

##### UdfDescription Annotation

The `@UdfDescription` annotation is applied at the class level and has
four fields, two of which are required. The information provided here is
used by the `SHOW FUNCTIONS` and `DESCRIBE FUNCTION <function>`
commands.

| Field       | Description                                                          | Required |
|-------------|----------------------------------------------------------------------|----------|
| name        | The case-insensitive name of the UDF(s) represented by this class.   | Yes      |
| description | A string describing generally what the function(s) in this class do. | Yes      |
| category    | For grouping similar functions in the output of SHOW FUNCTIONS.      | No       |
| author      | The author of the UDF.                                               | No       |
| version     | The version of the UDF.                                              | No       |

##### Udf Annotation

The `@Udf` annotation is applied to public methods of a class annotated
with `@UdfDescription`. Each annotated method will become an invocable function
in SQL. This annotation supports the following fields:

| Field         | Description                  | Required               |
|---------------|------------------------------|------------------------|
| description   | A string describing generally what a particular version of the UDF does (see the following example). | No |
| schema        | The ksqlDB schema for the return type of this UDF.  | For complex types such as STRUCT if `schemaProvider` is not passed in.   |
| schemaProvider| A reference to a method that computes the return schema of this UDF. For more info, see [Dynamic return type](#dynamic-return-type). | For complex types,  like STRUCT, if `schema` is not provided.  |

```java
@Udf(description = "Returns a substring of str that starts at pos"
  + " and continues to the end of the string")
public String substring(final String str, final int pos)

@Udf(description = "Returns a substring of str that starts at pos and is of length len")
public String substring(final String str, final int pos, final int len)
```

##### UdfParameter Annotation

The `@UdfParameter` annotation is applied to parameters of methods
annotated with `@Udf`. ksqlDB uses the additional information in the
`@UdfParameter` annotation to specify the parameter schema (if it can't
be inferred from the Java type) or to provide users with richer
information about the method when, for example, they execute
`DESCRIBE FUNCTION` on the method.

| Field       | Description                                                 | Required                                                                        |
|-------------|-------------------------------------------------------------|---------------------------------------------------------------------------------|
| value       | The case-insensitive name of the parameter                  | Required if the UDF JAR was not compiled with the `-parameters` javac argument. |
| description | A string describing generally what the parameter represents | No                                                                              |
| schema      | The ksqlDB schema for the parameter.                          | For complex types, like STRUCT                                                |

!!! note
      If `schema` is supplied in the `@UdfParameter` annotation for a `STRUCT`
      it is considered "strict" - any inputs must match exactly, including
      order and names of the fields.

```java
@Udf
public String substring(
   @UdfParameter("str") final String str,
   @UdfParameter(value = "pos", description = "Starting position of the substring") final int pos)

@Udf
public boolean livesInRegion(
   @UdfParameter(value = "zipcode", description = "a US postal code") final String zipcode,
   @UdfParameter(schema = "STRUCT<ZIP STRING, NAME STRING>") final Struct employee)
```

If your Java8 class is compiled with the `-parameters` compiler flag,
the name of the parameter will be inferred from the method declaration.

##### UdafDescription Annotation

The `@UdafDescription` annotation is applied at the class level and has
four fields, two of which are required. The information provided here is
used by the `SHOW FUNCTIONS` and `DESCRIBE FUNCTION <function>`
commands.

| Field       | Description                                                          | Required |
|-------------|----------------------------------------------------------------------|----------|
| name        | The case-insensitive name of the UDAF(s) represented by this class.   | Yes      |
| description | A string describing generally what the function(s) in this class do. | Yes      |
| author      | The author of the UDF.                                               | No       |
| version     | The version of the UDF.                                              | No       |

##### UdafFactory Annotation

The `@UdafFactory` annotation is applied to public static methods of a
class annotated with `@UdafDescription`. The method must return either
`Udaf`, or, if it supports table aggregations, `TableUdaf`. Each
annotated method is a factory for an invocable aggregate function in
SQL. The annotation supports the following fields:

| Field           | Description                                                    | Required                       |
|-----------------|----------------------------------------------------------------|--------------------------------|
| description     | A string describing generally what the function(s) in this class do. | Yes                            |
| paramSchema     | The ksqlDB schema for the input parameter.                       | For complex types, like STRUCT |
| aggregateSchema | The ksqlDB schema for the intermediate state.                    | For complex types, like STRUCT |
| returnSchema    | The ksqlDB schema for the return value.                          | For complex types, like STRUCT |

!!! note
      If `paramSchema` , `aggregateSchema` or `returnSchema` is supplied in
      the `@UdfParameter` annotation for a `STRUCT`, it's considered
      "strict" - any inputs must match exactly, including order and names of
      the fields.

You can use this to better describe what a particular version of the UDAF
does, for example:

```java
@UdafFactory(description = "Compute average of column with type Long.",
          aggregateSchema = "STRUCT<SUM bigint, COUNT bigint>")
public static TableUdaf<Long, Struct, Double> averageLong(){...}

@@UdafFactory(description = "Compute average of length of strings",
           aggregateSchema = "STRUCT<SUM bigint, COUNT bigint>")
public static Udaf<String, Struct, Double> averageStringLength(final String initialString){...}
```

##### UdtfDescription Annotation

The `@UdtfDescription` annotation is applied at the class level and has four
fields, two of which are required. The information provided here is used by
the `SHOW FUNCTIONS` and `DESCRIBE FUNCTION <function>` commands.

|    Field    |                             Description                              | Required |
| ----------- | -------------------------------------------------------------------- | -------- |
| name        | The case-insensitive name of the UDTF(s) represented by this class.  | Yes      |
| description | A string describing generally what the function(s) in this class do. | Yes      |
| author      | The author of the UDTF.                                              | No       |
| version     | The version of the UDTF.                                             | No       |

##### Udtf Annotation

The `@Udtf` annotation is applied to public methods of a class annotated with
`@UdtfDescription`. Each annotated method becomes an invocable function in
SQL. This annotation supports the following fields:

|     Field      |                                                   Description                                                   |                                Required                                 |
| -------------- | --------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| description    | A string describing generally what a particular version of the UDTF does (see example).                         | No                                                                      |
| schema         | The ksqlDB schema for the return type of this UDTF.                                                               | For complex types such as STRUCT if `schemaProvider` is  not passed in. |
| schemaProvider | A reference to a method that computes the return schema of this UDTF. (See Dynamic Return Types for more info). | For complex types such as STRUCT if ``schema`` is not passed in.        |

##### Annotating UDTF Parameters

You can use the `@UdfParameter` annotation to provide extra information for
UDTF parameters. This is the same annotation as used for UDFs. Please see the
earlier documentation on this for further information.

## Null values

## Dynamic types

## Generics

## External parameters

If the UDF class needs access to the ksqlDB Server configuration it can
implement `org.apache.kafka.common.Configurable`, for example:

```java
@UdfDescription(name = "MyFirstUDF", description = "multiplies 2 numbers")
public class SomeConfigurableUdf implements Configurable {
  private String someSetting = "a.default.value";

  @Override
  public void configure(final Map<String, ?> map) {
    this.someSetting = (String)map.get("ksql.functions.myfirstudf.some.setting");
  }

  ...
}
```

For security reasons, only settings whose name is prefixed with
`ksql.functions.<lowercase-udfname>.` or `ksql.functions._global_.` are
propagated to the UDF.

## Security

#### Blacklisting

In some deployment environments, it may be necessary to restrict the
classes that UD(A)Fs have access to, as they may represent a security
risk. To reduce the attack surface of ksqlDB user defined functions you can
optionally blacklist classes and packages so that they can't be used from a
UD(A)F. An example blacklist is in a file named `resource-blacklist.txt`
in the `ext/` directory. All of the entries in the default version of the
file are commented out, but it shows how you can use the blacklist.

This file contains one entry per line, where each line is a class or
package that should be blacklisted. The matching of the names is based
on a regular expression, so if you have an entry, `java.lang.Process` like
this:

```
java.lang.Process
```

This matches any paths that begin with `java.lang.Process`, like
`java.lang.Process`, `java.lang.ProcessBuilder`, *etc*.

If you want to blacklist a single class, for example,
`java.lang.Compiler`, then you would add:

```
java.lang.Compiler$
```

Any blank lines or lines beginning with `#` are ignored. If the file is
not present, or is empty, then no classes are blacklisted.

#### Security Manager

By default, ksqlDB installs a simple Java security manager for executing
user defined functions. The security manager blocks attempts by any functions
to fork processes from the ksqlDB Server. It also prevents them from calling
`System.exit(..)`.

You can disable the security manager by setting
`ksql.udf.enable.security.manager` to `false`.

#### Disabling ksqlDB Custom Functions

You can disable the loading of all UDFs in the `ext/` directory by
setting `ksql.udfs.enabled` to `false`. By default, they are enabled.

## Metrics

Metric collection can be enabled by setting the config
`ksql.udf.collect.metrics` to `true`. This defaults to `false` and is
generally not recommended for production usage, as metrics are
collected on each invocation and introduce some overhead to
processing time. See more details in the
[UDF metrics reference section](../../reference/metrics/#user-defined-functions).