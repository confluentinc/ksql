---
layout: page
title: ksqlDB Custom Function Reference (UDF, UDAF, and UDTF)
tagline: Program user-defined functions in ksqlDB
description: Learn how to create customer functions that run in your ksqlDB queries 
---

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

Implement a Custom Function
---------------------------

Follow these steps to create your custom functions:

1.  Write your UDF, UDAF, or UDTF class in Java.

    - If your Java class is a UDF, mark it with the `@UdfDescription`
      and `@Udf` annotations.
    - If your class is a UDAF, mark it with the `@UdafDescription` and
      `@UdafFactory` annotations.
    - If your class is a UDTF, mark it with the `@UdtfDescription` and
     `@UdtfFactory` annotations.

    For more information, see [Example UDF class](#example-udf-class) and
    [Example UDAF class](#example-udaf-class).

2.  Deploy the JAR file to the `ksql` extensions directory. For more
    information, see [Deploying](#deploying).
3.  Use your function like any other ksqlDB function in your queries.

!!! tip
      The SHOW FUNCTIONS statement lists the available functions in your ksqlDB
      Server, including your custom UDF and UDAF functions. Use the DESCRIBE
      FUNCTION statement to display details about your custom functions.

For a detailed walkthrough on creating a UDF, see
[the how-to guide for creating a user-defined function](../../how-to-guides/create-a-user-defined-function).

### Creating UDFs, UDAFs, and UDTFs

ksqlDB supports creating User Defined Scalar Functions (UDFs), User Defined
Aggregate Functions (UDAFs), and User Defined Table Functions (UDTFs)
by using custom jars that are uploaded to the `ext/` directory of the ksqlDB
installation. At start up time, ksqlDB scans the jars in the directory looking
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

#### UDFs

To create a UDF you need to create a class that's annotated with
`@UdfDescription`. Each method in the class that represents a UDF must
be public and annotated with `@Udf`. The class you create represents a
collection of UDFs all with the same name but may have different
arguments and return types.

`@UdfParameter` annotations can be added to method parameters to provide
users with richer information, including the parameter schema. This
annotation is required if the SQL type can't be inferred from the Java
type, for example, `STRUCT`.

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

##### Example UDF class

The class below creates a UDF named `multiply`. The name of the UDF is
provided in the `name` parameter of the `UdfDescription` annotation.
This name is case-insensitive and is what can be used to call the UDF.
As can be seen this UDF can be invoked in different ways:

-   with two int parameters returning a long (BIGINT) result.
-   with two long (BIGINT) parameters returning a long (BIGINT) result.
-   with two nullable Long (BIGINT) parameters returning a nullable Long
    (BIGINT) result.
-   with two double parameters returning a double result.
-   with variadic double parameters returning a double result.

```java
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

@UdfDescription(name = "multiply", description = "multiplies 2 numbers")
public class Multiply {

  @Udf(description = "multiply two non-nullable INTs.")
  public long multiply(
    @UdfParameter(value = "V1", description = "the first value") final int v1,
    @UdfParameter(value = "V2", description = "the second value") final int v2) {
    return v1 * v2;
  }

  @Udf(description = "multiply two non-nullable BIGINTs.")
  public long multiply(
    @UdfParameter("V1") final long v1,
    @UdfParameter("V2") final long v2) {
    return v1 * v2;
  }

  @Udf(description = "multiply two nullable BIGINTs. If either param is null, null is returned.")
  public Long multiply(final Long v1, final Long v2) {
    return v1 == null || v2 == null ? null : v1 * v2;
  }

  @Udf(description = "multiply two non-nullable DOUBLEs.")
  public double multiply(final double v1, final double v2) {
    return v1 * v2;
  }

  @Udf(description = "multiply N non-nullable DOUBLEs.")
  public double multiply(final double... values) {
    return Arrays.stream(values).reduce((a, b) -> a * b);
  }
}
```

If you're using Gradle to build your UDF or UDAF, specify the
`ksqldb-udf` dependency:

```bash
compile 'io.confluent.ksql:ksqldb-udf:{{ site.cprelease }}'
```

To compile with the latest version of `ksqldb-udf`:

```bash
compile 'io.confluent.ksql:ksqldb-udf:+'
```

If you're using Maven to build your UDF or UDAF, specify the `ksqldb-udf`
dependency in your POM file:

```xml
<!-- Specify the repository for Confluent dependencies -->
    <repositories>
        <repository>
            <id>confluent</id>
            <url>http://packages.confluent.io/maven/</url>
        </repository>
    </repositories>

<!-- Specify the ksqldb-udf dependency -->
<dependencies>
    <dependency>
        <groupId>io.confluent.ksql</groupId>
        <artifactId>ksqldb-udf</artifactId>
        <version>{{ site.cprelease }}</version>
    </dependency>
</dependencies>
```

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

##### Configurable UDF

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

##### Example UDAF class

The following class creates a UDAF named `my_average`. The name of the UDAF
is provided in the `name` parameter of the `UdafDescription` annotation.
This name is case-insensitive and is what can be used to call the UDAF.

The class provides three factories that return a `TableUdaf`, one for
each of the input types Long, Integer, and Double. Moreover, it provides
a factory that returns a `Udaf` that doesn't support undo. Each method
defines a different type for the intermediate state based on the input
type (`I`), which in this case is a STRUCT consisting of two fields, the
SUM, of type `I`, and the COUNT, of type Long. To get the result of the
UDAF, each method implements a `map` function that returns the Double
division of the accumulated SUM and COUNT.

The UDAF can be invoked in four ways:

-   With a Long (BIGINT) column, returning the aggregated value as
    Double. Defines the schema for intermediate state type using the
    annotation parameter `parameterSchema`. The return type is
    `TableUdaf` and therefore supports the `undo` operation.
-   With an Integer column returning the aggregated value as Double.
    Likewise defines the schema of the Struct and supports undo.
-   With a Double column, returning the aggregated value as Double.
    Likewise defines the schema of the Struct and supports undo.
-   With a String (VARCHAR) column and an initializer that is a String
    (VARCHAR), returning the average String (VARCHAR) length as a
    Double.

```java
@UdafDescription(name = "my_average", description = "Computes the average.")
public class AverageUdaf {

  private static final String COUNT = "COUNT";
  private static final String SUM = "SUM";

  @UdafFactory(description = "Compute average of column with type Long.",
      aggregateSchema = "STRUCT<SUM bigint, COUNT bigint>")
  // Can be used with table aggregations
  public static TableUdaf<Long, Struct, Double> averageLong() {

    final Schema STRUCT_LONG = SchemaBuilder.struct().optional()
          .field(SUM, Schema.OPTIONAL_INT64_SCHEMA)
          .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
          .build();

    return new TableUdaf<Long, Struct, Double>() {

      @Override
      public Struct initialize() {
        return new Struct(STRUCT_LONG).put(SUM, 0L).put(COUNT, 0L);
      }

      @Override
      public Struct aggregate(final Long newValue,
                              final Struct aggregate) {

        if (newValue == null) {
          return aggregate;
        }
        return new Struct(STRUCT_LONG)
            .put(SUM, aggregate.getInt64(SUM) + newValue)
            .put(COUNT, aggregate.getInt64(COUNT) + 1);
      }

      @Override
      public Double map(final Struct aggregate) {
        final long count = aggregate.getInt64(COUNT);
        if (count == 0) {
          return 0.0;
        }
        return aggregate.getInt64(SUM) / ((double)count);
      }

      @Override
      public Struct merge(final Struct agg1,
                          final Struct agg2) {

        return new Struct(STRUCT_LONG)
            .put(SUM, agg1.getInt64(SUM) + agg2.getInt64(SUM))
            .put(COUNT, agg1.getInt64(COUNT) + agg2.getInt64(COUNT));
      }

      @Override
      public Struct undo(final Long valueToUndo,
                         final Struct aggregate) {

        return new Struct(STRUCT_LONG)
            .put(SUM, aggregate.getInt64(SUM) - valueToUndo)
            .put(COUNT, aggregate.getInt64(COUNT) - 1);
      }
    };
  }

  @UdafFactory(description = "Compute average of column with type Integer.",
      aggregateSchema = "STRUCT<SUM integer, COUNT bigint>")
  public static TableUdaf<Integer, Struct, Double> averageInt() {

    final Schema STRUCT_INT = SchemaBuilder.struct().optional()
          .field(SUM, Schema.OPTIONAL_INT32_SCHEMA)
          .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
          .build();

    return new TableUdaf<Integer, Struct, Double>() {

      @Override
      public Struct initialize() {
        return new Struct(STRUCT_INT).put(SUM, 0).put(COUNT, 0L);
      }

      @Override
      public Struct aggregate(final Integer newValue,
                              final Struct aggregate) {

        if (newValue == null) {
          return aggregate;
        }
        return new Struct(STRUCT_INT)
            .put(SUM, aggregate.getInt32(SUM) + newValue)
            .put(COUNT, aggregate.getInt64(COUNT) + 1);

      }

      @Override
      public Double map(final Struct aggregate) {
        final long count = aggregate.getInt64(COUNT);
        if (count == 0) {
          return 0.0;
        }
        return aggregate.getInt64(SUM) / ((double)count);
      }

      @Override
      public Struct merge(final Struct agg1,
                          final Struct agg2) {

        return new Struct(STRUCT_INT)
            .put(SUM, agg1.getInt32(SUM) + agg2.getInt64(SUM))
            .put(COUNT, agg1.getInt64(COUNT) + agg2.getInt64(COUNT));
      }

      @Override
      public Struct undo(final Integer valueToUndo,
                         final Struct aggregate) {

        return new Struct(STRUCT_INT)
            .put(SUM, aggregate.getInt32(SUM) - valueToUndo)
            .put(COUNT, aggregate.getInt64(COUNT) - 1);
      }
    };
  }

  @UdafFactory(description = "Compute average of column with type Double.",
      aggregateSchema = "STRUCT<SUM double, COUNT bigint>")
  public static TableUdaf<Double, Struct, Double> averageDouble() {

    final Schema STRUCT_DOUBLE = SchemaBuilder.struct().optional()
        .field(SUM, Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    return new TableUdaf<Double, Struct, Double>() {

      @Override
      public Struct initialize() {
        return new Struct(STRUCT_DOUBLE).put(SUM, 0.0).put(COUNT, 0L);
      }

      @Override
      public Struct aggregate(final Double newValue,
                              final Struct aggregate) {

        if (newValue == null) {
          return aggregate;
        }
        return new Struct(STRUCT_DOUBLE)
            .put(SUM, aggregate.getFloat64(SUM) + newValue)
            .put(COUNT, aggregate.getInt64(COUNT) + 1);

      }

      @Override
      public Double map(final Struct aggregate) {
        final long count = aggregate.getInt64(COUNT);
        if (count == 0) {
          return 0.0;
        }
        return aggregate.getFloat64(SUM) / ((double)count);
      }

      @Override
      public Struct merge(final Struct agg1,
                          final Struct agg2) {

        return new Struct(STRUCT_DOUBLE)
            .put(SUM, agg1.getFloat64(SUM) + agg2.getFloat64(SUM))
            .put(COUNT, agg1.getInt64(COUNT) + agg2.getInt64(COUNT));
      }

      @Override
      public Struct undo(final Double valueToUndo,
                         final Struct aggregate) {

        return new Struct(STRUCT_DOUBLE)
            .put(SUM, aggregate.getFloat64(SUM) - valueToUndo)
            .put(COUNT, aggregate.getInt64(COUNT) - 1);
      }
    };
  }

  // This method shows providing an initial value to an aggregated, i.e., it would be called
  // with my_average(col1, 'some_initial_value')
  @UdafFactory(description = "Compute average of length of strings",
      aggregateSchema = "STRUCT<SUM bigint, COUNT bigint>")
  public static Udaf<String, Struct, Double> averageStringLength(final String initialString) {

    final Schema STRUCT_LONG = SchemaBuilder.struct().optional()
          .field(SUM, Schema.OPTIONAL_INT64_SCHEMA)
          .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
          .build();

    return new Udaf<String, Struct, Double>() {

      @Override
      public Struct initialize() {
        return new Struct(STRUCT_LONG).put(SUM, (long) initialString.length()).put(COUNT, 1L);
      }

      @Override
      public Struct aggregate(final String newValue,
                              final Struct aggregate) {

        if (newValue == null) {
          return aggregate;
        }
        return new Struct(STRUCT_LONG)
            .put(SUM, aggregate.getInt64(SUM) + newValue.length())
            .put(COUNT, aggregate.getInt64(COUNT) + 1);
      }

      @Override
      public Double map(final Struct aggregate) {
        final long count = aggregate.getInt64(COUNT);
        if (count == 0) {
          return 0.0;
        }
        return aggregate.getInt64(SUM) / ((double)count);
      }

      @Override
      public Struct merge(final Struct agg1,
                          final Struct agg2) {

        return new Struct(STRUCT_LONG)
            .put(SUM, agg1.getInt64(SUM) + agg2.getInt64(SUM))
            .put(COUNT, agg1.getInt64(COUNT) + agg2.getInt64(COUNT));
      }
    };
  }
}
```

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

##### Example UDTF class

The following class creates a UDTF named `split_string`. The name of the UDTF
is provided in the `name` parameter of the `UdtfDescription` annotation. This
name is case-insensitive, and you can use it to call the UDTF.

UDTF methods must return a value of type `List<T>`, where `T` is any of the
supported SQL Java types.

You can invoke this UDTF in two different ways:

- with a single string containing the string to split;
- with a string containing the string to split and a regex to define
  the delimiter.

```java
import io.confluent.ksql.function.udf.Udtf;
import io.confluent.ksql.function.udf.UdtfDescription;

@UdtfDescription(name = "split_string", description = "splits a string into words")
public class SplitString {

  @Udtf(description="Splits a string into words")
  public List<String> split(String input) {
    return Arrays.asList(String.split("\\s+"));
  }

  @Udtf(description="Splits a string into words")
  public List<String> split(String input, String delimRegex) {
    return Arrays.asList(String.split(delimRegex));
  }
}
```

If you're using Gradle to build your UDF or UDAF, specify the `ksqldb-udf`
dependency:

```bash
compile 'io.confluent.ksql:ksqldb-udf:{{ site.cprelease }}'
```

To compile with the latest version of `ksqldb-udf`:

```bash
compile 'io.confluent.ksql:ksqldb-udf:+'
```

If you're using Maven to build your function, specify the `ksqldb-udf`
dependency in your POM file:

```xml
<!-- Specify the repository for Confluent dependencies -->
    <repositories>
        <repository>
            <id>confluent</id>
            <url>http://packages.confluent.io/maven/</url>
        </repository>
    </repositories>

<!-- Specify the ksqldb-udf dependency -->
<dependencies>
    <dependency>
        <groupId>io.confluent.ksql</groupId>
        <artifactId>ksqldb-udf</artifactId>
        <version>{{ site.cprelease }}</version>
    </dependency>
</dependencies>
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

### Supported Types

ksqlDB supports the following Java types for UDFs, UDAFs, and UDTFs.

| Java Type  | SQL Type  |
| ---------- | --------- |
| int        | INTEGER   |
| Integer    | INTEGER   |
| boolean    | BOOLEAN   |
| Boolean    | BOOLEAN   |
| long       | BIGINT    |
| Long       | BIGINT    |
| double     | DOUBLE    |
| Double     | DOUBLE    |
| String     | VARCHAR   |
| List       | ARRAY     |
| Map        | MAP       |
| Struct     | STRUCT    |
| BigDecimal | DECIMAL   |

!!! note
    Using `Struct` or `BigDecimal` in your functions requires specifying the
    schema by using `paramSchema`, `returnSchema`, `aggregateSchema`, or a
    schema provider.

### Deploying

To deploy your user defined functions, you create a jar containing all of the
classes required by the functions. If you depend on third-party libraries,
this should be an uber-jar containing these libraries. Once the jar
is created, deploy it to each ksqlDB server instance. Copy the jar
to the `ext/` directory that's part of the ksqlDB distribution. The `ext/`
 directory can be configured via the `ksql.extension.dir`.

The jars in the `ext/` directory are scanned only at start-up, so you
must restart your ksqlDB Server instances to pick up new and updated UD(A)Fs.

It s important to ensure that you deploy the custom jars to each server
instance. Failure to do so results in errors when processing any
statements that try to use these functions. The errors may go unnoticed
in the ksqlDB CLI if the ksqlDB Server instance it is connected to has the
jar installed, but one or more other ksqlDB servers don't have it
installed. In these cases, the errors will appear in the ksqlDB Server log
(ksql.log) . The error would look something like:

```
[2018-07-04 12:37:28,602] ERROR Failed to handle: Command{statement='create stream pageviews_ts as select tostring(viewtime) from pageviews;', overwriteProperties={}} (io.confluent.ksql.rest.server.computation.InteractiveStatementExecutor:218)
io.confluent.ksql.util.KsqlException: Can't find any functions with the name 'TOSTRING'
```

The servers that don't have the jars don't process any queries using
the custom UD(A)Fs. Processing will continue, but it's restricted
to only the servers with the correct jars installed.

### Usage

Once your functions are deployed, you can call them in the same way you
would invoke any of the ksqlDB built-in functions. The function names are
case-insensitive. For example, using the `multiply` example:

```sql
CREATE STREAM number_stream (int1 INT, int2 INT, long1 BIGINT, long2 BIGINT)
  WITH (VALUE_FORMAT = 'JSON', KAFKA_TOPIC = 'numbers');

SELECT multiply(int1, int2), MULTIPLY(long1, long2) FROM number_stream EMIT CHANGES;
```

### ksqlDB Custom Functions and Security

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

### Metric Collection

Metric collection can be enabled by setting the config
`ksql.udf.collect.metrics` to `true`. This defaults to `false` and is
generally not recommended for production usage, as metrics are
collected on each invocation and introduce some overhead to
processing time. See more details in the
[UDF metrics reference section](../../reference/metrics/#user-defined-functions).

### Suggested Reading

- [Data Enrichment in ksqlDB Using UDTFs](https://www.confluent.io/blog/infrastructure-monitoring-with-ksqldb-udtf/)
- [ksqlDB UDFs and UDAFs Made Easy](https://www.confluent.io/blog/kafka-ksql-udf-udaf-with-maven-made-easy/)
- [How to Build a UDF and/or UDAF in ksqlDB 5.0](https://www.confluent.io/blog/build-udf-udaf-ksql-5-0)
