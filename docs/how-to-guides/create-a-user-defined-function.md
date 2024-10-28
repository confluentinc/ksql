---
layout: page
title: How to create a user-defined function 
tagline: Create a custom ksqlDB function
description: Extend ksqlDB to apply custom logic in your queries
keywords: function, scalar, tabular, aggregation
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/how-to-guides/create-a-user-defined-function.html';
</script>

# How to create a user-defined function

## Context

You have a piece of logic for transforming or aggregating events that
ksqlDB can't currently express. You want to extend ksqlDB to apply
that logic in your queries. To do that, ksqlDB exposes hooks through
Java programs. This functionality is broadly called *user-defined
functions*, or UDFs for short.

## In action

```java
package com.example;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

import java.util.Map;

@UdfDescription(name = "formula",
                author = "example user",
                version = "1.0.2",
                description = "A custom formula for important business logic.")
public class FormulaUdf {

    @Udf(description = "The standard version of the formula with integer parameters.")
    public long formula(@UdfParameter int v1, @UdfParameter int v2) {
        return (v1 * v2);
    }

    @Udf(description = "A special variant of the formula, handling double parameters.")
    public long formula(@UdfParameter double v1, @UdfParameter double v2) {
        return ((int) (Math.ceil(v1) * Math.ceil(v2)));
    }

}
```

## Set up a Java project

To implement a user-defined function, start by creating a Java project
with a dependency on ksqlDB's UDF library. This library contains the
annotations you use to signal that the classes you're implementing are
UDFs specifically. You can manage your Java project with any build
tool, but this guide demonstrates how it works with Gradle. If you
like, you can use the [Maven
archetype](https://github.com/confluentinc/ksql/tree/master/ksqldb-udf-quickstart)
instead. What matters is that you can put an uberjar in ksqlDB's
extension directory.

In a fresh directory, create the following `build.gradle` file to set
up the Java project:

```
buildscript {
    repositories {
        jcenter()
    }
}

plugins {
    id "java"
    id "com.github.johnrengelman.shadow" version "6.0.0"
}

sourceCompatibility = "1.8"
targetCompatibility = "1.8"
version = "0.0.1"

repositories {
    mavenCentral()
    jcenter()

    maven {
        url "https://packages.confluent.io/maven"
    }
}

dependencies {
    implementation "io.confluent.ksql:ksqldb-udf:{{ site.cprelease }}"
    implementation "org.apache.kafka:kafka_2.13:2.5.0"
    implementation "org.apache.kafka:connect-api:2.5.0"
}

apply plugin: "com.github.johnrengelman.shadow"
apply plugin: "java"

compileJava {
    options.compilerArgs << "-parameters"
}

shadowJar {
    archiveBaseName = "example-udfs"
    archiveClassifier = ""
    destinationDirectory = file("extensions")
}
```

Notice that:

- Dependencies are also declared on `kafka` and `connect-api`. You
  need both of these dependencies to make your UDFs capable of being
  externally configured or able to handle structs. This guide does
  both of those things.

- The Java compiler is invoked with the `-parameters` flag. This
  enables the JVM to retain method parameter names at runtime, which
  ksqlDB can use as auto-generated documentation in `DESCRIBE FUNCTION`
  statements.

!!! important
    Parameter names must be supplied using one of two approaches. You
    can either compile your jar with the `-parameters` flag to infer them
    from the Java code, or you can explicitly name them with the
    `@UdfParameter` annotation, as seen below.

## Implement the classes

There are three kinds of UDFs which manipulate rows in different ways:
scalar functions, tabular functions, and aggregation functions. Each
is demonstrated below with simple examples using a variety of
features. You can learn about more sophisticated usage in the
[concepts section](../concepts/functions.md).

Start by creating a directory for the class files:

```
mkdir -p src/main/java/com/example
```

### Scalar functions

A scalar function consumes one row as input and produces one row as
output. Use this when you simply want to transform a value.

Create a file at `src/main/java/com/example/FormulaUdf.java` and
populate it with the following code. This UDF takes two parameters and
executes a simple formula.

```java
package com.example;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

import org.apache.kafka.common.Configurable;

import java.util.Map;

@UdfDescription(name = "formula",
                author = "example user",
                version = "1.0.2",
                description = "A custom formula for important business logic.")
public class FormulaUdf implements Configurable {

    private int baseValue;

    @Override
    public void configure(final Map<String, ?> map) {
        String s = (String) map.get("ksql.functions.formula.base.value");
        baseValue = Integer.parseInt(s);
    }

    @Udf(description = "The standard version of the formula with integer parameters.")
    public long formula(@UdfParameter int v1, @UdfParameter int v2) {
        return (v1 * v2) + baseValue;
    }

    @Udf(description = "A special variant of the formula, handling double parameters.")
    public long formula(@UdfParameter double v1, @UdfParameter double v2) {
        return ((int) (Math.ceil(v1) * Math.ceil(v2))) + baseValue;
    }

}
```

Some important points to notice:


- The `@UdfDescription` annotation marks the class as a scalar
  UDF. The `name` parameter gives the function a name so you can refer
  to it in SQL. You can optionally add a version, which is a string of
  your choosing. You might find this useful if you evolve a function
  over time and want to know which particular version a server is
  currently using.

- The `@Udf` annotation marks a method as a body of code to invoke
  when the function is called. Because ksqlDB is strongly typed, you
  need to supply multiple signatures if you want your function to work
  with different column types. This UDF has two signatures: one that
  takes integer parameters and another that takes doubles.

- The `@UdfParameter` annotation marks each parameter of the
  method. This can be used for providing some amount of optional
  metadata about each parameter, but it's mainly useful so that ksqlDB
  can infer information at runtime.

- This UDF uses an external parameter,
  `ksql.functions.formula.base.value`. When a UDF implements the
  `Configurable` interface, it will be invoked once as the server
  starts up. `configure()` supplies a map of the parameters that
  ksqlDB server was started with. You will see how this value is
  populated later in the guide.

!!! warning
    External parameters aren't yet supported for tabular functions.

Either continue following this guide by implementing more functions or
skip ahead to [compiling the
classes](#add-the-uberjar-to-ksqldb-server) so you can use the
functions in ksqlDB.

### Tabular functions

A tabular function (UDTF for short) takes one row as input and
produces zero or more rows as output. This is sometimes called "flat
map" or "mapcat" in different programming languages. Use this when a
value represents many smaller values and needs to be "exploded" into
its individual parts to be useful.

Create a file at `src/main/java/com/example/IndexSequenceUdtf.java`
and populate it with the following code. This UDTF takes one parameter
as input, an array of any type, and returns a sequence of rows, where
each element is the element in the array concatenated with its index
position as a string.

```java
package com.example;

import io.confluent.ksql.function.udtf.Udtf;
import io.confluent.ksql.function.udtf.UdtfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

import java.util.ArrayList;
import java.util.List;

@UdtfDescription(name = "index_seq",
                 author = "example user",
                 version = "1.5.0",
                 description = "Disassembles a sequence and produces new elements concatenated with indices.")
public class IndexSequenceUdtf {

    private final String DELIMITER = "-";

    @Udtf(description = "Takes an array of any type and returns rows with each element paired to its index.")
    public <E> List<String> indexSequence(@UdfParameter List<E> x) {
        List<String> result = new ArrayList<>();

        for(int i = 0; i < x.size(); i++) { 
            result.add(x.get(i) + DELIMITER + i);
        }

        return result;
    }

}
```

Notice how:

- The UDTF returns a Java `List`. This is the collection type that
  ksqlDB expects all tabular functions to return.

- This UDTF uses Java generics, which enable operating over any
  [ksqlDB supported
  types](/reference/user-defined-functions/#data-type-mapping). Use this if you
  want to express logic that operates uniformly over many different
  column types. The generic parameter must be declared at the head of
  the method, since you can have multiple signatures, each with a
  different generic type parameter.

!!! info
    Java arrays and `List`s are not interchangeable in user-defined functions,
    which is especially important to remember when working with UDTFs. ksqlDB
    arrays correspond to the Java `List` type, not native Java arrays.

Either continue following this guide by implementing more functions or
skip ahead to [compiling the
classes](#add-the-uberjar-to-ksqldb-server) so you can use the
functions in ksqlDB.

### Aggregation functions

An aggregation function (UDAF for short) consumes one row at a time
and maintains a stateful representation of all historical data. Use
this when you want to compound data from multiple rows together.

Create a file at `src/main/java/com/example/RollingSumUdaf.java` and
populate it with the following code. This UDAF maintains a rolling sum
of the last `3` integers in a stream, discarding the oldest values as
new ones arrive.

```java
package com.example;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;

@UdafDescription(name = "rolling_sum",
                 author = "example user",
                 version = "2.0.0",
                 description = "Maintains a rolling sum of the last 3 integers of a stream.")
public class RollingSumUdaf {

    private RollingSumUdaf() {
    }

    @UdafFactory(description = "Sums the previous 3 integers of a stream, discarding the oldest elements as new ones arrive.")
    public static Udaf<Integer, List<Integer>, Integer> createUdaf() {
        return new RollingSumUdafImpl();
    }

    private static class RollingSumUdafImpl implements Udaf<Integer, List<Integer>, Integer> {

        private final int CAPACITY = 3;

        @Override
        public List<Integer> initialize() {
            return new LinkedList<Integer>();
        }

        @Override
        public List<Integer> aggregate(Integer newValue, List<Integer> aggregateValue) {
            aggregateValue.add(newValue);

            if (aggregateValue.size() > CAPACITY) {
                aggregateValue = aggregateValue.subList(1, CAPACITY + 1);
            }

            return aggregateValue;
        }

        @Override
        public Integer map(List<Integer> intermediate) {
            return intermediate.stream().reduce(0, Integer::sum);
        }

        @Override
        public List<Integer> merge(List<Integer> aggOne, List<Integer> aggTwo) {
            return aggTwo;
        }
    }
}
```

There are many things to observe in this class:

- Aggregation functions are designated by a static method with the `@UdafFactory` annotation, which differs from scalar and tabular functions. Because aggregations must implement multiple methods, this helps ksqlDB differentiate aggregations when multiple type signatures are used.

- The static factory method must either return `Udaf` or `TableUdaf`
  in package `io.confluent.ksql.function.udaf`.
    - Use the `Udaf` return value, as shown in this example, to
      aggregate streams into tables.

    - Use the `TableUdaf` return value, which derives from `Udaf`, to
      aggregate tables into other tables. Also, you must implement the `undo()` method.

- ksqlDB decouples the internal representation of an aggregate from
  its use in an operation. This is useful because aggregations can
  maintain complex state and expose it in a simpler way in a query. In
  this example, the internal representation is a `LinkedList`, as
  indicated by the `initialize()` method. But when ksqlDB interacts
  with the aggregation value, `map()` is called, which sums the values
  in the list. The `List` is needed to keep a running history of
  values, but the summed value is needed for the query itself.

- UDAFs must be parameterized with three generic types. In this
  example, they are `<Integer, List<Integer>, Integer>`. The first
  parameter represents the type of the column(s) to aggregate over. The
  second column represents the internal representation of the
  aggregation, which is established in `initialize()`. The third
  parameter represents the type that the query interacts with, which
  is converted by `map()`.
  - The input type can be a tuple of supported column types. Currently, 
    `Pair`, `Triple`, `Quadruple`, and `Quintuple` are supported. Tuple 
    types cannot be nested.
  - One input column inside a tuple can be variadic by wrapping it with 
    `VariadicArgs`. Only one variadic argument is supported, but it may 
    occur anywhere in the function signature. This means that if the 
    `UdafFactory` is variadic, none of the function's column arguments 
    may be variadic. A variadic column argument may have `Object` as its 
    type parameter to accept any number of columns of any type, though a
    variadic `Object` factory argument is not supported. A variadic 
    column argument outside a tuple is not supported. For example, the 
    input type would be `Pair<Double, VariadicArgs<Double>>` for a function 
    that accepts at least one double column.

- All types, including inputs, intermediate representations, and final
  representations, must be [types that ksqlDB
  supports](/reference/user-defined-functions/#data-type-mapping).

- The `merge` method controls how two [session
  windows](../../concepts/time-and-windows-in-ksqldb-queries/#session-window)
  fuse together when one extends and overlaps another. In this
  example, the content of the "later" aggregate is simply taken since
  it by definition contains values from a later window of time. If
  you're using session windows, consider what good merge semantics are
  for your aggregation.

#### Dynamic UDAFs

If a UDAF's aggregate or return types vary based on the input type, you can either write a separate
function annotated with @UdafFactory per type or override the following three methods
`initializeTypeArguments(List<SqlArgument> argTypeList)`, `getAggregateSqlType()`, and
`getReturnSqlType()`.  To see a concrete example in the ksqlDB codebase, check out the 
implementation of [`latest_by_offset`](https://github.com/confluentinc/ksql/blob/master/ksqldb-engine/src/main/java/io/confluent/ksql/function/udaf/offset/LatestByOffset.java) 
or [`collect_list`](https://github.com/confluentinc/ksql/blob/master/ksqldb-engine/src/main/java/io/confluent/ksql/function/udaf/array/CollectListUdaf.java).

## Add the uberjar to ksqlDB server

In order for ksqlDB to be able to load your UDFs, they need to be
compiled from classes into an uberjar. Run the following command to
build an uberjar:

```bash
gradle shadowJar
```

You should now have a directory, `extensions`, with a file named
`example-udfs-0.0.1.jar` in it.

In order to use the uberjar, you need to make it available to ksqlDB
server. Create the following `docker-compose.yml` file:

```yaml
---
version: '2'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:{{ site.cprelease }}
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  broker:
    image: confluentinc/cp-enterprise-kafka:{{ site.cprelease }}
    hostname: broker
    container_name: broker
    depends_on:
      - zookeeper
    ports:
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1

  schema-registry:
    image: confluentinc/cp-schema-registry:{{ site.cprelease }}
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      - zookeeper
      - broker
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL: 'zookeeper:2181'

  ksqldb-server:
    image: confluentinc/ksqldb-server:{{ site.ksqldbversion }}
    hostname: ksqldb-server
    container_name: ksqldb-server
    depends_on:
      - broker
      - schema-registry
    ports:
      - "8088:8088"
    volumes:
      - "./extensions/:/opt/ksqldb-udfs"
    environment:
      KSQL_LISTENERS: "http://0.0.0.0:8088"
      KSQL_BOOTSTRAP_SERVERS: "broker:9092"
      KSQL_KSQL_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: "true"
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: "true"
      # Configuration for UDFs
      KSQL_KSQL_EXTENSION_DIR: "/opt/ksqldb-udfs"
      KSQL_KSQL_FUNCTIONS_FORMULA_BASE_VALUE: 5

  ksqldb-cli:
    image: confluentinc/ksqldb-cli:{{ site.ksqldbversion }}
    container_name: ksqldb-cli
    depends_on:
      - broker
      - ksqldb-server
    entrypoint: /bin/sh
    tty: true
```

Notice that:

- A volume is mounted from the local `extensions` directory
  (containing your uberjar) to the container `/opt/ksqldb-udfs`
  directory. The latter can be any directory that you like. This
  command effectively puts the uberjar on ksqlDB server's file system.

- The environment variable `KSQL_KSQL_EXTENSION_DIR` is configured to
  the same path that was set for the container in the volume
  mount. This is the path that ksqlDB will look for UDFs in.

- The environment variable `KSQL_KSQL_FUNCTIONS_FORMULA_BASE_VALUE` is
  set to `5`. Recall that in the UDF example, the function loads an
  external parameter named` ksql.functions.formula.base.value`. All
  `KSQL_` environment variables are converted automatically to server
  configuration properties, which is where UDF parameters are looked
  up.

!!! info
    Although this is a single node setup, remember that every node in
    your ksqlDB cluster needs to have event variable parameters
    configured since any node can handle any query at any time.

## Invoke the functions

Bring up your stack by running:

```bash
docker-compose up
```

And connect to ksqlDB's server by using its interactive CLI:

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

Verify that your functions have been loaded by running the following
ksqlDB command:

```sql
SHOW FUNCTIONS;
```

You should see a long list of built-in functions, including your own
`FORMULA`, `INDEX_SEQ`, and `ROLLING_SUM` (which are listed as
`SCALAR`, `TABLE`, and `AGGREGATE` respectively). If they aren't there,
check that your uberjar was correctly mounted into the container. Be
sure to check the log files of ksqlDB server, too, using `docker logs
-f ksqldb-server`. You should see log lines similar to:

```
[2020-06-24 23:38:10,942] INFO Adding UDAF name=rolling_sum from path=/opt/ksqldb-udfs/example-udfs-0.0.1.jar class=class my.example.RollingSumUdaf (io.confluent.ksql.function.UdafLoader:71)
```

!!! info
    UDFs are loaded only once as ksqlDB server starts up. ksqlDB does
    not support hot-reloading UDFs. If you want to change the code of
    a UDF, you must create a new uberjar, replace the one that is
    available to ksqlDB, and restart the server. Keep in mind that in
    a multi-node setup, different nodes may be running different
    versions of a UDF at the same time.

Before you run any queries, be sure to have ksqlDB start all queries
from the earliest point in each topic.

```sql
SET 'auto.offset.reset' = 'earliest';
```

### Invoke the scalar function

Inspect the `formula` function by running:

```sql
DESCRIBE FUNCTION formula;
```

Your output should resemble:

```
Name        : FORMULA
Author      : example user
Version     : 1.0.2
Overview    : A custom formula for important business logic.
Type        : SCALAR
Jar         : /opt/ksqldb-udfs/example-udfs-0.0.1.jar
Variations  : 

	Variation   : FORMULA(v1 DOUBLE, v2 DOUBLE)
	Returns     : BIGINT
	Description : A special variant of the formula, handling double parameters.

	Variation   : FORMULA(v1 INT, v2 INT)
	Returns     : BIGINT
	Description : The standard version of the formula with integer parameters.
```

Create a stream named `s1`:

```sql
CREATE STREAM s1 (
    a VARCHAR KEY,
    b INT,
    c INT
) WITH (
    kafka_topic = 's1',
    partitions = 1,
    value_format = 'avro'
);
```
The DESCRIBE FUNCTION statement shows all of the type signatures that
the UDF implements. For the `formula` function, there are two type
signatures.

Insert some rows into the stream:

```sql
INSERT INTO s1 (a, b, c) VALUES ('k1', 2, 3);
INSERT INTO s1 (a, b, c) VALUES ('k2', 4, 6);
INSERT INTO s1 (a, b, c) VALUES ('k3', 6, 9);
```

Execute a push query. The `formula` function multiplies two integers
and adds the value of the parameter
`ksql.functions.formula.base.value`, which is set to `5` in your
Docker Compose file:

```sql
SELECT a, formula(b, c) AS result FROM s1 EMIT CHANGES;
```

Your output should resemble:

```
+--------------------------------------------------------------+--------------------------------------------------------------+
|A                                                             |RESULT                                                        |
+--------------------------------------------------------------+--------------------------------------------------------------+
|k1                                                            |11                                                            |
|k2                                                            |29                                                            |
|k3                                                            |59                                                            |
```

Try the other variant of the `formula` function, which takes two
doubles. This implementation takes the ceiling of `a` and `b` before
multiplying. Notice how you can use constants instead of column names
as arguments to the function:

```sql
SELECT a, formula(CAST(b AS DOUBLE), 7.3) AS result FROM s1 EMIT CHANGES;
```

Your output should resemble:

```
+--------------------------------------------------------------+--------------------------------------------------------------+
|A                                                             |RESULT                                                        |
+--------------------------------------------------------------+--------------------------------------------------------------+
|k1                                                            |21                                                            |
|k2                                                            |37                                                            |
|k3                                                            |53                                                            |

```

### Invoke the tabular function

Inspect the `index_seq` function by running:

```sql
DESCRIBE FUNCTION index_seq;
```

Your output should resemble:

```
Name        : INDEX_SEQ
Author      : example user
Version     : 1.5.0
Overview    : Disassembles a sequence and produces new elements concatenated with indices.
Type        : TABLE
Jar         : /opt/ksqldb-udfs/example-udfs-0.0.1.jar
Variations  : 

	Variation   : INDEX_SEQ(x ARRAY<E>)
	Returns     : VARCHAR
	Description : Disassembles a sequence and produces new elements concatenated with indices.
```

The DESCRIBE FUNCTION statement shows that `index_seq`  is a generic
function with the type parameter `E`, which means that this UDTF can
take a parameter that is an array of any type.

Create a stream named `s2`:

```sql
CREATE STREAM s2 (
    a VARCHAR KEY,
    b ARRAY<VARCHAR>
) WITH (
    kafka_topic = 's2',
    partitions = 1,
    value_format = 'avro'
);
```

Insert some rows into the stream:

```sql
INSERT INTO s2 (a, b) VALUES ('k1', ARRAY['a', 'b', 'c']);
INSERT INTO s2 (a, b) VALUES ('k2', ARRAY['d', 'e']);
INSERT INTO s2 (a, b) VALUES ('k3', ARRAY['f']);
```

Execute a push query. The `index_seq` function creates one row for
each element in an array, concatenated with the element's index
position.

```sql
SELECT a, index_seq(b) AS str FROM s2 EMIT CHANGES;
```

Your output should resemble:

```
+--------------------------------------------------------------+--------------------------------------------------------------+
|A                                                             |STR                                                           |
+--------------------------------------------------------------+--------------------------------------------------------------+
|k1                                                            |a-0                                                           |
|k1                                                            |b-1                                                           |
|k1                                                            |c-2                                                           |
|k2                                                            |d-0                                                           |
|k2                                                            |e-1                                                           |
|k3                                                            |f-0                                                           |
```

### Invoke the aggregation function

Inspect the `rolling_sum` function by running:

```sql
DESCRIBE FUNCTION rolling_sum;
```

Your output should resemble:

```
Name        : ROLLING_SUM
Author      : example user
Version     : 2.0.0
Overview    : Maintains a rolling sum of the last 3 integers of a stream.
Type        : AGGREGATE
Jar         : /opt/ksqldb-udfs/example-udfs-0.0.1.jar
Variations  : 

	Variation   : ROLLING_SUM(val INT)
	Returns     : INT
	Description : Sums the previous 3 integers of a stream, discarding the oldest elements as new ones arrive.
```

Create a stream named `s3`:

```sql
CREATE STREAM s3 (
    a VARCHAR KEY,
    b INT
) WITH (
    kafka_topic = 's3',
    partitions = 1,
    value_format = 'avro'
);
```

Insert some rows into the stream:

```sql
INSERT INTO s3 (a, b) VALUES ('k1', 3);
INSERT INTO s3 (a, b) VALUES ('k1', 5);
INSERT INTO s3 (a, b) VALUES ('k1', 7);
INSERT INTO s3 (a, b) VALUES ('k2', 6);
INSERT INTO s3 (a, b) VALUES ('k2', 2);
```

Execute a push query. The `rolling_sum` function aggregates the
previous three elements together, sums them, and emits their output.

```sql
SELECT a, rolling_sum(b) AS MOVING_SUM FROM s3 GROUP BY a EMIT CHANGES;
```

Your output should resemble:

```
+--------------------------------------------------------------+--------------------------------------------------------------+
|A                                                             |MOVING_SUM                                                    |
+--------------------------------------------------------------+--------------------------------------------------------------+
|k1                                                            |15                                                            |
|k2                                                            |8                                                             |
```

`k1` sums `3`, `5`, and `7` together to get a result of `15`. `k2`
sums `6` and `2` together to get a result of `8`.

Insert more rows, to shift older elements out of the aggregate:

```sql
INSERT INTO s3 (a, b) VALUES ('k1', 9);
INSERT INTO s3 (a, b) VALUES ('k2', 1);
INSERT INTO s3 (a, b) VALUES ('k2', 6);
```

Run the query again:

```sql
SELECT a, rolling_sum(b) AS MOVING_SUM FROM s3 GROUP BY a EMIT CHANGES;
```

Your output should resemble:

```
+--------------------------------------------------------------+--------------------------------------------------------------+
|A                                                             |MOVING_SUM                                                    |
+--------------------------------------------------------------+--------------------------------------------------------------+
|k1                                                            |21                                                            |
|k2                                                            |9                                                             |
```

 The output from the `rolling_sum` function has changed. In `k1`, the
 previous three values are now `5`, `7`, and `9`. In `k2`, the
 elements are now `2`, `1`, and `6`.
 
## Using structs and decimals

Working with structs and decimals in UDFs requires a more specific
type contract with ksqlDB. Both of these types are different from the
other Java types that ksqlDB interfaces with because their typing is
more dynamic. In the case of structs, fields can be added and removed,
and their types are inferred on the fly. In the case of decimals,
their precision and scale can change based on the inputs they are
computed against. Because of this dynamism, UDFs need to be more
explicit in their type contract with ksqlDB.

ksqlDB has two mechanisms handling these situations: explicitly
provided schemas and dynamic schemas. The former is generally used for
working with structs, and the latter is generally used for working
with decimals. This guide only uses explicitly provided schemas, but
you can read more about dynamic schema returns using the
`@UdfSchemaProvider` in the [concepts
section](/reference/user-defined-functions/#dynamic-types).

As an example of explicitly provided schemas, create a simple function
that maintains simple statistics. This example uses a UDAF, but the
concepts are applicable for UDFs and UDTFs. Although the example is a
bit contrived, it is useful because it demonstrates using a struct in
all possible positions.

### Implement the class

Create a file at `src/main/java/com/example/StatsUdaf.java` and
populate it with the following code. This UDAF maintains the minimum,
maximum, count, and difference between the min and max for a series of
numbers.

```java
package my.example;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Schema;

import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;

@UdafDescription(name = "stats",
                 author = "example user",
                 version = "1.3.5",
                 description = "Maintains statistical values.")
public class StatsUdaf {

    public static final Schema PARAM_SCHEMA = SchemaBuilder.struct().optional()
        .field("C", Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    public static final String PARAM_SCHEMA_DESCRIPTOR = "STRUCT<" +
        "C BIGINT" +
        ">";

    public static final Schema AGGREGATE_SCHEMA = SchemaBuilder.struct().optional()
        .field("MIN", Schema.OPTIONAL_INT64_SCHEMA)
        .field("MAX", Schema.OPTIONAL_INT64_SCHEMA)
        .field("COUNT", Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    public static final String AGGREGATE_SCHEMA_DESCRIPTOR = "STRUCT<" +
        "MIN BIGINT," +
        "MAX BIGINT," +
        "COUNT BIGINT" +
        ">";

    public static final Schema RETURN_SCHEMA = SchemaBuilder.struct().optional()
        .field("MIN", Schema.OPTIONAL_INT64_SCHEMA)
        .field("MAX", Schema.OPTIONAL_INT64_SCHEMA)
        .field("COUNT", Schema.OPTIONAL_INT64_SCHEMA)
        .field("DIFFERENTIAL", Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    public static final String RETURN_SCHEMA_DESCRIPTOR = "STRUCT<" +
        "MIN BIGINT," +
        "MAX BIGINT," +
        "COUNT BIGINT," +
        "DIFFERENTIAL BIGINT" +
        ">";

    private StatsUdaf() {
    }

    @UdafFactory(description = "Computes the min, max, count, and difference between min/max.",
                 paramSchema = PARAM_SCHEMA_DESCRIPTOR,
                 aggregateSchema = AGGREGATE_SCHEMA_DESCRIPTOR,
                 returnSchema = RETURN_SCHEMA_DESCRIPTOR)
    public static Udaf<Struct, Struct, Struct> createUdaf() {
        return new StatsUdafImpl();
    }

    private static class StatsUdafImpl implements Udaf<Struct, Struct, Struct> {

        @Override
        public Struct initialize() {
            return new Struct(AGGREGATE_SCHEMA);
        }

        @Override
        public Struct aggregate(Struct newValue, Struct aggregateValue) {
            long c = newValue.getInt64("C");

            long min = Math.min(c, getMin(aggregateValue));
            long max = Math.max(c, getMax(aggregateValue));
            long count = (getCount(aggregateValue) + 1);

            aggregateValue.put("MIN", min);
            aggregateValue.put("MAX", max);
            aggregateValue.put("COUNT", count);

            return aggregateValue;
        }

        @Override
        public Struct map(Struct intermediate) {
            Struct result = new Struct(RETURN_SCHEMA);

            long min = intermediate.getInt64("MIN");
            long max = intermediate.getInt64("MAX");

            result.put("MIN", min);
            result.put("MAX", max);
            result.put("COUNT", intermediate.getInt64("COUNT"));
            result.put("DIFFERENTIAL", max - min);

            return result;
        }

        @Override
        public Struct merge(Struct aggOne, Struct aggTwo) {
            return aggOne;
        }

        private Long getMin(Struct aggregateValue) {
            Long result = aggregateValue.getInt64("MIN");

            if (result != null) {
                return result;
            } else {
                return Long.MAX_VALUE;
            }
        }

        private Long getMax(Struct aggregateValue) {
            Long result = aggregateValue.getInt64("MAX");

            if (result != null) {
                return result;
            } else {
                return Long.MIN_VALUE;
            }
        }

        private Long getCount(Struct aggregateValue) {
            Long result = aggregateValue.getInt64("COUNT");

            if (result != null) {
                return result;
            } else {
                return 0L;
            }
        }
    }
}
```

There are a few important things to call out in this class:

- Schemas are declared for the input struct parameter, intermediate
  aggregation struct value, and output struct value. These structs
  hold different data, so they each need their own schema.

- Descriptor strings are created for each schema, too. This
  communicates the underlying types to ksqlDB is a way that its type
  system can understand.

- The schemas, and all of the contained fields, are declared as
  optional. ksqlDB doesn't have null constraints, meaning that any
  value can be null. To handle this, all schemas and field values must
  be marked as optional.

- The field names in the descriptors must match their declared field
  names in ksqlDB exactly, including their casing. By default, ksqlDB
  uppercases all field names, which is why the descriptors are also
  uppercased. If you use backticks for your field names to preserve
  casing, you must also use backticks in the descriptors, too.

- Explicitly declaring schemas is *only* needed when using structs and
  decimals. But because this example makes use of structs in all
  possible places (input, intermediate, and output values), schemas
  are declared for all of them.

!!! info
    If you're using a struct with a UDF or UDTF, you can set the
    schema using the `Udf`, `Udtf`, and `UdfParameter`
    annotations. Each provides the option to supply a schema for
    various positions.

Create an uberjar in the same manner. If you already have ksqlDB
running, be sure to restart it and remount the jar so that it picks up
the new code. When you restart the server, keep an eye on the log
files. If any of the type schemas are missing or incoherent, ksqlDB
will log an error, such as:

```
[2020-06-29 21:10:23,889] WARN Failed to create UDAF name=struct_example, method=createUdaf, class=class my.example.StructExample, path=/opt/ksqldb-udfs/example-udfs-0.0.1.jar (io.confluent.ksql.function.UdafLoader:87)
io.confluent.ksql.util.KsqlException: Must specify 'aggregateSchema' for STRUCT parameter in @UdafFactory.
```

### Invoke the function

Try using the UDAF. Create the stream `s4`:

```sql
CREATE STREAM s4 (
    a VARCHAR KEY,
    b STRUCT<c BIGINT>
) WITH (
    kafka_topic = 's4',
    partitions = 1,
    value_format = 'avro'
);
```

And insert some rows into it:

```sql
INSERT INTO s4 (
    a, b
) VALUES (
    'k1', STRUCT(c := 5)
);

INSERT INTO s4 (
    a, b
) VALUES (
    'k1', STRUCT(c := 3)
);

INSERT INTO s4 (
    a, b
) VALUES (
    'k1', STRUCT(c := 9)
);
```

Remember to have ksqlDB start all queries from the earliest point in
each topic:

```sql
SET 'auto.offset.reset' = 'earliest';
```

Execute the following push query:

```sql
SELECT a, stats(b) AS stats FROM s4 GROUP BY a EMIT CHANGES;
```

Your output should resemble:

```
+--------------------------------------------------------------+--------------------------------------------------------------+
|A                                                             |STATS                                                         |
+--------------------------------------------------------------+--------------------------------------------------------------+
|k1                                                            |{MIN=3, MAX=9, COUNT=3, DIFFERENTIAL=6}                       |
```

If you like, you can destructure the output into individual
columns. Try following the [query structured data
guide](query-structured-data.md).

## Tear down the stack

When you're done, tear down the stack by running:

```bash
docker-compose down
```
