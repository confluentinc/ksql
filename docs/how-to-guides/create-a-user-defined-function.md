# How to create a user-defined function

## Context

You have a piece of logic for transforming or aggregating events that ksqlDB can't currently express. You want to extend ksqlDB to apply that logic in your queries. To do that, ksqlDB exposes hooks through Java programs. This functionality is broadly called *user-defined functions*, or UDFs for short.

## In action

```java
package my.example;

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
    public long formula(@UdfParameter(value = "a") int v1, @UdfParameter(value = "b") int v2) {
        return (v1 * v2) + baseValue;
    }

    @Udf(description = "A special variant of the formula, handling double parameters.")
    public long formula(@UdfParameter(value = "a") double v1, @UdfParameter(value = "b") double v2) {
        return ((int) (Math.ceil(v1) * Math.ceil(v2))) + baseValue;
    }

}
```

## Set up a Java project

To implement a user-defined function, the first thing that you need to do is create a Java project with a dependency on ksqlDB's UDF library. This library contains the annotations you'll use to signal that the classes you're implementing aren't just any old classes, they're UDFs. You can manage your Java project with any build tool, but this guide demonstrates how it works with Gradle. In the end, all that matters is that you're able to put an uberjar in ksqlDB's extension directory.

In a fresh directory, create the following `build.gradle` file to set up the Java project:

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
        url "http://packages.confluent.io/maven"
    }
}

dependencies {
    compile "io.confluent.ksql:ksqldb-udf:{{ site.cprelease }}"
    compile "org.apache.kafka:kafka_2.13:2.5.0"
}

apply plugin: "com.github.johnrengelman.shadow"
apply plugin: "java"

shadowJar {
    archiveBaseName = "example-udfs"
    archiveClassifier = ""
}
```

## Implement the classes

There are three kinds of UDFs which manipulate rows in different ways: scalar functions, tabular functions, and aggregation functions. Each is demonstrated below with simple examples using a variety of features (you can learn about more sophisticated usage in the [concepts section](../concepts/functions.md)). Start by creating a directory to house the class files:

```
mkdir -p src/main/java/com/example
```

### Scalar functions

A scalar function (UDF for short) consumes one row as input and produces one row as output. Use this when you simply want to transform a value.

Create a file at `src/main/java/com/example/FormulaUdf.java` and populate it with the following code. This UDF takes two parameters and executes a simple formula.

```java
package my.example;

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
    public long formula(@UdfParameter(value = "a") int v1, @UdfParameter(value = "b") int v2) {
        return (v1 * v2) + baseValue;
    }

    @Udf(description = "A special variant of the formula, handling double parameters.")
    public long formula(@UdfParameter(value = "a") double v1, @UdfParameter(value = "b") double v2) {
        return ((int) (Math.ceil(v1) * Math.ceil(v2))) + baseValue;
    }

}
```

Some important points to notice:


- The `@UdfDescription` annotation marks the class as a scalar UDF. The `name` parameter gives the function a name so you can refer to it in SQL.

- The `@Udf` annotation marks a method as a body of code to invoke when the function is called. Because ksqlDB is strongly typed, you need to supply multiple signatures if you want your function to work with different column types. This UDF has two signatures: one that takes integer parameters and another that takes doubles.

- The `@UdfParameter` annotation lets you give the function parameters names. These are rendered when using the `DESCRIBE` statement on a function.

- This UDF uses an external parameter, `ksql.functions.formula.base.value`. When a UDF implements the `Configurable` interface, it will be invoked once as the server starts up. `configure()` supplies a map of the parameters that ksqlDB server was started with. You will see how this value is populated later in the guide.

!!! warning
    External parameters do not yet work for tabular or aggregation functions.
    Support for these function types will be added soon.

Either continue following this guide by implementing more functions or skip ahead to [compiling the classes](#add-the-uberjar-to-the-classpath) so you can use the functions in ksqlDB.

### Tabular functions

A tabular function (UDTF for short) takes one row as input and produces zero or more rows as output. This is sometimes called "flat map" or "mapcat" in different programming languages. Use this when a value represents many smaller values and needs to be "exploded" into its individual parts to be useful.

Create a file at `src/main/java/com/example/IndexSeqUdtf.java` and populate it with the following code. This UDTF takes one parameter as input, an array of any type, and returns a sequence of rows, where each element is the element in the array concatenated with its index position as a string.

```java
package my.example;

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
    public <E> List<String> indexSequence(@UdfParameter(value = "s") List<E> x) {
        List<String> result = new ArrayList<>();

        for(int i = 0; i < x.size(); i++) { 
            result.add(x.get(i) + DELIMITER + i);
        }

        return result;
    }

}
```

Notice how:

- The UDTF returns a Java `List`. This is the collection type that ksqlDB expects all tabular functions to return.

- This UDTF uses Java generics which allow it operate over any ksqlDB supported types. Use this if you want to express logic that operates uniformly over many different column types. The generic parameter must be declared at the head of the method since you can have multiple signatures, each with a different generic type parameter.


!!! info
    Java arrays and `List`s are not interchangable in user-defined functions,
    which is especially important to remember when working with UDTFs. ksqlDB
    arrays correspond to the Java `List` type, not native Java arrays.

Either continue following this guide by implementing more functions or skip ahead to [compiling the classes](#add-the-uberjar-to-the-classpath) so you can use the functions in ksqlDB.

### Aggregation functions

An aggregation function (UDAF for short) consumes one row at a time and maintains a stateful representation of all historical data. Use this when you want to compound data from multiple rows together.

Create a file at `src/main/java/com/example/RollingsumUdaf.java` and populate it with the following code. This UDAF maintains a rolling sum of the last `3` integers in a stream, discarding the oldest values as new ones arrive.

```java
package my.example;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.function.udf.UdfParameter;

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
            Iterator<Integer> it = intermediate.iterator();
            int k = 0;

            while(it.hasNext()) {
                k += it.next();
            }

            return k;
        }

        @Override
        public List<Integer> merge(List<Integer> aggOne, List<Integer> aggTwo) {
            return aggOne;
        }
    }
}
```

There are a few things to observe in this class:

- By contrast to scalar and tabular functions, aggregation functions are designated by a static method with the `@UdafFactory` annotation. Because aggregations must implement multiple methods, this helps ksqlDB differentiate aggregations when multiple type signatures are used.

- The static factory method needs to either return `Udaf` or `TableUdaf` (in package `io.confluent.ksql.function.udaf`). The former, which is used in this example, can only be used to aggregate streams into tables, and cannot aggregate tables into other tables. To achieve the latter, use the `TableUdaf`, which derives from `Udaf`, and implement the `undo()` method, too.

- ksqlDB decouples the internal representation of an aggregate from how it is used in an operation. This is very useful because aggregations can maintain complex state, but expose it in a simpler way in a query. In this example, the internal representation is a `LinkedList`, as indicated by the `initialize()` method. But when ksqlDB interacts with the aggregation value, `map()` is called, which sums the values in the list. The `List` is needed to keep a running history of values, but the summed value is needed for the query itself.

- UDAFs must be parameterized with three generic types. In this example, they are `<Integer, List<Integer>, Integer>`. The first parameter represents the type of the column to aggregate over. The second column represents the internal representation of the aggregation, which is established in `initialize()`. The third parameter represents the type that the query interacts with, which is converted by `map()`.

- All types, including inputs, intermediate representations, and final representations, must be [types that ksqlDB supports](../../developer-guide/syntax-reference/#ksqldb-data-types).

## Add the uberjar to ksqlDB server

In order for ksqlDB to be able to load your UDFs, they need to be compiled from classes into an uberjar. Run the following command to build an uberjar:

```
gradle build
```

You should now have a directory, `extensions` with a file named `how-to-guides-0.0.1.jar` in it.

In order to use the uberjar, you need to make it available to ksqlDB server. Create the following `docker-compose.yml` file:

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
    image: confluentinc/ksqldb-server:{{ site.release }}
    hostname: ksqldb-server
    container_name: ksqldb-server
    depends_on:
      - broker
      - schema-registry
    ports:
      - "8088:8088"
    volumes:
      - "./build/libs/:/opt/ksqldb-udfs"
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
    image: confluentinc/ksqldb-cli:{{ site.release }}
    container_name: ksqldb-cli
    depends_on:
      - broker
      - ksqldb-server
    entrypoint: /bin/sh
    tty: true
```

Notice that:

- A volume is mounted from the local `build/libs` directory (containing your uberjar) to the container `/opt/ksqldb-udfs` directory. The latter can be any directory that you like. This command effectively puts the uberjar on ksqlDB server's file system.

- The environment variable `KSQL_KSQL_EXTENSION_DIR` is configured to the same path that was set for the container in the volume mount. This is the path that ksqlDB will look for UDFs in.

- The environment variable `KSQL_KSQL_FUNCTIONS_FORMULA_BASE_VALUE` is set to `5`. Recall that in the UDF example, the function loads an external parameter named` ksql.functions.formula.base.value`. All `KSQL_` environment variables are converted automatically to server configuration properties, which is where UDF parameters are looked up.

!!! info
    Although this is a single node setup, remember that every node in your ksqlDB cluster needs to have event variable parameters configured since any node can handle any query at any time.

## Invoke the functions

Bring up your stack by running:

```
docker-compose up
```

And connect to ksqlDB's server by using its interactive CLI:

```
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

Verify that your functions have been loaded by running the following ksqlDB command:

```sql
SHOW FUNCTIONS;
```

You should see a long list of built-in functions, including your own `FORMULA`, `INDEX_SEQ`, and `ROLLING_SUM` (which are listed as `SCALAR`, `TABLE`, and `AGGREGATE` respectivly). If they aren't there, check that your uberjar was correctly mounted into the container. Be sure to check the log files of ksqlDB server, too, using `docker logs -f ksqldb-server`. You should see log lines similar to:

```
[2020-06-24 23:38:10,942] INFO Adding UDAF name=rolling_sum from path=/opt/ksqldb-udfs/example-udfs-0.0.1.jar class=class my.example.RollingSumUdaf (io.confluent.ksql.function.UdafLoader:71)
```

!!! info
    UDFs are only loaded once as ksqlDB server starts up. ksqlDB does not support hot-reloading UDFs. If you want to change the code of a UDF, you need to create a new uberjar, replace the one that is available to ksqlDB, and restart the server. Keep in mind that in a multi-node setup, different nodes may be running different versions of a UDF at the same time.

Before you run any queries, be sure to have ksqlDB start all queries from the earliest point in each topic.

```sql
SET 'auto.offset.reset' = 'earliest';
```

### Invoke the scalar function

Inspect the `formula` function by running:

```sql
DESCRIBE FUNCTION formula;
```

It should output the following. ksqlDB shows all the type signatures that the UDF implements, which in two in this case.

```
Name        : FORMULA
Author      : example user
Version     : 1.0.2
Overview    : A custom formula for important business logic.
Type        : SCALAR
Jar         : /opt/ksqldb-udfs/example-udfs-0.0.1.jar
Variations  : 

	Variation   : FORMULA(a DOUBLE, b DOUBLE)
	Returns     : BIGINT
	Description : A special variant of the formula, handling double parameters.

	Variation   : FORMULA(a INT, b INT)
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

Insert some rows into the stream:

```sql
INSERT INTO s1 (a, b, c) VALUES ('k1', 2, 3);
INSERT INTO s1 (a, b, c) VALUES ('k2', 4, 6);
INSERT INTO s1 (a, b, c) VALUES ('k3', 6, 9);
```

Execute a push query. Recall what `formula` does. When given two integers, it multiples the together, then adds the value of the parameter `ksql.functions.formula.base.value`, which is set to `5` in your Docker Compose file:

```sql
SELECT a, formula(b, c) AS result FROM s1 EMIT CHANGES;
```

You should see:

```
+--------------------------------------------------------------+--------------------------------------------------------------+
|A                                                             |RESULT                                                        |
+--------------------------------------------------------------+--------------------------------------------------------------+
|k1                                                            |11                                                            |
|k2                                                            |29                                                            |
|k3                                                            |59                                                            |
```

Try the other variant which takes two doubles. This implementation takes the ceiling of `a` and `b` before multiplying. Notice how you can use constants instead of column names as arguments to the function:

```sql
SELECT a, formula(CAST(b AS DOUBLE), 7.3) AS result FROM s1 EMIT CHANGES;
```

You should see:

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

It should output the following. Notice how ksqlDB shows that this is a generic function with the type parameter `E`. This means that this UDTF can take a parameter that is an array of any type.

```
Name        : INDEX_SEQ
Author      : example user
Version     : 1.5.0
Overview    : Disassembles a sequence and produces new elements concatenated with indices.
Type        : TABLE
Jar         : /opt/ksqldb-udfs/example-udfs-0.0.1.jar
Variations  : 

	Variation   : INDEX_SEQ(s ARRAY<E>)
	Returns     : VARCHAR
	Description : Disassembles a sequence and produces new elements concatenated with indices.
```

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

Execute a push query. Recall what `index_seq` does. It creates a row per element in an array concatenated with its index position.

```sql
SELECT a, index_seq(b) AS str FROM s2 EMIT CHANGES;
```

You should see the following:

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

It should output the following:

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

Execute a push query. Recall what `rolling_sum` does. It aggregates the previous three elements together, sums them up, and emits their output.

```sql
SELECT a, rolling_sum(b) AS MOVING_SUM FROM s3 GROUP BY a EMIT CHANGES;
```

Your output should look like the following. `k1` sums `3`, `5`, and `7` together to get a result of `15`. `k2` sums `6` and `2` together to get a result of `8`.

```
+--------------------------------------------------------------+--------------------------------------------------------------+
|A                                                             |MOVING_SUM                                                    |
+--------------------------------------------------------------+--------------------------------------------------------------+
|k1                                                            |15                                                            |
|k2                                                            |8                                                             |
```

Insert some more rows and shift older elements out of the aggregate:

```sql
INSERT INTO s3 (a, b) VALUES ('k1', 9);
INSERT INTO s3 (a, b) VALUES ('k2', 1);
INSERT INTO s3 (a, b) VALUES ('k2', 6);
```

Run the query again:

```sql
SELECT a, rolling_sum(b) AS MOVING_SUM FROM s3 GROUP BY a EMIT CHANGES;
```

And you should now see these results. In `k1`, the previous three values are now `5`, `7`, and `9`. In `k2`, the elements are `2`, `1`, and `6`.

```
+--------------------------------------------------------------+--------------------------------------------------------------+
|A                                                             |MOVING_SUM                                                    |
+--------------------------------------------------------------+--------------------------------------------------------------+
|k1                                                            |21                                                            |
|k2                                                            |9                                                             |
```

## Tear down the stack

When you're done, tear down the stack by running:

```
docker-compose down
```

- << TODO: right Gradle UDF coordinates >>
- << TODO: Struct example? >>
- << TODO: fix project name >>