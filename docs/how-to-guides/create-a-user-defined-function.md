# How to create a user-defined function

## Context

You have a piece of logic for transforming or aggregating events that ksqlDB can't currently express. You want to extend ksqlDB to apply that logic in your persistent queries. To do that, ksqlDB exposes hooks so that you can add new logic with Java programs. This functionality is broadly called *user-defined functions*, or UDFs for short.

## In action

```java
package my.example;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(name = "multiply", description = "multiplies 2 numbers")
public class Multiply {

    @Udf(description = "multiply two non-nullable integers")
    public long multiply(@UdfParameter(value = "v1") int v1, @UdfParameter(value = "v2") int v2) {
        return v1 * v2;
    }

}
```

## Set up a Java project

To implement a user-defined function, the first thing that you need to do is create a Java project with a dependency on ksqlDB's UDF library. This library contains the annotations that you'll use to signal that the classes you're implementing aren't just any old classes, they're UDFs. You can manage your Java project with any build tool, but this guide demonstrates how it works with Gradle. In the end, all that matters is that you're able to put an uberjar in ksqlDB's extension directory.

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
    compile "io.confluent.ksql:ksqldb-udf:5.5.0"
}

apply plugin: "com.github.johnrengelman.shadow"
apply plugin: "java"

shadowJar {
    archiveBaseName = "example-udfs"
    archiveClassifier = ""
}
```

## Implement the classes

There are three kinds of UDFs for manipulating rows in different ways: scalar functions, tabular functions, and aggregation functions. Each is demonstrated below with simple examples (you can learn about more sophisticated usage in the [concepts section](../concepts/functions.md)). Start by creating a directory to house the class files:

```
mkdir -p src/main/java/com/example
```

### Scalar functions

A scalar function (UDF for short) consumes one row as input and produces one row as output. Use this when you want to simply transform a value.

Create a file at `src/main/java/com/example/MultiplyUdf.java` and populate it with the following code. This UDF takes two parameters and returns the value of multiplying them together.

```java
package my.example;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(name = "multiply", description = "Multiplies 2 numbers.")
public class MultiplyUdf {

    @Udf(description = "Multiply two non-nullable integers.")
    public long multiply(@UdfParameter(value = "v1") int v1, @UdfParameter(value="v2") int v2) {
        return v1 * v2;
    }

}
```

Some important points to notice:

1. The `@UdfDescription` annotation marks the class as a scalar UDF. The `name` parameter gives the function a name so you can refer to it in SQL. The `description` parameter gives the function an explanation of what it does so that ksqlDB can render something useful when asked to `DESCRIBE` the function.
2. The `@Udf` annotation marks the method as a body of code to invoke when the function is called. A UDF can have multiple type signatures, so you can have as many methods as you want per class. The supplied description helps you distinguish what each method does.
3. The `@UdfParameter` annotation lets you give the function parameters names, which will be rendered in `DESCRIBE` statements too.

Either continue following this guide by implementing more functions, or skip ahead to [compiling the classes](#add-the-uberjar-to-the-classpath) so you can use the functions in ksqlDB.

### Tabular functions

A tabular function (UDTF for short) takes one row as input and produces zero or more rows as output. This is sometimes called "flat map" or "mapcat" in different programming languages. Use this when a value represents many other values and needs to be "exploded" into its individual values to be useful.

Create a file at `src/main/java/com/example/IndexCharactersUdtf.java` and populate it with the following code. This UDTF takes one parameter as input, a string, and returns a sequence of rows, where each element is the character in the string concatenated with its index position.

```java
package my.example;

import java.util.ArrayList;
import java.util.List;

import io.confluent.ksql.function.udtf.Udtf;
import io.confluent.ksql.function.udtf.UdtfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdtfDescription(name = "index_characters", description = "Returns a sequence of rows, where each element is the character in the string concatenated with its index position.")
public class IndexCharactersUdtf {

    @Udtf(description = "Concat each character with its index.")
    public List<String> indexCharacters(@UdfParameter(value = "s") String s) {
        List<String> result = new ArrayList<>();

        for(int i = 0; i < s.length(); i++) { 
            result.add(s.charAt(i) + "-" + i);
        }

        return result;
    }

}
```

Notice how:

- The UDTF returns a Java `List`. This is the collection type that ksqlDB expects all tabular functions to return.
- The annotations work similiarly to the [scalar function](#scalar-functions) example.

Either continue following this guide by implementing more functions, or skip ahead to [compiling the classes](#add-the-uberjar-to-the-classpath) so you can use the functions in ksqlDB.

### Aggregation functions

## Add the uberjar to ksqlDB server

In order for ksqlDB to be able to load your UDFs, they need to be compiled from classes into an uberjar. Run the following command to build an uberjar:

```
gradle build
```

You should now have a directory, `extensions`, with a file named `how-to-guides-0.0.1.jar` in it.

In order to use the uberjar, you need to make it available to ksqlDB server. Create the following `docker-compose.yml` file:

```yaml
---
version: '2'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:5.4.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  broker:
    image: confluentinc/cp-enterprise-kafka:5.4.0
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
    image: confluentinc/cp-schema-registry:5.4.1
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
    image: confluentinc/ksqldb-server:0.9.0
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

  ksqldb-cli:
    image: confluentinc/ksqldb-cli:0.9.0
    container_name: ksqldb-cli
    depends_on:
      - broker
      - ksqldb-server
    entrypoint: /bin/sh
    tty: true
```

There are a few important things to notice:

- A volume is mounted from the local `extensions` directory (containing your uberjar) to the container `/opt/ksqldb-udfs` directory. The latter can be any directory that you like. This command effectively puts the uberjar on ksqlDB server's file system.
- The environment variable `KSQL_KSQL_EXTENSION_DIR` is configured to the same path that was set for the container in the volume mount. This is the path that ksqlDB will look for UDFs in.
- Although this is a single node setup, remember that every node in your ksqlDB cluster needs to have this configured since any node can handle any query at any time.

## Invoke the functions

Bring up your local setup by running:

```
docker-compose up
```

And connect to ksqlDB's server by using its interactive CLI. Run the following command:

```
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

Verify that your functions have been loaded by running the following ksqlDB command:

```sql
SHOW FUNCTIONS;
```

You should see a long list of built-in functions, including your own `MULTIPLY` and `INDEX_CHARACTERS` (which are listed as `SCALAR` and `TABLE` respectivly).

- What to do if they didn't load.


- udf demo: using columns or constants as parameters

- UDFs only load once!

Introspect the `MULTIPLY` function by running:

```sql
DESCRIBE FUNCTION multiply;
```

Which should output:

```
Name        : MULTIPLY
Overview    : multiplies 2 numbers
Type        : SCALAR
Jar         : /opt/ksqldb-udfs/how-to-guides-0.0.1.jar
Variations  : 

	Variation   : MULTIPLY(v1 INT, v2 INT)
	Returns     : BIGINT
	Description : multiply two non-nullable INTs.
```

Do the same for `index_characters`:

```sql
DESCRIBE FUNCTION index_characters;
```

Which should output:

```
Name        : INDEX_CHARACTERS
Overview    : ...
Type        : TABLE
Jar         : /opt/ksqldb-udfs/how-to-guides-0.0.1.jar
Variations  : 

	Variation   : INDEX_CHARACTERS(s VARCHAR)
	Returns     : VARCHAR
	Description : ...
```

```sql
SET 'auto.offset.reset' = 'earliest';
```

```sql
CREATE STREAM s1 (
    a VARCHAR KEY,
    b INT
) WITH (
    kafka_topic = 's1',
    partitions = 1,
    value_format = 'avro'
);
```

```sql
CREATE STREAM s1 (
    a VARCHAR,
    b INT,
    c VARCHAR
) WITH (
    kafka_topic = 's1',
    partitions = 1,
    value_format = 'avro',
    key = 'a'
);
```

```sql
INSERT INTO s1 (a, b, c) VALUES ('k1', 2, 'abc');
INSERT INTO s1 (a, b, c) VALUES ('k2', 4, 'de');
INSERT INTO s1 (a, b, c) VALUES ('k3', 6, 'f');
```

```sql
SELECT a, multiply(b, 3) AS product FROM s1 EMIT CHANGES;
```

```
+------------------------------------------------------------+------------------------------------------------------------+
|A                                                           |PRODUCT                                                     |
+------------------------------------------------------------+------------------------------------------------------------+
|k1                                                          |6                                                           |
|k2                                                          |12                                                          |
|k3                                                          |18                                                          |
```

```sql
SELECT a, index_characters(c) AS indexed FROM s1 EMIT CHANGES;
```

```
+------------------------------------------------------------+------------------------------------------------------------+
|A                                                           |INDEXED                                                     |
+------------------------------------------------------------+------------------------------------------------------------+
|k1                                                          |a-0                                                         |
|k1                                                          |b-1                                                         |
|k1                                                          |c-2                                                         |
|k2                                                          |d-0                                                         |
|k2                                                          |e-1                                                         |
|k3                                                          |f-0                                                         |
```

```sql
CREATE STREAM s1 (
    a VARCHAR,
    b INT,
    c ARRAY<VARCHAR>
) WITH (
    kafka_topic = 's1',
    partitions = 1,
    value_format = 'avro',
    key = 'a'
);

INSERT INTO s1 (a, b, c) VALUES ('k1', 2, ARRAY['a', 'b', 'c']);
INSERT INTO s1 (a, b, c) VALUES ('k2', 4, ARRAY['d', 'e']);
INSERT INTO s1 (a, b, c) VALUES ('k3', 6, ARRAY['f']);


CREATE STREAM s1 (
    a VARCHAR,
    b INT,
    c ARRAY<VARCHAR>
) WITH (
    kafka_topic = 's1',
    partitions = 1,
    value_format = 'avro',
    key = 'a'
);

INSERT INTO s1 (a, b, c) VALUES ('k1', 2, ARRAY['a', 'b', 'c']);
INSERT INTO s1 (a, b, c) VALUES ('k2', 4, ARRAY['d', 'e']);
INSERT INTO s1 (a, b, c) VALUES ('k3', 6, ARRAY['f']);

CREATE STREAM s2 (
    a VARCHAR,
    b INT,
    c ARRAY<INT>
) WITH (
    kafka_topic = 's2',
    partitions = 1,
    value_format = 'avro',
    key = 'a'
);

INSERT INTO s2 (a, b, c) VALUES ('k1', 2, ARRAY[0, 1, 2]);
INSERT INTO s2 (a, b, c) VALUES ('k2', 4, ARRAY[3, 4]);
INSERT INTO s2 (a, b, c) VALUES ('k3', 6, ARRAY[5]);
```

```sql
select a, rolling_sum(b) from s1 group by a emit changes;
```

## Tear down the stack

When you're done, tear down the stack by running:

```
docker-compose down
```

- << TODO: UDFs, multiple signatures >>
- << TODO: versions of UDFs, authors >>
- << TODO: Table UDAF >>
- << TODO: why UDAFs need a factory >>
- << TODO: Show how to parameterize a UDF >>
- << TODO: why it needs to be List, not array types >>
- << TODO: Intermediate type restrictions >>
- << TODO: Call out load in log file like `[2020-06-24 23:38:10,942] INFO Adding UDAF name=rolling_sum from path=/opt/ksqldb-udfs/example-udfs-0.0.1.jar class=class my.example.RollingSumUdaf (io.confluent.ksql.function.UdafLoader:71)` >>