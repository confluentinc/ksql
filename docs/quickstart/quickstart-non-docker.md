# Non-Docker Setup for KSQL

| [Overview](/docs#ksql-documentation) |[Quick Start](/docs/quickstart#quick-start) | [Concepts](/docs/concepts.md#concepts) | [Syntax Reference](/docs/syntax-reference.md#syntax-reference) |[Demo](/ksql-clickstream-demo#clickstream-analysis) | [Examples](/docs/examples.md#examples) | [FAQ](/docs/faq.md#frequently-asked-questions)  |
|---|----|-----|----|----|----|----|

This part of the quick start will guide you through the steps to setup a Kafka cluster and start KSQL for non-Docker environments. After you complete these steps, you can return to the [main Quick Start](/docs/quickstart#quick-start) and use KSQL to query the Kafka cluster.

**Table of Contents**

- [Start Kafka](#start-kafka)
- [Start KSQL](#start-ksql)
- [Produce topic data](#produce-topic-data)

**Prerequisites:**
- KSQL is in developer preview. Do not run KSQL against a production cluster.
- [Confluent Platform 4.0.0](http://docs.confluent.io/current/installation.html) is installed. This installation includes a Kafka broker, ZooKeeper, Schema Registry, REST Proxy, and Kafka Connect.
  - If you installed Confluent Platform via tar or zip, navigate into the installation directory. The paths and commands used throughout this quick start assume that your are in this installation directory.
- Java: Minimum version 1.8. Install Oracle Java JRE or JDK \>= 1.8 on your local machine

## Start Kafka

Navigate to the `confluent-4.0.0` directory and start the Confluent Platform using the new Confluent CLI (part of the free Confluent Open Source distribution). ZooKeeper is listening on `localhost:2181`, Kafka broker is listening on `localhost:9092`, and Confluent Schema Registry is listening on `localhost:8081`.

```bash
$ ./bin/confluent start
```

Your output should resemble this.

```bash
Starting zookeeper
zookeeper is [UP]
Starting kafka
kafka is [UP]
Starting schema-registry
schema-registry is [UP]
Starting kafka-rest
kafka-rest is [UP]
Starting connect
connect is [UP]
```

## Start KSQL

1. Download the KSQL release tarball from the [KSQL releases page](https://github.com/confluentinc/ksql/releases/latest)

1. Unpack the release tarball.

    ```bash
    $ tar -xzf ksql.tgz
    ```

1.  Change directory to the `ksql` directory.

    ```bash
    $ cd ksql
    ```

1.  Start KSQL. The `local` argument starts KSQL in [standalone mode](/docs/concepts.md#modes-of-operation).

    ```bash
    $ ./bin/ksql-cli local
    ```

    After KSQL is started, your terminal should resemble this.

    ```bash
    ksql>
    ```

See the steps below to generate data to the Kafka cluster.

## Produce topic data
Minimally, to use the [quick start exercises](/docs/quickstart#quick-start), you must run the following steps to produce data to the Kafka topics `pageviews` and `users`.

1.  Produce Kafka data to the `pageviews` topic using the data generator. The following example continuously generates data with a value in DELIMITED format.

    ```bash
    $ ./bin/ksql-datagen quickstart=pageviews format=delimited topic=pageviews maxInterval=10000
    ```

1.  Produce Kafka data to the `users` topic using the data generator. The following example continuously generates data with a value in JSON format.

    ```bash
    $ ./bin/ksql-datagen quickstart=users format=json topic=users maxInterval=10000
    ```

Optionally, you can return to the [main KSQL quick start page](/docs/quickstart#quick-start) to start querying the Kafka cluster. Or you can do additional testing with topic data produced from the command line tools.

1.  You can produce Kafka data with the Kafka command line `kafka-console-producer` provided with the Confluent Platform. The following example generates data with a value in DELIMITED format.

    ```bash
    $ kafka-console-producer --broker-list localhost:9092  \
                             --topic t1 \
                             --property parse.key=true \
                             --property key.separator=:
    key1:v1,v2,v3
    key2:v4,v5,v6
    key3:v7,v8,v9
    key1:v10,v11,v12
    ```

1.  This example generates data with a value in JSON format.

    ```bash
    $ kafka-console-producer --broker-list localhost:9092 \
                             --topic t2 \
                             --property parse.key=true \
                             --property key.separator=:
    key1:{"id":"key1","col1":"v1","col2":"v2","col3":"v3"}
    key2:{"id":"key2","col1":"v4","col2":"v5","col3":"v6"}
    key3:{"id":"key3","col1":"v7","col2":"v8","col3":"v9"}
    key1:{"id":"key1","col1":"v10","col2":"v11","col3":"v12"}
    ```

