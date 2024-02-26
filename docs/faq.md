---
layout: page
title: ksqlDB Frequently Asked Questions
tagline: FAQ for ksqlDB
description: Common issues and gotchas when you use ksqlDB
---

What are the benefits of ksqlDB?
--------------------------------

ksqlDB allows you to query, read, write, and process data in {{ site.aktm }}
in real-time and at scale using a lightweight SQL syntax. ksqlDB does
not require proficiency with a programming language such as Java or
Scala, and you don't have to install a separate processing cluster
technology.

What are the technical requirements of ksqlDB?
----------------------------------------------

ksqlDB requires only:

1.  A Java runtime environment.
2.  Access to a {{ site.ak }} cluster for reading and writing data in
    real-time. The cluster can be on-premises or in the cloud. ksqlDB
    works with clusters running vanilla {{ site.ak }} as well as with
    clusters running the Kafka versions included in {{ site.cp }}.

We recommend using [Confluent
Platform](https://www.confluent.io/product/confluent-platform/) or
[Confluent Cloud](https://www.confluent.io/confluent-cloud/) to run
{{ site.ak }}. ksqlDB supports Java 8 and 11.

Is ksqlDB owned by the Apache Software Foundation?
--------------------------------------------------

No. {{ site.community }} ksqlDB is owned and maintained by
[Confluent Inc.](https://www.confluent.io/) as part of its
[Confluent Platform](https://www.confluent.io/product/confluent-platform/)
product. However, ksqlDB is licensed under the Confluent Community License.

How does ksqlDB compare to Apache Kafka's Streams API?
------------------------------------------------------

ksqlDB is complementary to the {{ site.kstreams }} API, and indeed executes
queries through {{ site.kstreams }} applications. They share some similarities
such as having very flexible deployment models so you can integrate them
easily into your existing technical and organizational processes and
tooling, regardless of whether you have opted for containers, VMs,
bare-metal machines, cloud services, or on-premise environments.

One of the key benefits of ksqlDB is that it does not require the user to
develop any code in Java or Scala. This enables users to leverage a
SQL-like interface alone to construct streaming ETL pipelines, to
respond to real-time, continuous business requests, to spot anomalies,
and more. ksqlDB is a great fit when your processing logic can be
naturally expressed through SQL.

For full-fledged stream processing applications {{ site.kstreams }} remains
a more appropriate choice. For example, implementing a finite state
machine that's driven by streams of data is easier to achieve in a
programming language like Java or Scala than in SQL. In {{ site.kstreams }},
you can also choose between the
[DSL](https://docs.confluent.io/current/streams/developer-guide/dsl-api.html)
(a functional programming API) and the
[Processor API](https://docs.confluent.io/current/streams/developer-guide/processor-api.html)
(an imperative programming API), and even combine the two.

As with many technologies, each has its sweet-spot based on technical
requirements, mission-criticality, and user skillset.

For more information, see
[Kafka Streams and ksqlDB Compared – How to Choose](https://www.confluent.io/blog/kafka-streams-vs-ksqldb-compared/).

Does ksqlDB work with vanilla Kafka clusters, or does it require the Kafka version included in Confluent Platform?
------------------------------------------------------------------------------------------------------------------

ksqlDB works with both vanilla {{ site.ak }} clusters as well as with the
Kafka versions included in {{ site.cp }}.

Does ksqlDB support Kafka's exactly-once processing semantics?
--------------------------------------------------------------

Yes, ksqlDB supports exactly-once processing, which means it will compute
correct results even in the face of failures such as machine crashes.

Can I use ksqlDB with my favorite data format, like JSON and Avro?
------------------------------------------------------------------

ksqlDB supports the following formats:

-   DELIMITED (for example, comma-separated values)
-   JSON
-   JSON_SR, for schema support in {{ site.sr }}.
-   Avro message values. Avro keys are not yet supported.
    Requires {{ site.sr }} and `ksql.schema.registry.url` in the ksqlDB
    server configuration file. For more information, see
    [Configure ksqlDB for Avro, Protobuf, and JSON schemas](operate-and-deploy/installation/server-config/avro-schema.md).
-   Protocol Buffers (Protobuf) 
-   KAFKA (for example, a `BIGINT` that's serialized using Kafka's
    standard `LongSerializer`).

For more information, see
[Serialization Formats](/reference/serialization#serialization-formats).

Is ksqlDB fully compliant with ANSI SQL?
----------------------------------------

ksqlDB uses a dialect inspired by ANSI SQL. It has some differences because
it's geared at processing streaming data. For example, ANSI SQL has no
notion of "windowing" for use cases such as performing aggregations on
data grouped into 5-minute windows, which is a commonly required
functionality in the streaming world.

How do I shut down a ksqlDB environment?
----------------------------------------

Exit ksqlDB CLI:

```bash
ksql> exit
```

If you're running with Confluent CLI, use the `confluent stop` command:

```bash
confluent stop KSQL
```

If you're running ksqlDB in Docker containers, stop the `ksqldb-server`
container:

```bash
docker stop <ksqldb-server-container-name>
```

If you're running ksqlDB as a system service, use the `systemctl stop`
command:

```bash
sudo systemctl stop confluent-ksql
```

For more information on shutting down {{ site.cp }}, see [Install and
Upgrade](https://docs.confluent.io/current/installation/index.html).

How do I configure the target Kafka cluster?
--------------------------------------------

Define `bootstrap.servers` in the
[ksqlDB server configuration](operate-and-deploy/installation/server-config/index.md).

How do I add ksqlDB servers to an existing ksqlDB cluster?
------------------------------------------------------

You can add or remove ksqlDB servers during live operations. ksqlDB servers
that have been configured to use the same Kafka cluster
(`bootstrap.servers`) and the same ksqlDB service ID (`ksql.service.id`)
form a given ksqlDB cluster.

To add a ksqlDB server to an existing ksqlDB cluster the server must be
configured with the same `bootstrap.servers` and `ksql.service.id`
settings as the ksqlDB cluster it should join. For more information, see
[Configuring ksqlDB Server](operate-and-deploy/installation/server-config/index.md) and
[Scaling ksqlDB](operate-and-deploy/capacity-planning.md#scaling-ksqldb).

How can I lock-down ksqlDB servers for production and prevent interactive client access?
----------------------------------------------------------------------------------------

You can configure your servers to run a set of predefined queries by
using `ksql.queries.file` or the `--queries-file` command line flag. For
more information, see [Configuring ksqlDB Server](operate-and-deploy/installation/server-config/index.md).

How do I use Avro data and integrate with Confluent Schema Registry?
--------------------------------------------------------------------

Configure the `ksql.schema.registry.url` property in the ksqlDB server
configuration to point to {{ site.sr }}. For more information, see
[Configure ksqlDB for Avro, Protobuf, and JSON schemas](operate-and-deploy/installation/server-config/avro-schema.md).

!!! important
	-   To use Avro data with ksqlDB you must have {{ site.sr }}
        installed. This is included by default with {{ site.cp }}.
    -   Avro message values are supported. Avro keys are not yet
        supported.

How can I scale out ksqlDB?
---------------------------

The maximum parallelism depends on the number of partitions.

-   To scale out: start additional ksqlDB servers with the same config. This
    can be done during live operations. For more information, see
    [How do I add ksqlDB servers to an existing ksqlDB cluster?](#how-do-i-add-ksqldb-servers-to-an-existing-ksqldb-cluster).
-   To scale in: stop the desired running ksqlDB servers, but keep at
    least one server running. This can be done during live operations.
    The remaining servers should have sufficient capacity to take over
    work from stopped servers.

!!! tip
	Idle servers will consume a small amount of resources. For example, if
    you have 10 ksqlDB servers and run a query against a two-partition input
    topic, only two servers perform the actual work, but the other eight
    will run an "idle" query.

Can ksqlDB connect to an Apache Kafka cluster over SSL?
-----------------------------------------------------

Yes. Internally, ksqlDB uses standard Kafka consumers and producers. The
procedure to securely connect ksqlDB to Kafka is the same as connecting
any app to Kafka. For more information, see
[Configuring Kafka Encrypted Communication](operate-and-deploy/installation/server-config/security.md#configuring-kafka-encrypted-communication).

Can ksqlDB connect to an Apache Kafka cluster over SSL and authenticate using SASL?
-----------------------------------------------------------------------------------

Yes. Internally, ksqlDB uses standard Kafka consumers and producers. The
procedure to connect ksqlDB securely to Kafka is the same as connecting
any app to Kafka.

For more information, see
[Configure Kafka Authentication](operate-and-deploy/installation/server-config/security.md#configure-kafka-authentication).

Will ksqlDB work with Confluent Cloud?
--------------------------------------

Yes. Running ksqlDB against a {{ site.ak }} cluster running in the cloud
is pretty straightforward. For more information, see
[Connecting ksqlDB to Confluent Cloud](https://docs.confluent.io/current/cloud/cp-component/ksql-cloud-config.html).

Will ksqlDB work with a Kafka cluster secured using Kafka ACLs?
---------------------------------------------------------------

Yes. For more information, see
[Configure Authorization of ksqlDB with Kafka ACLs](operate-and-deploy/installation/server-config/security.md#configure-authorization-of-ksqldb-with-kafka-acls).

Will ksqlDB work with an HTTPS Schema Registry?
-----------------------------------------------

Yes. ksqlDB can be configured to communicate with {{ site.srlong }} over HTTPS.
For more information, see
[Configure ksqlDB for Secured {{ site.srlong }}](operate-and-deploy/installation/server-config/security.md#configure-ksqldb-for-secured-confluent-schema-registry).

Where are ksqlDB-related data and metadata stored?
--------------------------------------------------

In interactive mode, ksqlDB stores metadata in and builds metadata from
the ksqlDB command topic. To secure the metadata, you must secure the
command topic.

The ksqlDB command topic stores all data definition language (DDL)
statements: CREATE STREAM, CREATE TABLE, DROP STREAM, and DROP TABLE.
Also, the ksqlDB command topic stores TERMINATE statements, which stop
persistent queries based on CREATE STREAM AS SELECT (CSAS) and CREATE
TABLE AS SELECT (CTAS).

Currently, data manipulation language (DML) statements, like UPDATE and
DELETE aren't available.

In headless mode, ksqlDB stores metadata in the config topic. The config
topic stores the ksqlDB properties provided to ksqlDB when the application
was first started. ksqlDB uses these configs to ensure that your SQL
queries are built compatibly on every restart of the server.

Which ksqlDB queries read or write data to Kafka?
-------------------------------------------------

SHOW STREAMS and EXPLAIN <query> statements run against the ksqlDB
server that the ksqlDB client is connected to. They don't communicate
directly with Kafka.

CREATE STREAM WITH <topic> and CREATE TABLE WITH <topic> write
metadata to the ksqlDB command topic.

Persistent queries based on CREATE STREAM AS SELECT and CREATE TABLE AS
SELECT read and write to Kafka topics.

Non-persistent queries based on SELECT that are stateless only read from
Kafka topics, for example SELECT ... FROM foo WHERE ....

Non-persistent queries that are stateful read and write to Kafka, for
example, COUNT and JOIN. The data in Kafka is deleted automatically when
you terminate the query with CTRL-C.

How do I check the health of a ksqlDB server?
---------------------------------------------

Use the `ps` command to check whether the ksqlDB server process is
running, for example:

```bash
ps -aux | grep ksql
```

Your output should resemble:

```
jim       2540  5.2  2.3 8923244 387388 tty2   Sl   07:48   0:33 /usr/lib/jvm/java-8-oracle/bin/java -cp /home/jim/confluent-{{ site.ksqldbversion }}/share/java/monitoring-interceptors/* ...
```

If the process status of the JVM isn't `Sl` or `Ssl`, the ksqlDB Server
may be down.

If you're running ksqlDB Server in a Docker container, run the
`docker ps` or `docker-compose ps` command, and check that the status of
the `ksql-server` container is `Up`. Check the health of the process in
the container by running `docker logs <ksql-server-container-id>`.

Check runtime stats for the ksqlDB server that you're connected to.

- Run SHOW STREAMS or SHOW TABLES, then run `DESCRIBE <stream|table> EXTENDED`.
- Run SHOW QUERIES, then run `EXPLAIN <query>`.

The ksqlDB REST API supports a "server info" request (for example,
`http://<ksql-server-url>/info`), which returns info such as the ksqlDB
version. For more info, see [ksqlDB REST API Reference](developer-guide/api.md).

What if automatic topic creation is turned off?
-----------------------------------------------

If automatic topic creation is disabled, ksqlDB and {{ site.kstreams }}
applications continue to work. ksqlDB and {{ site.kstreams }} applications use
the Admin Client, so topics are still created.
