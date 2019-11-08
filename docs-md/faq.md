---
layout: page
title: ksqlDB Frequently Asked Questions
tagline: FAQ for ksqlDB
description: Common issues and gotchas when you use ksqlDB
---

What are the benefits of KSQL?
------------------------------

KSQL allows you to query, read, write, and process data in {{ site.aktm
}} in real-time and at scale using intuitive SQL-like syntax. KSQL does
not require proficiency with a programming language such as Java or
Scala, and you don't have to install a separate processing cluster
technology.

What are the technical requirements of KSQL?
--------------------------------------------

KSQL requires only:

1.  A Java runtime environment
2.  Access to a {{ site.ak }} cluster for reading and writing data in
    real-time. The cluster can be on-premises or in the cloud. KSQL
    works with clusters running vanilla {{ site.ak }} as well as with
    clusters running the Kafka versions included in {{ site.cp }}.

We recommend using [Confluent
Platform](https://www.confluent.io/product/confluent-platform/) or
[Confluent Cloud](https://www.confluent.io/confluent-cloud/) to run
{{ site.ak }}.

Is KSQL owned by the Apache Software Foundation?
------------------------------------------------

No. {{ site.community }} KSQL is owned and maintained by [Confluent
Inc.](https://www.confluent.io/) as part of its [Confluent
Platform](https://www.confluent.io/product/confluent-platform/) product.
However, KSQL is licensed under the Confluent Community License.

How does KSQL compare to Apache Kafka's Streams API?
----------------------------------------------------

KSQL is complementary to the {{ site.kstreams }} API, and indeed executes
queries through {{ site.kstreams }} applications. They share some similarities
such as having very flexible deployment models so you can integrate them
easily into your existing technical and organizational processes and
tooling, regardless of whether you have opted for containers, VMs,
bare-metal machines, cloud services, or on-premise environments.

One of the key benefits of KSQL is that it does not require the user to
develop any code in Java or Scala. This enables users to leverage a
SQL-like interface alone to construct streaming ETL pipelines, to
respond to real-time, continuous business requests, to spot anomalies,
and more. KSQL is a great fit when your processing logic can be
naturally expressed through SQL.

For full-fledged stream processing applications {{ site.kstreams }} remains
a more appropriate choice. For example, implementing a finite state
machine that's driven by streams of data is easier to achieve in a
programming language like Java or Scala than in SQL. In {{ site.kstreams }},
you can also choose between the
[DSL](https://docs.confluent.io/current/streams/developer-guide/dsl-api.html)
(a functional programming API) and the [Processor
API](https://docs.confluent.io/current/streams/developer-guide/processor-api.html)
(an imperative programming API), and even combine the two.

As with many technologies, each has its sweet-spot based on technical
requirements, mission-criticality, and user skillset.

Does KSQL work with vanilla Kafka clusters, or does it require the Kafka version included in Confluent Platform?
----------------------------------------------------------------------------------------------------------------

KSQL works with both vanilla {{ site.ak }} clusters as well as with the
Kafka versions included in {{ site.cp }}.

Does KSQL support Kafka's exactly-once processing semantics?
------------------------------------------------------------

Yes, KSQL supports exactly-once processing, which means it will compute
correct results even in the face of failures such as machine crashes.

Can I use KSQL with my favorite data format, like JSON and Avro?
----------------------------------------------------------------

KSQL currently supports the following formats:

-   DELIMITED (for example, comma-separated values)
-   JSON
-   Avro message values. Avro keys are not yet supported.
    Requires {{ site.sr }} and `ksql.schema.registry.url` in the KSQL
    server configuration file. For more information, see
    [Configure Avro and {{ site.sr }} for KSQL](installation/server-config/avro-schema.md).
-   KAFKA (for example, a `BIGINT` that's serialized using Kafka's
    standard `LongSerializer`).

For more information, see
[Serialization Formats](developer-guide/serialization.md#serialization-formats).

Is KSQL fully compliant with ANSI SQL?
--------------------------------------

KSQL is a dialect inspired by ANSI SQL. It has some differences because
it's geared at processing streaming data. For example, ANSI SQL has no
notion of "windowing" for use cases such as performing aggregations on
data grouped into 5-minute windows, which is a commonly required
functionality in the streaming world.

How do I shut down a KSQL environment?
--------------------------------------

Exit KSQL CLI:

```bash
ksql> exit
```

If you're running with Confluent CLI, use the `confluent stop` command:

```bash
confluent stop KSQL
```

If you're running KSQL in Docker containers, stop the `cp-ksql-server`
container:

```bash
docker stop <cp-ksql-server-container-name>
```

If you're running KSQL as a system service, use the `systemctl stop`
command:

```bash
sudo systemctl stop confluent-ksql
```

For more information on shutting down {{ site.cp }}, see [Install and
Upgrade](https://docs.confluent.io/current/installation/index.html).

How do I configure the target Kafka cluster?
--------------------------------------------

Define `bootstrap.servers` in the
[KSQL server configuration](installation/server-config/index.md).

How do I add KSQL servers to an existing KSQL cluster?
------------------------------------------------------

You can add or remove KSQL servers during live operations. KSQL servers
that have been configured to use the same Kafka cluster
(`bootstrap.servers`) and the same KSQL service ID (`ksql.service.id`)
form a given KSQL cluster.

To add a KSQL server to an existing KSQL cluster the server must be
configured with the same `bootstrap.servers` and `ksql.service.id`
settings as the KSQL cluster it should join. For more information, see
[Configuring KSQL Server](installation/server-config/index.md) and
[Scaling KSQL](capacity-planning.md#scaling-ksql).

How can I lock-down KSQL servers for production and prevent interactive client access?
--------------------------------------------------------------------------------------

You can configure your servers to run a set of predefined queries by
using `ksql.queries.file` or the `--queries-file` command line flag. For
more information, see [Configuring KSQL Server](installation/server-config/index.md).

How do I use Avro data and integrate with Confluent Schema Registry?
--------------------------------------------------------------------

Configure the `ksql.schema.registry.url` property in the KSQL server
configuration to point to {{ site.sr }} (see
[Configure Avro and {{ site.sr }} for KSQL](installation/server-config/avro-schema.md#configure-avro-and-schema-registry-for-ksql)).

!!! important
	-   To use Avro data with KSQL you must have {{ site.sr }}
        installed. This is included by default with {{ site.cp }}.
    -   Avro message values are supported. Avro keys are not yet
        supported.

How can I scale out KSQL?
-------------------------

The maximum parallelism depends on the number of partitions.

-   To scale out: start additional KSQL servers with same config. This
    can be done during live operations. For more information, see
    [How do I add KSQL servers to an existing KSQL cluster?](#how-do-i-add-ksql-servers-to-an-existing-ksql-cluster).
-   To scale in: stop the desired running KSQL servers, but keep at
    least one server running. This can be done during live operations.
    The remaining servers should have sufficient capacity to take over
    work from stopped servers.

!!! tip
	Idle servers will consume a small amount of resource. For example, if
    you have 10 KSQL servers and run a query against a two-partition input
    topic, only two servers perform the actual work, but the other eight
    will run an "idle" query.

Can KSQL connect to an Apache Kafka cluster over SSL?
-----------------------------------------------------

Yes. Internally, KSQL uses standard Kafka consumers and producers. The
procedure to securely connect KSQL to Kafka is the same as connecting
any app to Kafka. For more information, see
[Configuring Kafka Encrypted Communication](installation/server-config/security.md#configuring-kafka-encrypted-communication).

Can KSQL connect to an Apache Kafka cluster over SSL and authenticate using SASL?
---------------------------------------------------------------------------------

Yes. Internally, KSQL uses standard Kafka consumers and producers. The
procedure to connect KSQL securely to Kafka is the same as connecting
any app to Kafka.

For more information, see
[Configure Kafka Authentication](installation/server-config/security.md#configure-kafka-authentication).

Will KSQL work with Confluent Cloud?
------------------------------------

Yes. Running KSQL against a {{ site.ak }} cluster running in the cloud
is pretty straightforward. For more information, see
[Connecting KSQL to Confluent Cloud](https://docs.confluent.io/current/cloud/connect/ksql-cloud-config.html).

Will KSQL work with a Kafka cluster secured using Kafka ACLs?
---------------------------------------------------------------------

Yes. For more information, see
[Configure Authorization of KSQL with Kafka ACLs](installation/server-config/security.md#configure-authorization-of-ksql-with-kafka-acls).

Will KSQL work with an HTTPS Schema Registry?
---------------------------------------------

Yes. KSQL can be configured to communicate with {{ site.srlong }} over HTTPS.
For more information, see
[Configure KSQL for Secured {{ site.srlong }}](installation/server-config/security.md#configure-ksql-for-https).

Where are KSQL-related data and metadata stored?
------------------------------------------------

In interactive mode, KSQL stores metadata in and builds metadata from
the KSQL command topic. To secure the metadata, you must secure the
command topic.

The KSQL command topic stores all data definition language (DDL)
statements: CREATE STREAM, CREATE TABLE, DROP STREAM, and DROP TABLE.
Also, the KSQL command topic stores TERMINATE statements, which stop
persistent queries based on CREATE STREAM AS SELECT (CSAS) and CREATE
TABLE AS SELECT (CTAS).

Currently, data manipulation language (DML) statements, like UPDATE and
DELETE aren't available.

In headless mode, KSQL stores metadata in the config topic. The config
topic stores the KSQL properties provided to KSQL when the application
was first started. KSQL uses these configs to ensure that your KSQL
queries are built compatibly on every restart of the server.

Which KSQL queries read or write data to Kafka?
-----------------------------------------------

SHOW STREAMS and EXPLAIN <query> statements run against the KSQL
server that the KSQL client is connected to. They don't communicate
directly with Kafka.

CREATE STREAM WITH <topic> and CREATE TABLE WITH <topic> write
metadata to the KSQL command topic.

Persistent queries based on CREATE STREAM AS SELECT and CREATE TABLE AS
SELECT read and write to Kafka topics.

Non-persistent queries based on SELECT that are stateless only read from
Kafka topics, for example SELECT ... FROM foo WHERE ....

Non-persistent queries that are stateful read and write to Kafka, for
example, COUNT and JOIN. The data in Kafka is deleted automatically when
you terminate the query with CTRL-C.

How do I check the health of a KSQL server?
-------------------------------------------

Use the `ps` command to check whether the KSQL server process is
running, for example:

```bash
ps -aux | grep ksql
```

Your output should resemble:

```
jim       2540  5.2  2.3 8923244 387388 tty2   Sl   07:48   0:33 /usr/lib/jvm/java-8-oracle/bin/java -cp /home/jim/confluent-{{ site.release }}/share/java/monitoring-interceptors/* ...
```

If the process status of the JVM isn't `Sl` or `Ssl`, the KSQL server
may be down.

If you're running KSQL server in a Docker container, run the
`docker ps` or `docker-compose ps` command, and check that the status of
the `ksql-server` container is `Up`. Check the health of the process in
the container by running `docker logs <ksql-server-container-id>`.

Check runtime stats for the KSQL server that you\'re connected to.

- Run `ksql-print-metrics` on a server host. The tool connects to
a KSQL server that's running on `localhost` and collects JMX
metrics from the server process. Metrics include the number of
messages, the total throughput, the throughput distribution, and
the error rate.
- Run SHOW STREAMS or SHOW TABLES, then run `DESCRIBE EXTENDED <stream|table>`.
- Run SHOW QUERIES, then run `EXPLAIN <query>`.

The KSQL REST API supports a "server info" request (for example,
`http://<ksql-server-url>/info`), which returns info such as the KSQL
version. For more info, see [KSQL REST API Reference](developer-guide/api.md).

What if automatic topic creation is turned off?
-----------------------------------------------

If automatic topic creation is disabled, KSQL and {{ site.kstreams }}
applications continue to work. KSQL and {{ site.kstreams }} applications use
the Admin Client, so topics are still created.

Page last revised on: {{ git_revision_date }}
