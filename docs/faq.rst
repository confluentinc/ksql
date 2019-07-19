.. _ksql_faq:

Frequently Asked Questions
==========================

==============================
What are the benefits of KSQL?
==============================

KSQL allows you to query, read, write, and process data in |ak-tm|
in real-time and at scale using intuitive SQL-like syntax. KSQL does not
require proficiency with a programming language such as Java or Scala,
and you don’t have to install a separate processing cluster technology.

============================================
What are the technical requirements of KSQL?
============================================

KSQL only requires:

1. A Java runtime environment
2. Access to an Apache Kafka cluster for reading and writing data in
   real-time. The cluster can be on-premises or in the cloud. KSQL works
   with clusters running vanilla Apache Kafka as well as with clusters
   running the Kafka versions included in Confluent Platform.

We recommend the use of `Confluent
Platform <https://www.confluent.io/product/confluent-platform/>`__ or
`Confluent Cloud <https://www.confluent.io/confluent-cloud/>`__ for
running Apache Kafka.

================================================
Is KSQL owned by the Apache Software Foundation?
================================================

No, KSQL is owned and maintained by `Confluent
Inc. <https://www.confluent.io/>`__ as part of its `Confluent Platform
<https://www.confluent.io/product/confluent-platform/>`__
product. However, KSQL is licensed under the Confluent Community License.

====================================================
How does KSQL compare to Apache Kafka’s Streams API?
====================================================

KSQL is complementary to the Kafka Streams API, and indeed executes queries through Kafka Streams applications. They share some similarities such as having very flexible deployment models so you can integrate them easily into your existing technical and organizational processes and tooling, regardless of whether you have opted for containers, VMs, bare-metal machines, cloud services, or on-premise environments.

One of the key benefits of KSQL is that it does not require the user to develop any code in Java or Scala. This enables users to leverage a SQL-like interface alone to construct streaming ETL pipelines, to respond to real-time, continuous business requests, to spot anomalies, and more. KSQL is a great fit when your processing logic can be naturally expressed through SQL.

For full-fledged stream processing applications Kafka Streams remains a more appropriate choice. For example, implementing a finite state machine that is driven by streams of data is easier to achieve in a programming language such as Java or Scala than in SQL. In Kafka Streams you can also choose between the :ref:`DSL <streams_developer-guide_dsl>` (a functional programming API) and the :ref:`Processor API <streams_developer-guide_processor-api>` (an imperative programming API), and even combine the two.

As with many technologies, each has its sweet-spot based on technical requirements, mission-criticality, and user skillset.

=======================================================================================================================
Does KSQL work with vanilla Apache Kafka clusters, or does it require the Kafka version included in Confluent Platform?
=======================================================================================================================

KSQL works with both vanilla Apache Kafka clusters as well as with the
Kafka versions included in Confluent Platform.

============================================================
Does KSQL support Kafka’s exactly-once processing semantics?
============================================================

Yes, KSQL supports exactly-once processing, which means it will compute
correct results even in the face of failures such as machine crashes.

==============================================================
Can I use KSQL with my favorite data format (e.g. JSON, Avro)?
==============================================================

KSQL currently supports formats:

-  DELIMITED (e.g. comma-separated value)
-  JSON
-  Avro message values are supported. Avro keys are not yet supported. Requires |sr| and ``ksql.schema.registry.url`` in the
   KSQL server configuration file. For more information, see :ref:`install_ksql-avro-schema`.
-  KAFKA (for example, a ``BIGINT`` that's serialized using Kafka's standard ``LongSerializer``).

See :ref:`ksql_formats` for more details.

====================================
Is KSQL fully compliant to ANSI SQL?
====================================

KSQL is a dialect inspired by ANSI SQL. It has some differences because
it is geared at processing streaming data. For example, ANSI SQL has no
notion of “windowing” for use cases such as performing aggregations on
data grouped into 5-minute windows, which is a commonly required
functionality in the streaming world.

======================================
How do I shut down a KSQL environment?
======================================

Exit KSQL CLI:

.. code:: bash

   ksql> exit

If you're running with Confluent CLI, use the ``confluent stop`` command:

.. code:: bash

   confluent stop KSQL

If you're running KSQL in Docker containers, stop the
``cp-ksql-server`` container:

.. code:: bash

   docker stop <cp-ksql-server-container-name>

If you're running KSQL as a system service, use the ``systemctl stop``
command:

.. code:: bash

   sudo systemctl stop confluent-ksql

For more information on shutting down |cp|, see
:ref:`installation-overview`.

============================================
How do I configure the target Kafka cluster?
============================================

Define ``bootstrap.servers`` in the :ref:`KSQL server configuration <ksql-server-config>`.

.. _add-ksql-servers:

======================================================
How do I add KSQL servers to an existing KSQL cluster?
======================================================

You can add or remove KSQL servers during live operations. KSQL servers that have been configured to use the same
Kafka cluster (``bootstrap.servers``) and the same KSQL service ID (``ksql.service.id``) form a given KSQL cluster.

To add a KSQL server to an existing KSQL cluster the server must be configured with the same ``bootstrap.servers`` and
``ksql.service.id`` settings as the KSQL cluster it should join. For more information, see :ref:`ksql-server-config`
and :ref:`ksql-capacity-planning-scaling`.

======================================================================================
How can I lock-down KSQL servers for production and prevent interactive client access?
======================================================================================

You can configure your servers to run a set of predefined queries by using ``ksql.queries.file`` or the
``--queries-file`` command line flag. For more information, see :ref:`ksql-server-config`.

====================================================
How do I use Avro data and integrate with |sr-long|?
====================================================

Configure the ``ksql.schema.registry.url`` property in the KSQL server configuration to point to |sr|
(see :ref:`install_ksql-avro-schema`).

.. important::
    - To use Avro data with KSQL you must have |sr| installed. This is included by default with |cp|.
    - Avro message values are supported. Avro keys are not yet supported.

=========================
How can I scale out KSQL?
=========================

The maximum parallelism depends on the number of partitions.

- To scale out: start additional KSQL servers with same config. This can be done during live operations.
  See :ref:`add-ksql-servers`.
- To scale in: stop the desired running KSQL servers, but keep at least one server running. This can be done during live
  operations. The remaining servers should have sufficient capacity to take over work from stopped servers.

.. tip:: Idle servers will consume a small amount of resource. For example, if you have 10 KSQL servers and run a query
         against a two-partition input topic, only two servers perform the actual work, but the other eight will run an
         "idle" query.

=====================================================
Can KSQL connect to an Apache Kafka cluster over SSL?
=====================================================

Yes. Internally, KSQL uses standard Kafka consumers and producers.
The procedure to securely connect KSQL to Kafka is the same as connecting any app to Kafka. For more information,
see :ref:`config-security-ssl`.

=================================================================================
Can KSQL connect to an Apache Kafka cluster over SSL and authenticate using SASL?
=================================================================================

Yes. Internally, KSQL uses standard Kafka consumers and producers.
The procedure to securely connect KSQL to Kafka is the same as connecting any app to Kafka.

For more information, see :ref:`config-security-ssl-sasl`.

====================================
Will KSQL work with Confluent Cloud?
====================================

Yes. Running KSQL against an Apache Kafka cluster running in the cloud is pretty straight forward. For more information, see :ref:`install_ksql-ccloud`.

====================================================================
Will KSQL work with a Apache Kafka cluster secured using Kafka ACLs?
====================================================================

Yes. For more information, see :ref:`config-security-ksql-acl`.

======================================
Will KSQL work with a HTTPS |sr-long|?
======================================

Yes. KSQL can be configured to communicate with |sr-long| over HTTPS. For more information, see
:ref:`config-security-ksql-sr`.

================================================
Where are KSQL-related data and metadata stored?
================================================

In interactive mode, KSQL stores metatada in and builds metadata ifrom the KSQL
command topic. To secure the metadata, you must secure the command topic.

The KSQL command topic stores all data definition language (DDL) statements:
CREATE STREAM, CREATE TABLE, DROP STREAM, and DROP TABLE. Also, the KSQL command
topic stores TERMINATE statements, which stop persistent queries based on
CREATE STREAM AS SELECT (CSAS) and CREATE TABLE AS SELECT (CTAS). 

Currently, data manipulation language (DML) statements, like UPDATE and DELETE
aren't available.

In headless mode, KSQL stores metadata in the config topic. The config topic stores
the KSQL properties provided to KSQL when the application was first started. KSQL
uses these configs to ensure that your KSQL queries are built compatibly on every
restart of the server.

===============================================
Which KSQL queries read or write data to Kafka?
===============================================

SHOW STREAMS and EXPLAIN <query> statements run against the KSQL server that
the KSQL client is connected to. They don't communicate directly with Kafka.

CREATE STREAM WITH <topic> and CREATE TABLE WITH <topic> write metadata to the
KSQL command topic.

Persistent queries based on CREATE STREAM AS SELECT and CREATE TABLE AS SELECT
read and write to Kafka topics.

Non-persistent queries based on SELECT that are stateless only read from Kafka
topics, for example SELECT … FROM foo WHERE ….

Non-persistent queries that are stateful read and write to Kafka, for example,
COUNT and JOIN. The data in Kafka is deleted automatically when you terminate
the query with CTRL-C.

===========================================
How do I check the health of a KSQL server?
===========================================

Use the ``ps`` command to check whether the KSQL server process is running, 
for example:

.. code:: bash

    ps -aux | grep ksql

Your output should resemble:

.. code:: bash

    jim       2540  5.2  2.3 8923244 387388 tty2   Sl   07:48   0:33 /usr/lib/jvm/java-8-oracle/bin/java -cp /home/jim/confluent-5.0.0/share/java/monitoring-interceptors/* ...

If the process status of the JVM isn't ``Sl`` or ``Ssl``, the KSQL server may be down.

If you're running KSQL server in a Docker container, run the ``docker ps`` or 
``docker-compose ps`` command, and check that the status of the ``ksql-server``
container is ``Up``. Check the health of the process in the container by running
``docker logs <ksql-server-container-id>``.

Check runtime stats for the KSQL server that you're connected to.
  - Run ``ksql-print-metrics`` on a server host. The tool connects to a KSQL server
    that's running on ``localhost`` and collects JMX metrics from the server process.
    Metrics include the number of messages, the total throughput, the throughput
    distribution, and the error rate. 
  - Run SHOW STREAMS or SHOW TABLES, then run DESCRIBE EXTENDED <stream|table>.
  - Run SHOW QUERIES, then run EXPLAIN <query>.

The KSQL REST API supports a "server info" request (for example, ``http://<ksql-server-url>/info``), 
which returns info such as the KSQL version. For more info, see :ref:`ksql-rest-api`.

=======================================================================
How do I set the retention period for streams created for KSQL queries?
=======================================================================

When you create a stream, you can set ``retention.ms`` for the output topic.
In the KSQL CLI, use the SET statement to assign a value to ``ksql.streams.retention.ms``:

.. code:: bash

    SET 'ksql.streams.retention.ms' = '86400000';

Make the setting global by assigning ``ksql.streams.retention.ms`` in the KSQL
server configuration file.

.. note:: If you set ``windowstore.changelog.additional.retention.ms``, the
          ``ksql.streams.retention.ms`` value is added to the retention period
          for changelog topics. For example, if you set ``ksql.streams.retention.ms``
          to 7 days, the sink topic retention is 7 days. If you set ``windowstore.changelog.additional.retention.ms``
          to 2 days, the retention for the internal changelog topic for
          statestore is the sum of these values: 7 + 2 = 9 days.

===============================================
What if automatic topic creation is turned off?
===============================================

If automatic topic creation is disabled, KSQL and Kafka Streams applications
continue to work. KSQL and Kafka Streams applications use the Admin Client,
so topics are still created.
