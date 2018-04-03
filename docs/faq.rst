.. _ksql_faq:

Frequently Asked Questions
==========================

.. contents:: Contents
    :local:
    :depth: 1

==============================
What are the benefits of KSQL?
==============================

KSQL allows you to query, read, write, and process data in Apache Kafka
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
Inc. <https://www.confluent.io/>`__ as part of its free `Confluent Open
Source <https://www.confluent.io/product/confluent-open-source/>`__
product. However, KSQL is licensed under the Apache 2.0 license.

====================================================
How does KSQL compare to Apache Kafka’s Streams API?
====================================================

KSQL is complementary to the Kafka Streams API, and indeed executes
queries through Kafka Streams applications. One of the key benefits of
KSQL is that it does not require the user to develop any code in Java or
Scala. This enables users to use a SQL-like interface alone to construct
streaming ETL pipelines, as well as responding to a real-time,
continuous business requests. For full-fledged stream processing
applications Kafka Streams remains a more appropriate choice. As with
many technologies, each has its sweet-spot based on technical
requirements, mission-criticality, and user skillset.

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
-  Avro (requires Confluent Schema Registry and ``ksql.schema.registry.url`` in the KSQL server configuration file, see :ref:`<install_ksql-avro-schema>`)

====================================
Is KSQL fully compliant to ANSI SQL?
====================================

KSQL is a dialect inspired by ANSI SQL. It has some differences because
it is geared at processing streaming data. For example, ANSI SQL has no
notion of “windowing” for use cases such as performing aggregations on
data grouped into 5-minute windows, which is a commonly required
functionality in the streaming world.

=====================================
How do I shutdown a KSQL environment?
=====================================

-  To stop DataGen tasks that were started with the ``-daemon`` flag
   (cf. :ref:`ksql_clickstream-local`).

   .. code:: bash

       $ jps | grep DataGen
       25379 DataGen
       $ kill 25379

-  Exit KSQL.

   .. code:: bash

       ksql> exit

-  Stop Confluent Platform by shutting down all services including
   Kafka.

   .. code:: bash

       $ confluent stop

-  To remove all data, topics, and streams:

   .. code:: bash

       $ confluent destroy

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
``ksql.service.id`` settings as the KSQL cluster it should join. For more information, see :ref:`ksql-server-config`.

======================================================================================
How can I lock-down KSQL servers for production and prevent interactive client access?
======================================================================================

You can configure your servers to run a set of predefined queries by using ``ksql.queries.file`` or the
``--queries-file`` command line flag. For more information, see :ref:`ksql-server-config`.

====================================================================
How do I use Avro data and integrate with Confluent Schema Registry?
====================================================================

Configure the ``ksql.schema.registry.url`` property in the KSQL server configuration to point to Schema Registry (see :ref:`install_ksql-avro-schema`).

.. important:: To use Avro data with KSQL you must have Schema Registry installed. This is included by default with |cp|.

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

======================================================
Will KSQL work with a HTTPS Confluent Schema Registry?
======================================================

Yes. KSQL can be configured to communicate with the Confluent Schema Registry over HTTPS. For more information, see
:ref:`config-security-ksql-sr`.
