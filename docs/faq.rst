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
product.

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

=============================
Is KSQL ready for production?
=============================

KSQL is a technical preview at this point in time. We do not yet
recommend its use for production purposes. The planned GA release date for KSQL is March 2018.

==============================================================
Can I use KSQL with my favorite data format (e.g. JSON, Avro)?
==============================================================

KSQL currently supports formats:

-  DELIMITED (e.g. CSV)
-  JSON
-  Avro (requires Confluent Schema Registry and setting ``ksql.schema.registry.url`` in the KSQL configuration file)

*Support for Apache Avro is expected soon.*

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
   (cf. :ref:`ksql_clickstream`).

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

========================================================
Can KQL connect to a Apache Kafka cluster over SASL_SSL?
========================================================

Yes! Internally, KSQL uses standard Kafka Consumers and Producers, which you
can configure to connect to a secure Kafka cluster as you would for any app.

For example, adding the following entries, to the property file you use to start
KSQL, will enable KSQL to connect to a cluster secured using _PLAIN_ SASL,
(as opposed to, say, GSSAPI / Kerberos), where the SSL certificates have been
signed by a CA trusted by the default JVM trust store:

.. code:: bash
    security.protocol=SASL_SSL
    sasl.mechanism=PLAIN
    sasl.jaas.config=\
        org.apache.kafka.common.security.plain.PlainLoginModule required `
        username="<name of the user KSQL should use>" `
        password="<the password>";

The exact settings you will need will vary depending on what SASL mechanism your
Kafka cluster is using and how your SSL certificates are signed. For full details,
please refer to the `Security section of the Kafka documentation
<http://kafka.apache.org/documentation.html#security>`__.
