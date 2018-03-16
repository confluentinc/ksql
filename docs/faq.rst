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

==============================================================
Can I use KSQL with my favorite data format (e.g. JSON, Avro)?
==============================================================

KSQL currently supports formats:

-  DELIMITED (e.g. comma-separated value)
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

Define ``bootstrap.servers`` in the :ref:`KSQL server config <common-configs>`.

.. _add-ksql-servers:

======================================================
How do I add KSQL servers to an existing KSQL cluster?
======================================================

Start the additional servers by using the existing Kafka cluster name as defined in ``bootstrap.servers`` and command topic name (``ksql.command.topic.suffix``). For more information, see :ref:`install_ksql-server`.

====================================================================================
How can I secure KSQL servers for production and prevent interactive client access?
====================================================================================

You can configure your servers to run a set of predefined queries by using ``ksql.queries.file`` or the ``--queries-file``
flag. For more information, see :ref:`common-configs`.

====================================================================
How do I use Avro data and integrate with Confluent Schema Registry?
====================================================================

Configure the ``ksql.schema.registry.url`` to point to Schema Registry (see :ref:`common-configs`).

.. important:: To use Avro data with KSQL you must have Schema Registry installed. This is included by default with |cpe|.

=========================
How can I scale out KSQL?
=========================

The maximum parallelism depends on the number of partitions.

- To scale out: start additional KSQL servers with same config. See :ref:`add-ksql-servers`.
- To scale in: stop the desired running KSQL servers, but keep at least one server running. The remaining servers should
  have sufficient capacity to take over work from stopped servers.

.. tip:: Idle servers will consume a small amount of resource. For example, if you have 10 KSQL servers and run a query
         against a two-partition input topic, only two servers perform the actual work, but the other eight will run an “idle”
         query.

=====================================================
Can KSQL connect to an Apache Kafka cluster over SSL?
=====================================================

Yes. Internally, KSQL uses standard Kafka consumers and producers.
The procedure to securely connect KSQL to Kafka is the same as connecting any app to Kafka.

For example, you can add the following entries to the KSQL server configuration file
(ksql-server.properties). This configuration enables KSQL to connect to a Kafka
cluster over SSL, given a trust store that will validate the SSL certificates being used
by the Kafka Brokers.

.. code:: bash
    security.protocol=SSL
    ssl.truststore.location=<path to trust store that trusts broker certificates>
    ssl.truststore.password=<trust store secret>

The exact settings you will need will vary depending on the security settings the Kafka brokers
are using and how your SSL certificates are signed. For full details, please refer to the
`Security section of the Kafka documentation
<http://kafka.apache.org/documentation.html#security>`__.

=================================================================================
Can KSQL connect to an Apache Kafka cluster over SSL and authenticate using SASL?
=================================================================================

Yes. Internally, KSQL uses standard Kafka consumers and producers.
The procedure to securely connect KSQL to Kafka is the same as connecting any app to Kafka.

For example, you can add the following entries to the KSQL server configuration file
(ksql-server.properties). This configuration enables KSQL to connect to a secure Kafka
cluster using _PLAIN_ SASL (other options include GSSAPI / Kerberos), where the SSL
certificates have been signed by a CA trusted by the default JVM trust store:

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

====================================
Will KSQL work with Confluent Cloud?
====================================

Running KSQL against an Apache Kafka cluster running in the cloud is pretty straight forward.
To do so, add the following to the KSQL configuration file, (ksql-server.properties):

... code:: bash
    bootstrap.servers=<a comma separated list of the the ccloud broker endpoints. eg. r0.great-app.confluent.aws.prod.cloud:9092,r1.great-app.confluent.aws.prod.cloud:9093,r2.great-app.confluent.aws.prod.cloud:9094>
    ksql.sink.replicas=3
    replication.factor=3
    security.protocol=SASL_SSL
    sasl.mechanism=PLAIN
    sasl.jaas.config=\
        org.apache.kafka.common.security.plain.PlainLoginModule required \
        username="<confluent cloud access key>" \
        password="<confluent cloud secret>";

For more information, see :ref:`install_ksql-ccloud`.

====================================================================
Will KSQL work with a Apache Kafka cluster secured using Kafka ACLs?
====================================================================

Interactive KSQL clusters
-------------------------

Interactive KSQL clusters currently require that the KSQL user has open access to
create, read, write and delete topics and use any consumer group.

The required ACLs are:
- *DESCRIBE_CONFIGS* permission on the *CLUSTER*.
- *CREATE* permission on the *CLUSTER*.
- *DESCRIBE*, *READ*, *WRITE* and *DELETE* permissions on the *<any>* *TOPIC*.
- *DESCRIBE* and *READ* permissions  on the *<any>* *GROUP*.

It is still possible to restrict the KSQL user from accessing specific resources
using *DENY* ACLs, e.g. adding a *DENY* ACL to stop KSQL queries from accessing a
topic containing sensitive data.

Non-interactive KSQL clusters
-----------------------------

Non-interactive KSQL clusters will run with much more restrictive ACLs,
though it currently requires a little effort to work out what ACLs are required.
This will be improved in upcoming releases.

Standard ACLs
    The KSQL user will always require:
    - *DESCRIBE_CONFIGS* permission on the *CLUSTER*.
    - *DESCRIBE* permission on the *__consumer_offsets* topic.

    If you would prefer KSQL to be able to create internal and/or sink topics then
    the KSQL user should also be granted:
    - *CREATE* permission on the *CLUSTER*.

Source topics
    For each source/input topic, the KSQL user will require *DESCRIBE* and *READ* permissions.
    The topic should already exist when KSQL is started.

Sink topics
    For each sink/output topic, the KSQL user will require *DESCRIBE* and *WRITE* permissions.
    If the topic does not already exist, then the user will also require *CREATE* permissions
    on the *CLUSTER*.

Change-log and repartition topics
    The set of change-log and repartitioning topics that KSQL will require will depend on the
    queries being executed. The easiest way to determine the list of topics is to first run
    the queries on an open Kafka cluster and list the topics created.

    All change-log and repartition topics are prefixed with
    ``_confluent-ksql-<value of ksql.service.id property>_query_<query id>_``
    where the default of ``ksql.service.id`` is ``ksql_``.

    The KSQL user will require a minimum of *DESCRIBE*, *READ* and *WRITE* permissions for
    each change-log and repartition *TOPIC*.

    If the KSQL user does not have *CREATE* permissions on the *CLUSTER*, then all change-log and
    repartition topics must already exist, with the same number of partitions as the source topic,
    and ``replication.factor`` replicas.

Consumer groups
    The set of consumer groups that KSQL will require will depend on the queries being executed.
    The easiest way to determine the list of consumer groups is to first run the queries on an
    open Kafka cluster and list the groups created.

    All consumer groups are have a name in the format:
    ``_confluent-ksql-<value of ksql.service.id property>_query_<query id>``
    where the default of ``ksql.service.id`` is ``ksql_``.

    The KSQL user will require a minimum of *DESCRIBE* and *READ* permissions for *GROUP*.

======================================================
Will KSQL work with a HTTPS Confluent Schema Registry?
======================================================

KSQL can be configured to communicate with the Confluent Schema Registry over HTTPS.
To achieve this you will need to:

-  Specify the HTTPS endpoint in the ``ksql.schema.registry.url`` setting in the
   KSQL configuration file:

    ... code:: bash
        ksql.schema.registry.url=https://<host-name-of-schema-registry>:<ssl-port>

-  If the Schema Registry's SSL certificate is not signed by a CA that is recognised by the JVM
   by default, then you will need to provide a suitable truststore via the ``KSQL_OPTS``
   environment variable:

   ... code:: bash
      $ export KSQL_OPTS="-Djavax.net.ssl.trustStore=<path-to-trust-store>
                          -Djavax.net.ssl.trustStorePassword=<store-password>"

   or on the commandline when starting KSQL:

   ... code:: bash
      $ KSQL_OPTS="-Djavax.net.ssl.trustStore=<path-to-trust-store> -Djavax.net.ssl.trustStorePassword=<store-password>" ksql-server-start <props>
