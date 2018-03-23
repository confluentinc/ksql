.. _ksql-security:

Configuring Security for KSQL
=============

KSQL supports many of the security features of both Apache Kafka and the |sr|.

- KSQL supports Apache Kafka security features such as :ref:`SSL for encryption <kafka_ssl_encryption>`,
  :ref:`SASL for authentication <kafka_sasl_auth>`, and :ref:`Authorization with ACLs <kafka_authorization>`.
- KSQL supports :ref:`Schema Registry security features <schemaregistry_security>` such as SSL and SASL.

To configure security for KSQL, add your configuration settings to the ``<path-to-confluent>/etc/ksql/ksql-server.properties``
file and then :ref:`start the KSQL server <install_ksql-server>` with your configuration file specified.

.. code:: bash

    $ <path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties

.. contents:: Table of Contents
    :local:

Configuring KSQL for |ccloud|
-----------------------------

You can use KSQL with a Kafka cluster in |ccloud|. For more information, see :ref:`install_ksql-ccloud`.

Configuring KSQL for Secured Confluent Schema Registry
------------------------------------------------------

The following configuration is connecting KSQL with the Confluent Schema Registry over HTTPS.

#. Specify the HTTPS endpoint in the ``ksql.schema.registry.url`` setting in the
   KSQL server configuration file:

   .. code:: bash

        ksql.schema.registry.url=https://<host-name-of-schema-registry>:<ssl-port>

#. If the Schema Registry's SSL certificate is not signed by a CA that is recognized by the JVM by default, then you
   must provide a suitable truststore when starting KSQL via the command line:

   .. code:: bash

      $ KSQL_OPTS="-Djavax.net.ssl.trustStore=<path-to-trust-store> -Djavax.net.ssl.trustStorePassword=<store-password>" ksql-server-start <props>

The exact settings you need depend on what SASL mechanism the Confluent Schema Registry is using is using and how your SSL certificates
are signed. For more information, see :ref:`schemaregistry_security`.

Configuration KSQL for Secured Apache Kafka clusters
----------------------------------------------------

To connect to a secured Kafka cluster, Kafka client applications must provide their security credentials. The following
are common configuration examples.

----------------------------------------------------------
Configuring Encrypted Communication between KSQL and Kafka
----------------------------------------------------------

You can connect to a secure Kafka cluster using PLAIN SASL and SSL, where the certificates have been signed by a CA trusted by
the default JVM trust store.

This configuration enables KSQL to connect to a secure Kafka cluster using PLAIN SASL, where the SSL certificates have been
signed by a CA trusted by the default JVM trust store. Other options include GSSAPI and Kerberos.

.. code:: bash

    security.protocol=SASL_SSL
    sasl.mechanism=PLAIN
    sasl.jaas.config=\
        org.apache.kafka.common.security.plain.PlainLoginModule required `
        username="<authenticated-ksql-user>" `
        password="<password>";

-------------------------------------------
Configuring Authentication of KSQL to Kafka
-------------------------------------------

This configuration connects to an Apache Kafka cluster using SASL and SSL and integrate with |c3-short|
using the :ref:`Control Center interceptors <controlcenter_clients>`.


.. code:: bash

    # Confluent Monitoring Interceptors for Control Center streams monitoring
    producer.interceptor.classes=io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor
    consumer.interceptor.classes=io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor

    # Confluent Monitoring interceptors SASL / SSL config
    confluent.monitoring.interceptor.security.protocol=SASL_SSL
    confluent.monitoring.interceptor.ssl.truststore.location=/etc/kafka/secrets/kafka.client.truststore.jks
    confluent.monitoring.interceptor.ssl.truststore.password=confluent
    confluent.monitoring.interceptor.ssl.keystore.location=/etc/kafka/secrets/kafka.client.keystore.jks
    confluent.monitoring.interceptor.ssl.keystore.password=confluent
    confluent.monitoring.interceptor.ssl.key.password=confluent
    confluent.monitoring.interceptor.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="client" password="client-secret";
    confluent.monitoring.interceptor.sasl.mechanism=PLAIN

The exact settings you need depend on what SASL mechanism your Kafka cluster is using and how your SSL certificates
are signed. For more information, see the `Security section of the Kafka documentation <http://kafka.apache.org/documentation.html#security>`__.

-------------------------------------------------
Configuring Authorization of KSQL with Kafka ACLs
-------------------------------------------------

You can use KSQL with Apache Kafka clusters that are secured with ACLs. The behavior depends on whether the KSQL cluster is
interactive or non-interactive.

The :ref:`Kafka Authorizer <kafka_authorization>` (``SimpleAclAuthorizer``) terminology is used to describe the ACLs.
Each ACL is made up of these parts:

Resource
    A resource is an object that adheres to the permissions defined by the ACL. A resource is comprised of a resource type
    and resource name:

    - ``RESOURCE_TYPE``, for example ``TOPIC`` or consumer ``GROUP``.
    - Resource name, where the name is either specific or the wildcard ``*``, meaning all resources of this type.

    The ``CLUSTER`` resource type does not require a resource name because it refers to the entire Kafka cluster.

Operation
    The operation that is performed on the resource, for example ``READ``.

Permission
    The type of access an ACL allows (``ALLOW``) or denies (``DENY``) access.

Principal
    An authenticated user or group. For example, ``"user: Fred"`` or ``"group: fraud"``. The identity of Kafka clients
    is the user principal which represents a specific authenticated user in a secure cluster.


The ACLs described below list a ``RESOURCE_TYPE``, resource name, and OPERATION. All ACLs described are ``ALLOW`` ACLs, where
the principal is the user the KSQL server has authenticated as, with the Apache Kafka cluster, or an appropriate group
that includes the authenticated KSQL user.

.. tip:: For more information about ACLs see :ref:`kafka_authorization` and for more information about interactive and
non-interactive queries, see :ref:`restrict-ksql-interactive`.

^^^^^^^^^^^^^^^^^^^^^^^^^
Interactive KSQL clusters
^^^^^^^^^^^^^^^^^^^^^^^^^

Kafka clusters that use ACLs to control access to resources require clients to authenticate as a particular user. Interactive
KSQL clusters require that this authenticated KSQL user has open access to create, read, write, delete topics, and use any
consumer group.

:ref:`Interactive KSQL clusters <restrict-ksql-interactive>` require these ACLs:

- Permission for the ``DESCRIBE_CONFIGS`` operation on the ``CLUSTER`` resource type.
- Permission for the ``CREATE`` operation on the ``CLUSTER`` resource type.
- Permissions for ``DESCRIBE``, ``READ``, ``WRITE`` and ``DELETE`` operations on all ``TOPIC`` resource types.
- Permissions for ``DESCRIBE`` and ``READ`` operations  on all ``GROUP`` resource types.

It is still possible to restrict the authenticated KSQL user from accessing specific resources using ``DENY`` ACLs. For
example, you can add a ``DENY`` ACL to stop KSQL queries from accessing a topic that contains sensitive data.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Non-Interactive (headless) KSQL clusters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:ref:`Non-interactive KSQL clusters <restrict-ksql-interactive>` can be run with much more restrictive ACLs, though it
currently requires a bit of effort to determine what ACLs are required. This will be improved in future KSQL releases.

Standard ACLs
    The authenticated KSQL user always requires:

    - ``DESCRIBE_CONFIGS`` permission on the ``CLUSTER`` resource type.
    - ``DESCRIBE`` permission on the ``__consumer_offsets`` topic.

Input topics
    The authenticated KSQL user requires ``DESCRIBE`` and ``READ`` permissions for each input topic. The topic should already exist
    when KSQL is started.

Output topics
    KSQL creates output topics when you run persistent CREATE TABLE AS STREAM and CREATE TABLE AS STREAM queries. The
    authenticated KSQL user requires ``DESCRIBE`` and ``WRITE`` permissions on each output topic.

    By default, KSQL will attempt to create any output topics that do not exist. To allow this the authenticated KSQL user requires
    ``CREATE`` permissions on the ``CLUSTER`` resource type. Alternatively, topics can be created manual before running KSQL. To determine
    the list of output topics and their required configuration, for example, partition count, replication factor,
    retention policy, etc, you can run KSQL on an open cluster first.

    If you want KSQL to create output topics, then the authenticated KSQL user should be granted ``CREATE`` permission on the ``CLUSTER`` resource type. KSQL
    will create output topics whenever you are doing a persistent CTAS/CTAS query.

Change-log and repartition topics
    Internally KSQL uses repartition topics and changelog topics for selected operations. KSQL requires repartition topics
    when using either ``PARTITION BY``, or using ``GROUP BY`` on non-key values, and requires changelog topics for any
    ``CREATE TABLE x AS`` statements.

    The authenticated KSQL user minimally requires a ``DESCRIBE``, ``READ``, and ``WRITE`` permissions for each changelog
    and repartition the ``TOPIC`` resource type.

    KSQL will attempt to create any repartition or changelog topics that do not exist. To allow this, the authenticated
    KSQL user requires ``CREATE`` permissions on the ``CLUSTER`` resource type. Alternatively, you can create topics manually
    before running KSQL. To determine the list of topics and their required configuration, you can run KSQL on an open
    cluster first (e.g. partition count, replication factor, and retention policy).

    All changelog and repartition topics are prefixed with ``confluent-ksql-query`` where ``ksql.service.id`` defaults to
    ``default_``. For more information, see :ref:`ksql-service-id`.

Consumer groups
    KSQL uses Kafka consumer groups when consuming input, change-log and repartition topics. The set of consumer groups
    that KSQL requires depends on the queries that are being executed. The easiest way to
    determine the list of consumer groups is to first run the queries on an open Kafka cluster and list the
    groups created. For more information about how to list groups, see
    `Managing Consumer Groups <http://kafka.apache.org/documentation.html#basic_ops_consumer_group>`__.

    Consumer group names are formatted like ``_confluent-ksql-<value of ksql.service.id property>_query_<query id>``,
    where the default of ``ksql.service.id`` is ``ksql_``.

    Authenticated KSQL users require a minimum of ``DESCRIBE`` and ``READ`` permissions for the ``GROUP`` resource type.


Learn More
    See the blog post `Secure Stream Processing with Apache Kafka, Confluent Platform and KSQL <https://www.confluent.io/blog/secure-stream-processing-apache-kafka-ksql/>`__
    and try out the :ref:`Monitoring Kafka streaming ETL deployments <cp-demo>` tutorial.

