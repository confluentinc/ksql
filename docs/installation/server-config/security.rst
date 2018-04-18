.. _ksql-security:

Configuring Security for KSQL
=============

KSQL supports many of the security features of both Apache Kafka and the |sr|.

- KSQL supports Apache Kafka security features such as :ref:`SSL for encryption <kafka_ssl_encryption>`,
  :ref:`SASL for authentication <kafka_sasl_auth>`, and :ref:`authorization with ACLs <kafka_authorization>`.
- KSQL supports :ref:`Schema Registry security features <schemaregistry_security>` such SSL for encryption
and mutual authentication for authorization.

To configure security for KSQL, add your configuration settings to the ``<path-to-confluent>/etc/ksql/ksql-server.properties``
file and then :ref:`start the KSQL server <install_ksql-server>` with your configuration file specified.

.. code:: bash

    $ <path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties

.. contents:: Table of Contents
    :local:

Configuring KSQL for |ccloud|
-----------------------------

You can use KSQL with a Kafka cluster in |ccloud|. For more information, see :ref:`install_ksql-ccloud`.

.. _config-security-ksql-sr:

Configuring KSQL for Secured Confluent Schema Registry
------------------------------------------------------

KSQL can be configured to connect to the Schema Registry over HTTP by setting the
``ksql.schema.registry.url`` to the Schema Registry's HTTPS endpoint.
Depending on your security setup, you might also need to supply additional SSL configuration.
For example, a trustStore is required if the Schema Registry's SSL certificates are not trusted by
the JVM by default; a keyStore is required if the Schema Registry requires mutual authentication.

SSL configuration for communication with the Schema Registry can be supplied using none-prefixed,
e.g. `ssl.truststore.location`, or prefixed e.g. `ksql.schema.registry.ssl.truststore.location`,
names. Non-prefixed names are used for settings that are shared with other communication
channels, i.e. where the same settings are required to configure SSL communication
with both Kafka and Schema Registry. Prefixed names only affects communication with Schema registry
and overrides any non-prefixed setting of the same name.

Use the following to configure KSQL to communicate with the Schema Registry over HTTPS,
where mutual authentication is not required and the Schema Registry's SSL certificates are trusted
by the JVM:

.. code:: bash

    ksql.schema.registry.url=https://<host-name-of-schema-registry>:<ssl-port>

Use the following to configure KSQL to communicate with the Schema Registry over HTTPS, with
mutual authentication, with an explicit trustStore, and where the SSL configuration is shared
between Kafka and Schema Registry:

.. code:: bash

    ksql.schema.registry.url=https://<host-name-of-schema-registry>:<ssl-port>
    ssl.truststore.location=/etc/kafka/secrets/ksql.truststore.jks
    ssl.truststore.password=confluent
    ssl.keystore.location=/etc/kafka/secrets/ksql.keystore.jks
    ssl.keystore.password=confluent
    ssl.key.password=confluent

Use the following to configure KSQL to communicate with the Schema Registry over HTTP, without
mutual authentication and with an explicit trustStore. These settings explicitly configure only
KSQL to Schema Registry SSL communication.

.. code:: bash

    ksql.schema.registry.url=https://<host-name-of-schema-registry>:<ssl-port>
    ksql.schema.registry.ssl.truststore.location=/etc/kafka/secrets/sr.truststore.jks
    ksql.schema.registry.ssl.truststore.password=confluent

The exact settings will vary depending on the encryption and authentication mechanisms the
Confluent Schema Registry is using, and how your SSL certificates are signed.
For more information, see :ref:`schemaregistry_security`.

.. _config-security-kafka:

Configuring KSQL for Secured Apache Kafka clusters
--------------------------------------------------

The following are common configuration examples.

.. _config-security-ssl:

-----------------------------------------
Configuring Kafka Encrypted Communication
-----------------------------------------

This configuration enables KSQL to connect to a Kafka cluster over SSL, with a user supplied trust store:

.. code:: bash
    security.protocol=SSL
    ssl.truststore.location=/etc/kafka/secrets/kafka.client.truststore.jks
    ssl.truststore.password=confluent

The exact settings will vary depending on the security settings of the Kafka brokers,
and how your SSL certificates are signed. For full details, and instructions on how to create
suitable trust stores, please refer to the :ref:`Security Guide <security>`.

.. _config-security-ssl-sasl:

--------------------------------
Configuring Kafka Authentication
--------------------------------

This configuration enables KSQL to connect to a secure Kafka cluster using PLAIN SASL, where the SSL certificates have been
signed by a CA trusted by the default JVM trust store.

.. code:: bash

    security.protocol=SASL_SSL
    sasl.mechanism=PLAIN
    sasl.jaas.config=\
        org.apache.kafka.common.security.plain.PlainLoginModule required `
        username="<ksql-user>" `
        password="<password>";

The exact settings will vary depending on what SASL mechanism your Kafka cluster is using and how your SSL certificates are
signed. For more information, see the :ref:`Security Guide <security>`.

.. _config-security-ksql-acl:

-------------------------------------------------
Configuring Authorization of KSQL with Kafka ACLs
-------------------------------------------------

Kafka clusters can use ACLs to control access to resources. Such clusters require each client to authenticate as a particular user.
To work with such clusters, KSQL must be configured to :ref:`authenticate with the Kafka cluster <config-security-ssl-sasl>`,
and certain ACLs must be defined in the Kafka cluster to allow the user KSQL is authenticating as access to resources.
The list of ACLs that must be defined depends on whether the KSQL cluster is configured for
:ref:`interactive <config-security-ksql-acl-interactive>` or :ref:`non-interactive (headless) <config-security-ksql-acl-headless>`.

This section uses the terminology used by the :ref:`Kafka Authorizer <kafka_authorization>` (``SimpleAclAuthorizer``)
to describe the required ACLs. Each ACL is made up of these parts:

Resource
    A resource is comprised of a resource type and resource name:

    - ``RESOURCE_TYPE``, for example ``TOPIC`` or consumer ``GROUP``.
    - Resource name, where the name is either specific, e.g. ``users``, or the wildcard ``*``, meaning all resources of this type.

    The ``CLUSTER`` resource type does not require a resource name because it refers to the entire Kafka cluster.

Operation
    The operation that is performed on the resource, for example ``READ``.

Permission
    Defines if the ACL allows (``ALLOW``) or denies (``DENY``) access to the resource.

Principal
    An authenticated user or group. For example, ``"user: Fred"`` or ``"group: fraud"``. 

An example ACL might ``ALLOW`` ``user Fred`` to ``READ`` the ``TOPIC`` named ``users``.

The ACLs described below list a ``RESOURCE_TYPE``, resource name, and ``OPERATION``. All ACLs described are ``ALLOW`` ACLs, where
the principal is the user the KSQL server has authenticated as, with the Apache Kafka cluster, or an appropriate group
that includes the authenticated KSQL user.

.. tip:: For more information about ACLs see :ref:`kafka_authorization` and for more information about interactive and
non-interactive queries, see :ref:`restrict-ksql-interactive`.

.. _config-security-ksql-acl-interactive:

^^^^^^^^^^^^^^^^^^^^^^^^^
Interactive KSQL clusters
^^^^^^^^^^^^^^^^^^^^^^^^^

:ref:`Interactive KSQL clusters <restrict-ksql-interactive>`, (which is the default configuration),
require that the authenticated KSQL user has open access to create, read, write, delete topics, and use any consumer group:

Interactive KSQL clusters require these ACLs:

- Permission for the ``DESCRIBE_CONFIGS`` operation on the ``CLUSTER`` resource type.
- Permission for the ``CREATE`` operation on the ``CLUSTER`` resource type.
- Permissions for ``DESCRIBE``, ``READ``, ``WRITE`` and ``DELETE`` operations on all ``TOPIC`` resource types.
- Permissions for ``DESCRIBE`` and ``READ`` operations on all ``GROUP`` resource types.

It is still possible to restrict the authenticated KSQL user from accessing specific resources using ``DENY`` ACLs. For
example, you can add a ``DENY`` ACL to stop KSQL queries from accessing a topic that contains sensitive data.

.. _config-security-ksql-acl-headless:

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Non-Interactive (headless) KSQL clusters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Because the list of queries are known ahead of time, you can run
:ref:`Non-interactive KSQL clusters <restrict-ksql-interactive>`  with more restrictive ACLs.
Determining the list of ACLs currently requires a bit of effort. This will be improved in future KSQL releases.

Standard ACLs
    The authenticated KSQL user always requires:

    - ``DESCRIBE_CONFIGS`` permission on the ``CLUSTER`` resource type.
    - ``DESCRIBE`` permission on the ``__consumer_offsets`` topic.

Input topics
    An input topic is one that has been imported into KSQL using a ``CREATE STREAM`` or ``CREATE TABLE``
    statement. The topic should already exist when KSQL is started.

    The authenticated KSQL user requires ``DESCRIBE`` and ``READ`` permissions for each input topic.

Output topics
    KSQL creates output topics when you run persistent ``CREATE TABLE AS SELECT`` or ``CREATE TABLE AS SELECT`` queries.

    The authenticated KSQL user requires ``DESCRIBE`` and ``WRITE`` permissions on each output topic.

    By default, KSQL will attempt to create any output topics that do not exist. To allow this, the authenticated KSQL user requires
    ``CREATE`` permissions on the ``CLUSTER`` resource type. Alternatively, topics can be created manually before running KSQL. To determine
    the list of output topics and their required configuration, (partition count, replication factor,
    retention policy, etc), you can run initially run KSQL on a Kafka cluster with none or open ACLs first.

Change-log and repartition topics
    Internally, KSQL uses repartition and changelog topics for selected operations. KSQL requires repartition topics
    when using either ``PARTITION BY``, or using ``GROUP BY`` on non-key values, and requires changelog topics for any
    ``CREATE TABLE x AS`` statements.

    The authenticated KSQL user requires ``DESCRIBE``, ``READ``, and ``WRITE`` permissions for each changelog
    and repartition ``TOPIC``.

    By default, KSQL will attempt to create any repartition or changelog topics that do not exist. To allow this, the authenticated
    KSQL user requires ``CREATE`` permissions on the ``CLUSTER`` resource type. Alternatively, you can create topics manually
    before running KSQL. To determine the list of output topics and their required configuration, (partition count,
    replication factor, retention policy, etc), you can run initially run KSQL on a Kafka cluster with none or open ACLs first.

    All changelog and repartition topics are prefixed with ``confluent-ksql-<ksql-service-id>`` where ``ksql.service.id`` defaults to
    ``default_``, (for more information, see :ref:`ksql-service-id`), and postfixed with either ``-changelog`` or ``-repartition``,
    respectively.

Consumer groups
    KSQL uses Kafka consumer groups when consuming input, change-log and repartition topics. The set of consumer groups
    that KSQL requires depends on the queries that are being executed.

    The authenticated KSQL user requires ``DESCRIBE`` and ``READ`` permissions for each consumer ``GROUP``.

    The easiest way to determine the list of consumer groups is to initially run the queries on a Kafka cluster
    with none or open ACLS and then list the groups created. For more information about how to list groups, see
    `Managing Consumer Groups <http://kafka.apache.org/documentation.html#basic_ops_consumer_group>`__.

    Consumer group names are formatted like ``_confluent-ksql-<value of ksql.service.id property>_query_<query id>``,
    where the default of ``ksql.service.id`` is ``ksql_``.

----------------------------------------------
Configuring |c3-short| Monitoring Interceptors
----------------------------------------------

This configuration enables SASL and SSL for the :ref:`monitoring intercepts <controlcenter_clients>` that integrate KSQL
with |c3-short|.


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
    confluent.monitoring.interceptor.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="ksql-user" password="ksql-user-secret";
    confluent.monitoring.interceptor.sasl.mechanism=PLAIN

Learn More
    See the blog post `Secure Stream Processing with Apache Kafka, Confluent Platform and KSQL <https://www.confluent.io/blog/secure-stream-processing-apache-kafka-ksql/>`__
    and try out the :ref:`Monitoring Kafka streaming ETL deployments <cp-demo>` tutorial.