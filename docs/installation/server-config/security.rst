.. _ksql-security:

Configuring Security for KSQL
=============================

KSQL supports many of the security features of both Apache Kafka and the |sr|.

- KSQL supports Apache Kafka security features such as :ref:`SSL for encryption <kafka_ssl_encryption>`,
  :ref:`SASL for authentication <kafka_sasl_auth>`, and :ref:`authorization with ACLs <kafka_authorization>`.
- KSQL supports :ref:`Schema Registry security features <schemaregistry_security>` such SSL for encryption
and mutual authentication for authorization.

To configure security for KSQL, add your configuration settings to the ``<path-to-confluent>/etc/ksql/ksql-server.properties``
file and then :ref:`start the KSQL server <start_ksql-server>` with your configuration file specified.

.. code:: bash

    $ <path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties

.. include:: ../../../../includes/installation-types-zip-tar.rst

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

You can pass authentication settings to the Schema Registry client used by KSQL
by adding the following to your KSQL server config.

.. code:: bash
    ksql.schema.registry.basic.auth.credentials.source=USER_INFO
    ksql.schema.registry.basic.auth.user.info=username:password

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
The list of ACLs that must be defined depends on the version of the Kafka cluster.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
|cp| v5.0 (Apache Kafka v2.0) and above
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

|cp| 5.0 simplifies the ACLs required to run KSQL against a Kafka cluster secured with ACLs,
(see `KIP-277 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-277+-+Fine+Grained+ACL+for+CreateTopics+API>`__ and
`KIP-290 <https://cwiki.apache.org/confluence/display/KAFKA/KIP-290%3A+Support+for+Prefixed+ACLs>`__ for details).
It is highly recommended to use |cp| 5.0 or above for deploying secure installations of Kafka and KSQL.

ACL definition
^^^^^^^^^^^^^^

Kafka ACLs are defined in the general format of "Principal P is [Allowed/Denied] Operation O From Host H on any Resource R matching ResourcePattern RP".

Principal
    An authenticated user or group. For example, ``"user: Fred"`` or ``"group: fraud"``.

Permission
    Defines if the ACL allows (``ALLOW``) or denies (``DENY``) access to the resource.

Operation
    The operation that is performed on the resource, for example ``READ``.

Resource
    A resource is comprised of a resource type and resource name:

    - ``RESOURCE_TYPE``, for example ``TOPIC`` or consumer ``GROUP``.
    - Resource name, e.g. the name of a topic or a consumer-group.

ResourcePattern
    A resource pattern matches zero or more Resources and is comprised of a resource type, a resource name and a pattern type.

    - ``RESOURCE_TYPE``, for example ``TOPIC`` or consumer ``GROUP``. The pattern will only match resources of the same resource type.
    - Resource name. How the pattern uses the name to match Resources is dependant on the pattern type.
    - ``PATTERN_TYPE``, controls how the pattern matches a Resource's name to the patterns. Valid values are:

        - ``LITERAL`` pattern types match the name of a resource exactly, or, in the case of the special wildcard resource name `*`, resources of any name.
        - ``PREFIXED`` pattern types match when the resource's name is prefixed with the pattern's name.

    The ``CLUSTER`` resource type is implicitly a literal pattern with a constant name because it refers to the entire Kafka cluster.

The ACLs described below list a ``RESOURCE_TYPE``, resource name, ``PATTERN_TYPE``, and ``OPERATION``.
All ACLs described are ``ALLOW`` ACLs, where the principal is the user the KSQL server has authenticated as,
with the Apache Kafka cluster, or an appropriate group that includes the authenticated KSQL user.

.. tip:: For more information about ACLs see :ref:`kafka_authorization` and for more information about interactive and
         non-interactive queries, see :ref:`restrict-ksql-interactive`.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ACLs on Literal Resource Pattern
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A literal resource pattern matches resources exactly. They are case-sensitive. e.g.
``ALLOW`` ``user Fred`` to ``READ`` the ``TOPIC`` with the ``LITERAL`` name ``users``.

Here, user Fred would be allowed to read from the topic *users* only.
Fred would not be allowed to read from similarly named topics such as *user*, *users-europe*, *Users* etc.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ACLs on Prefixed Resource Pattern
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A prefixed resource pattern matches resources where the resource name starts with the pattern's name.
They are case-sensitive. e.g.
``ALLOW`` ``user Bob`` to ``WRITE`` to any ``TOPIC`` whose name is ``PREFIXED`` with ``fraud-``.

Here, user Bob would be allowed to write to any topic whose name starts with *fraud-*, e.g. *fraud-us*, *fraud-testing* and *fraud-*.
Bob would not be allowed to write to topics such as *production-fraud-europe*, *Fraud-us*, etc.

Required ACLs
^^^^^^^^^^^^^

The ACLs required are the same for both :ref:`Interactive and non-interactive (headless) KSQL clusters <restrict-ksql-interactive>`.

KSQL always requires the following ACLs for its internal operations and data management:

- The ``DESCRIBE_CONFIGS`` operation on the ``CLUSTER`` resource type.
- The ``ALL`` operation on all internal ``TOPICS`` that are ``PREFIXED`` with ``_confluent-ksql-<ksql.service.id>``.
- The ``ALL`` operation on all internal ``GROUPS`` that are ``PREFIXED`` with ``_confluent-ksql-<ksql.service.id>``.

Where ``ksql.service.id`` can be configured in the KSQL configuration and defaults to ``default_``.

In addition to the general permissions above, KSQL also needs permissions to perform the actual processing of your data.
Here, KSQL needs permissions to read data from your desired input topics and/or permissions to write data to your desired output topics:

- The ``READ`` operation on any input topics.
- The ``WRITE`` operation on any output topics.
- The ``CREATE`` operation on any output topics that do not already exist.

Often output topics from one query form the inputs to others. KSQL will require ``READ`` and ``WRITE`` permissions for such topics.

The set of input and output topics that a KSQL cluster requires access to will depend on your use case and
whether the KSQL cluster is configured in
:ref:`interactive <config-security-ksql-acl-interactive_post_ak_2_0>` or :ref:`non-interactive <config-security-ksql-acl-headless_post_ak_2_0>` mode.

.. _config-security-ksql-acl-headless_post_ak_2_0:

Non-Interactive (headless) KSQL clusters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
:ref:`Non-interactive KSQL clusters <restrict-ksql-interactive>` run a known set of SQL statements, meaning the set
of input and output topics is well defined. Add the ACLs required to allow KSQL access to these topics.

For example, given the following setup:

- A 3-node KSQL cluster with KSQL servers running on IPs 198.51.100.0, 198.51.100.1, 198.51.100.2
- Authenticating with the Kafka cluster as a 'KSQL1' user.
- With 'ksql.service.id' set to 'production_'.
- Running queries the read from input topics 'input-topic1' and 'input-topic2'.
- Writing to output topics 'output-topic1' and 'output-topic2'.
- Where 'output-topic1' is also used as an input for another query.

Then the following commands would create the necessary ACLs in the Kafka cluster to allow KSQL to operate:

.. code:: bash

    # Allow KSQL to discover the cluster:
    bin/kafka-acls --authorizer-properties zookeeper.connect=localhost:2181 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation DescribeConfigs --cluster

    # Allow KSQL to read the input topics (including output-topic1):
    bin/kafka-acls --authorizer-properties zookeeper.connect=localhost:2181 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation Read --topic input-topic1 --topic input-topic2 --topic output-topic1

    # Allow KSQL to write to the output topics:
    bin/kafka-acls --authorizer-properties zookeeper.connect=localhost:2181 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation Write --topic output-topic1 --topic output-topic2
    # Or, if the output topics do not already exist, the 'create' operation is also required:
    bin/kafka-acls --authorizer-properties zookeeper.connect=localhost:2181 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation Create --operation Write --topic output-topic1 --topic output-topic2

    # Allow KSQL to manage its own internal topics and consumer groups:
    bin/kafka-acls --authorizer-properties zookeeper.connect=localhost:2181 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation All --resource-pattern-type prefixed --topic _confluent-ksql-production_ --group _confluent-ksql-production_

.. _config-security-ksql-acl-interactive_post_ak_2_0:

Interactive KSQL clusters
^^^^^^^^^^^^^^^^^^^^^^^^^

:ref:`Interactive KSQL clusters <restrict-ksql-interactive>` accept SQL statements from users and hence may require access
to a wide variety of input and output topics. Add ACLs to appropriate literal and prefixed resource patterns to allow KSQL
access to the input and output topics, as required.

.. tip:: To simplify ACL management, you should configure a default custom topic name prefix such as ``ksql-interactive-`` for your
         KSQL cluster via the ``ksql.output.topic.name.prefix`` :ref:`server configuration setting <set-ksql-server-properties>`.
         Unless a user defines an explicit topic name in a KSQL statement, KSQL will then always prefix the name of any automatically
         created output topics.
         Then add an ACL to allow ``ALL`` operations on ``TOPIC``s ``PREFIXED`` with the configured custom name prefix (in the example above: ``ksql-interactive-``).

For example, given the following setup:

- A 3-node KSQL cluster with KSQL servers running on IPs 198.51.100.0, 198.51.100.1, 198.51.100.2
- Authenticating with the Kafka cluster as a 'KSQL1' user.
- With 'ksql.service.id' set to 'fraud_.
- Where users should be able to run queries against any input topics prefixed with 'accounts-', 'orders-' and 'payments-'.
- Where 'ksql.output.topic.name.prefix' is set to 'ksql-fraud-'
- And users won't use explicit topic names, i.e. users will rely on KSQL auto-creating any required topics with auto-generated names.
  (Note: If users want to use explicit topic names, then you must provide the necessary ACLs for these in addition to what's shown in the example below.)

Then the following commands would create the necessary ACLs in the Kafka cluster to allow KSQL to operate:

.. code:: bash

    # Allow KSQL to discover the cluster:
    bin/kafka-acls --authorizer-properties zookeeper.connect=localhost:2181 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation DescribeConfigs --cluster

    # Allow KSQL to read the input topics:
    bin/kafka-acls --authorizer-properties zookeeper.connect=localhost:2181 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation Read --resource-pattern-type prefixed --topic accounts- --topic orders- --topic payments-

    # Allow KSQL to manage output topics:
    bin/kafka-acls --authorizer-properties zookeeper.connect=localhost:2181 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation All --resource-pattern-type prefixed --topic ksql-fraud-

    # Allow KSQL to manage its own internal topics and consumer groups:
    bin/kafka-acls --authorizer-properties zookeeper.connect=localhost:2181 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation All --resource-pattern-type prefixed --topic _confluent-ksql-fraud_ --group _confluent-ksql-fraud_

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
|cp| versions below v5.0 (Apache Kafka < v2.0)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Versions of the |cp| below v5.0, (which use Apache Kafka versions below v2.0), do not benefit from the enhancements
found in later versions of Kafka, which simplify the ACLs required to run KSQL against a Kafka cluster secured with ACLs.
This means a much larger, or wider range, set of ACLs must be defined.
The set of ACLs that must be defined depends on whether the KSQL cluster is configured for
:ref:`interactive <config-security-ksql-acl-interactive_pre_ak_2_0>` or :ref:`non-interactive (headless) <config-security-ksql-acl-headless_pre_ak_2_0>`.

ACL definition
^^^^^^^^^^^^^^

Kafka ACLs are defined in the general format of "Principal P is [Allowed/Denied] Operation O From Host H on Resource R".

Principal
    An authenticated user or group. For example, ``"user: Fred"`` or ``"group: fraud"``.

Permission
    Defines if the ACL allows (``ALLOW``) or denies (``DENY``) access to the resource.

Operation
    The operation that is performed on the resource, for example ``READ``.

Resource
    A resource is comprised of a resource type and resource name:

    - ``RESOURCE_TYPE``, for example ``TOPIC`` or consumer ``GROUP``.
    - Resource name, where the name is either specific, e.g. ``users``, or the wildcard ``*``, meaning all resources of this type. The name is case-sensitive.

    The ``CLUSTER`` resource type does not require a resource name because it refers to the entire Kafka cluster.

An example ACL might ``ALLOW`` ``user Jane`` to ``READ`` the ``TOPIC`` named ``users``.

Here, user Jane would be allowed to read from the topic *users* only.
Jane would not be allowed to read from similarly named topics such as *user*, *users-europe*, *Users* etc.

The ACLs described below list a ``RESOURCE_TYPE``, resource name, and ``OPERATION``. All ACLs described are ``ALLOW`` ACLs, where
the principal is the user the KSQL server has authenticated as, with the Apache Kafka cluster, or an appropriate group
that includes the authenticated KSQL user.

.. tip:: For more information about ACLs see :ref:`kafka_authorization` and for more information about interactive and
         non-interactive queries, see :ref:`restrict-ksql-interactive`.

.. _config-security-ksql-acl-interactive_pre_ak_2_0:

Interactive KSQL clusters
^^^^^^^^^^^^^^^^^^^^^^^^^

:ref:`Interactive KSQL clusters <restrict-ksql-interactive>`, (which is the default configuration),
require that the authenticated KSQL user has open access to create, read, write, delete topics, and use any consumer group:

Interactive KSQL clusters require these ACLs:

- The ``DESCRIBE_CONFIGS`` operation on the ``CLUSTER`` resource type.
- The ``CREATE`` operation on the ``CLUSTER`` resource type.
- The ``DESCRIBE``, ``READ``, ``WRITE`` and ``DELETE`` operations on all ``TOPIC`` resource types.
- The ``DESCRIBE`` and ``READ`` operations on all ``GROUP`` resource types.

It is still possible to restrict the authenticated KSQL user from accessing specific resources using ``DENY`` ACLs. For
example, you can add a ``DENY`` ACL to stop KSQL queries from accessing a topic that contains sensitive data.

For example, given the following setup:

- A 3-node KSQL cluster with KSQL servers running on IPs 198.51.100.0, 198.51.100.1, 198.51.100.2
- Authenticating with the Kafka cluster as a 'KSQL1' user.

Then the following commands would create the necessary ACLs in the Kafka cluster to allow KSQL to operate:

.. code:: bash

    # Allow KSQL to discover the cluster and create topics:
    bin/kafka-acls --authorizer-properties zookeeper.connect=localhost:2181 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation DescribeConfigs --operation Create --cluster

    # Allow KSQL access to topics and consumer groups:
    bin/kafka-acls --authorizer-properties zookeeper.connect=localhost:2181 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation All --topic '*' --group '*'

.. _config-security-ksql-acl-headless_pre_ak_2_0:

Non-Interactive (headless) KSQL clusters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Because the list of queries are known ahead of time, you can run
:ref:`Non-interactive KSQL clusters <restrict-ksql-interactive>`  with more restrictive ACLs.
Determining the list of ACLs currently requires a bit of effort. This will be improved in future KSQL releases.

Standard ACLs
    The authenticated KSQL user always requires:

    - ``DESCRIBE_CONFIGS`` permission on the ``CLUSTER`` resource type.

Input topics
    An input topic is one that has been imported into KSQL using a ``CREATE STREAM`` or ``CREATE TABLE``
    statement. The topic should already exist when KSQL is started.

    The authenticated KSQL user requires ``DESCRIBE`` and ``READ`` permissions for each input topic.

Output topics
    KSQL creates output topics when you run persistent ``CREATE STREAM AS SELECT`` or ``CREATE TABLE AS SELECT`` queries.

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

    All changelog and repartition topics are prefixed with ``_confluent-ksql-<ksql.service.id>`` where ``ksql.service.id`` defaults to
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
    where the default of ``ksql.service.id`` is ``default_``.

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
