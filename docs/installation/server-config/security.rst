.. _ksql-security:

Configuring Security for KSQL
=============================

KSQL supports authentication on its HTTP endpoints and also supports many of the security features
of the other services it communicates with, like |ak-tm| and |sr|.

- KSQL supports Basic HTTP authentication on its RESTful and WebSocket endpoints, which means
  that the endpoints can be protected by a username and password.
- KSQL supports Apache Kafka security features such as :ref:`SSL for encryption <kafka_ssl_encryption>`,
  :ref:`SASL for authentication <kafka_sasl_auth>`, and :ref:`authorization with ACLs <kafka_authorization>`.
- KSQL supports :ref:`Schema Registry security features <schemaregistry_security>` such SSL for encryption
  and mutual authentication for authorization.
- Starting in |cp| 5.2, KSQL supports SSL on all network traffic.

To configure security for KSQL, add your configuration settings to the ``<path-to-confluent>/etc/ksql/ksql-server.properties``
file and then :ref:`start the KSQL server <start_ksql-server>` with your configuration file specified.

.. code:: bash

    $ <path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties

.. include:: ../../../../includes/installation-types-zip-tar.rst

.. _config-ksql-for-https:

Configuring KSQL for HTTPS
--------------------------
KSQL can be configured to use HTTPS rather than the default HTTP for all communication.

If you haven't already, you will need to :ref:`create SSL key and trust stores <generating-keys-certs>`.

Use the following settings to configure the KSQL server to use HTTPS:

::

    listeners=https://hostname:port
    ssl.keystore.location=/var/private/ssl/ksql.server.keystore.jks
    ssl.keystore.password=xxxx
    ssl.key.password=yyyy

Note the use of the HTTPS protocol in the ``listeners`` config.

To enable the server to authenticate clients (2-way authentication), use the following additional
settings:

::

    ssl.client.auth=true
    ssl.truststore.location=/var/private/ssl/ksql.server.truststore.jks
    ssl.truststore.password=zzzz

Additional settings are available for configuring KSQL for HTTPS. For example,
if you need to restrict the default configuration for
`Jetty <https://www.eclipse.org/jetty/>`__, there are settings like
``ssl.enabled.protocols``. For more information, see :ref:`kafka-rest-https-config`.

.. _configuring-cli-for-https:

-----------------------------
Configuring the CLI for HTTPS
-----------------------------
If the KSQL server is configured to use HTTPS, CLI instances may need to be configured with
suitable key and trust stores.

If the server's SSL certificate is not signed by a recognised public Certificate Authority,
the CLI will need to be configured with a trust store that trusts the servers SSL certificate.

If you haven't already, you will need to :ref:`create SSL key and trust stores <generating-keys-certs>`.

Use the following settings to configure the CLI server:

::

    ssl.truststore.location=/var/private/ssl/ksql.client.truststore.jks
    ssl.truststore.password=zzzz

If the server is performing client authentication (2-way authentication), use the following
additional settings:

::

    ssl.keystore.location=/var/private/ssl/ksql.client.keystore.jks
    ssl.keystore.password=xxxx
    ssl.key.password=yyyy

Settings for the CLI can be stored in a suitable file and passed to the CLI via the ``--config-file``
command-line arguments, for example:

.. code:: bash

    <ksql-install>bin/ksql --config-file ./config/ksql-cli.properties https://localhost:8088

Configuring KSQL for Basic HTTP Authentication
----------------------------------------------
KSQL can be configured to require users to authenticate using a username and password via the Basic
HTTP authentication mechanism.

.. note:: If you're using Basic authentication, we recommended that you
          :ref:`configure KSQL to use HTTPS for secure communication <config-ksql-for-https>`,
          because the Basic protocol passes credentials in plain text.

Use the following settings to configure the KSQL server to require authentication:

::

    authentication.method=BASIC
    authentication.roles=<user-role1>,<user-role2>,...
    authentication.realm=<KsqlServer-Props-in-jaas_config.file>

The ``authentication.roles`` config defines a comma-separated list of user roles. To be authorized
to use the KSQL server, an authenticated user must belong to at least one of these roles.

For example, if you define ``admin``, ``developer``, ``user``, and ``ksq-user``
roles, the following configuration assigns them for authentication.

::

    authentication.roles=admin,developer,user,ksq-user

The ``authentication.realm`` config must match a section within ``jaas_config.file``, which
defines how the server authenticates users and should be passed as a JVM option during server start:

.. code:: bash

    $ export KSQL_OPTS=-Djava.security.auth.login.config=/path/to/the/jaas_config.file
    $ <path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties

An example ``jaas_config.file`` is:

::

    KsqlServer-Props {
      org.eclipse.jetty.jaas.spi.PropertyFileLoginModule required
      file="/path/to/password-file"
      debug="false";
    };

The example ``jaas_config.file`` above uses the Jetty ``PropertyFileLoginModule``, which itself
authenticates users by checking for their credentials in a password file.

Assign the ``KsqlServer-Props`` section to the ``authentication.realm`` config setting:

::

    authentication.realm=KsqlServer-Props


You can also use other implementations of the standard Java ``LoginModule`` interface, such as
``JDBCLoginModule`` for reading credentials from a database or the ``LdapLoginModule``.

The file parameter is the location of the password file, The format is:

::

    <username>: <password-hash>[,<rolename> ...]

Here’s an example:

::

    fred: OBF:1w8t1tvf1w261w8v1w1c1tvn1w8x,user,admin
    harry: changeme,user,developer
    tom: MD5:164c88b302622e17050af52c89945d44,user
    dick: CRYPT:adpexzg3FUZAk,admin,ksq-user

The password hash for a user can be obtained by using the ``org.eclipse.jetty.util.security.Password``
utility, for example running:

.. code:: bash

    > bin/ksql-run-class org.eclipse.jetty.util.security.Password fred letmein

Which results in an output similar to:

::

    letmein
    OBF:1w8t1tvf1w261w8v1w1c1tvn1w8x
    MD5:0d107d09f5bbe40cade3de5c71e9e9b7
    CRYPT:frd5btY/mvXo6

Where each line of the output is the password encrypted using different mechanisms, starting with
plain text.

.. _basic-ksql-http:

-------------------------------------------------
Configuring the CLI for Basic HTTP Authentication
-------------------------------------------------
If the KSQL server is configured to use Basic authentication, CLI instances will need to be
configured with suitable valid credentials.  Credentials can be passed when starting the CLI using
the ``--user`` and ``--password`` command-line arguments, for example:

.. code:: bash

    <ksql-install>bin/ksql --user fred --password letmein http://localhost:8088

Configuring KSQL for |ccloud|
-----------------------------

You can use KSQL with a Kafka cluster in |ccloud|. For more information, see :ref:`install_ksql-ccloud`.

Configuring KSQL for |c3|
-----------------------------

You can use KSQL with a Kafka cluster in |c3|. For more information, see
:ref:`integrate-ksql-with-confluent-control-center`.

.. _config-security-ksql-sr:

Configuring KSQL for Secured |sr-long|
--------------------------------------

You can configure KSQL to connect to |sr| over HTTP by setting the
``ksql.schema.registry.url`` to the HTTPS endpoint of |sr|.
Depending on your security setup, you might also need to supply additional SSL configuration.
For example, a trustStore is required if the |sr| SSL certificates are not trusted by
the JVM by default; a keyStore is required if |sr| requires mutual authentication.

You can configure SSL for communication with |sr| by using non-prefixed names,
like ``ssl.truststore.location``, or prefixed names like ``ksql.schema.registry.ssl.truststore.location``.
Non-prefixed names are used for settings that are shared with other communication
channels, i.e. where the same settings are required to configure SSL communication
with both Kafka and |sr|. Prefixed names only affect communication with |sr|
and override any non-prefixed setting of the same name.

Use the following to configure KSQL to communicate with |sr| over HTTPS,
where mutual authentication is not required and |sr| SSL certificates are trusted
by the JVM:

::

    ksql.schema.registry.url=https://<host-name-of-schema-registry>:<ssl-port>

Use the following to configure KSQL to communicate with |sr| over HTTPS, with
mutual authentication, with an explicit trustStore, and where the SSL configuration is shared
between Kafka and |sr|:

::

    ksql.schema.registry.url=https://<host-name-of-schema-registry>:<ssl-port>
    ksql.schema.registry.ssl.truststore.location=/etc/kafka/secrets/ksql.truststore.jks
    ksql.schema.registry.ssl.truststore.password=<your-secure-password>
    ksql.schema.registry.ssl.keystore.location=/etc/kafka/secrets/ksql.keystore.jks
    ksql.schema.registry.ssl.keystore.password=<your-secure-password>
    ksql.schema.registry.ssl.key.password=<your-secure-password>

Use the following to configure KSQL to communicate with |sr| over HTTP, without
mutual authentication and with an explicit trustStore. These settings explicitly configure only
KSQL to |sr| SSL communication.

::

    ksql.schema.registry.url=https://<host-name-of-schema-registry>:<ssl-port>
    ksql.schema.registry.ssl.truststore.location=/etc/kafka/secrets/sr.truststore.jks
    ksql.schema.registry.ssl.truststore.password=<your-secure-password>

The exact settings will vary depending on the encryption and authentication mechanisms 
|sr| is using, and how your SSL certificates are signed.

You can pass authentication settings to the |sr| client used by KSQL
by adding the following to your KSQL server config.

::

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

::

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

::

    security.protocol=SASL_SSL
    sasl.mechanism=PLAIN
    sasl.jaas.config=\
        org.apache.kafka.common.security.plain.PlainLoginModule required \
        username="<ksql-user>" \
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
    - Resource name, for example the name of a topic or a consumer-group.

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

.. tip:: For more information about ACLs, see :ref:`kafka_authorization`.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ACLs on Literal Resource Pattern
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A literal resource pattern matches resources exactly. They are case-sensitive. For example
``ALLOW`` ``user Fred`` to ``READ`` the ``TOPIC`` with the ``LITERAL`` name ``users``.

Here, user Fred would be allowed to read from the topic *users* only.
Fred would not be allowed to read from similarly named topics such as *user*, *users-europe*, *Users* etc.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ACLs on Prefixed Resource Pattern
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A prefixed resource pattern matches resources where the resource name starts with the pattern's name.
They are case-sensitive. For example
``ALLOW`` ``user Bob`` to ``WRITE`` to any ``TOPIC`` whose name is ``PREFIXED`` with ``fraud-``.

Here, user Bob would be allowed to write to any topic whose name starts with *fraud-*, for example
*fraud-us*, *fraud-testing* and *fraud-*.
Bob would not be allowed to write to topics such as *production-fraud-europe*, *Fraud-us*, etc.

Required ACLs
^^^^^^^^^^^^^

The ACLs required are the same for both :ref:`Interactive and non-interactive (headless) KSQL clusters <restrict-ksql-interactive>`.

KSQL always requires the following ACLs for its internal operations and data management:

- The ``DESCRIBE_CONFIGS`` operation on the ``CLUSTER`` resource type.
- The ``ALL`` operation on all internal ``TOPICS`` that are ``PREFIXED`` with ``_confluent-ksql-<ksql.service.id>``.
- The ``ALL`` operation on all internal ``GROUPS`` that are ``PREFIXED`` with ``_confluent-ksql-<ksql.service.id>``.

Where ``ksql.service.id`` can be configured in the KSQL configuration and defaults to ``default_``.

If KSQL is configured to create a topic for the :ref:`record processing log <ksql_processing_log>`
which is the default configuration since KSQL version 5.2, the following ACLs are also needed:

- The ``ALL`` operation on the ``TOPIC`` with ``LITERAL`` name ``<ksql.logging.processing.topic.name>``.

Where ``ksql.logging.processing.topic.name`` can be configured in the KSQL configuration and defaults to ``<ksql.service.id>ksql_processing_log``.

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
- Authenticating with the Kafka cluster as a ``KSQL1`` user.
- With ``ksql.service.id`` set to ``production_``.
- Running queries that read from input topics ``input-topic1`` and ``input-topic2``.
- Writing to output topics ``output-topic1`` and ``output-topic2``.
- Where ``output-topic1`` is also used as an input for another query.

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

    # Allow KSQL to manage its record processing log topic, if configured:
    bin/kafka-acls --authorizer-properties zookeeper.connect=localhost:2181 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation All --topic production_ksql_processing_log

.. _config-security-ksql-acl-interactive_post_ak_2_0:

Interactive KSQL clusters
^^^^^^^^^^^^^^^^^^^^^^^^^

:ref:`Interactive KSQL clusters <restrict-ksql-interactive>` accept SQL statements from users and hence may require access
to a wide variety of input and output topics. Add ACLs to appropriate literal and prefixed resource patterns to allow KSQL
access to the input and output topics, as required.

.. tip:: To simplify ACL management, you should configure a default custom topic name prefix such as ``ksql-interactive-`` for your
         KSQL cluster via the ``ksql.output.topic.name.prefix`` :ref:`server configuration setting <set-ksql-server-properties>`.
         Unless a user defines an explicit topic name in a KSQL statement, KSQL will then always prefix the name of any automatically
         created output topics. Then add an ACL to allow ``ALL`` operations on ``TOPICs`` that are ``PREFIXED`` with the configured
         custom name prefix (in the example above: ``ksql-interactive-``).

For example, given the following setup:

- A 3-node KSQL cluster with KSQL servers running on IPs 198.51.100.0, 198.51.100.1, 198.51.100.2
- Authenticating with the Kafka cluster as a ``KSQL1`` user.
- With ``ksql.service.id`` set to ``fraud_``.
- Where users should be able to run queries against any input topics prefixed with ``accounts-``, ``orders-`` and ``payments-``.
- Where ``ksql.output.topic.name.prefix`` is set to ``ksql-fraud-``
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

    # Allow KSQL to manage its record processing log topic, if configured:
    bin/kafka-acls --authorizer-properties zookeeper.connect=localhost:2181 --add --allow-principal User:KSQL1 --allow-host 198.51.100.0 --allow-host 198.51.100.1 --allow-host 198.51.100.2 --operation All --topic fraud_ksql_processing_log

The following table shows the necessary ACLs in the Kafka cluster to allow
KSQL to operate in interactive mode.

========== ==================== ========= ==================================== =========
Permission Operation            Resource  Name                                 Type
========== ==================== ========= ==================================== =========
ALLOW      DESCRIBE             CLUSTER   kafka-cluster                        LITERAL
ALLOW      DESCRIBE_CONFIGS     CLUSTER   kafka-cluster                        LITERAL
ALLOW      CREATE               TOPIC     <ksql-service-id>                    PREFIXED
ALLOW      CREATE               TOPIC     _confluent-ksql-<ksql-service-id>    PREFIXED
ALLOW      CREATE               GROUP     _confluent-ksql-<ksql-service-id>    PREFIXED
ALLOW      DESCRIBE             TOPIC     <ksql-service-id>                    PREFIXED
ALLOW      DESCRIBE             TOPIC     _confluent-ksql-<ksql-service-id>    PREFIXED
ALLOW      DESCRIBE             GROUP     _confluent-ksql-<ksql-service-id>    PREFIXED
ALLOW      ALTER                TOPIC     <ksql-service-id>                    PREFIXED
ALLOW      ALTER                TOPIC     _confluent-ksql-<ksql-service-id>    PREFIXED
ALLOW      ALTER                GROUP     _confluent-ksql-<ksql-service-id>    PREFIXED
ALLOW      DESCRIBE_CONFIGS     TOPIC     <ksql-service-id>                    PREFIXED
ALLOW      DESCRIBE_CONFIGS     TOPIC     _confluent-ksql-<ksql-service-id>    PREFIXED
ALLOW      DESCRIBE_CONFIGS     GROUP     _confluent-ksql-<ksql-service-id>    PREFIXED
ALLOW      ALTER_CONFIGS        TOPIC     <ksql-service-id>                    PREFIXED
ALLOW      ALTER_CONFIGS        TOPIC     _confluent-ksql-<ksql-service-id>    PREFIXED
ALLOW      ALTER_CONFIGS        GROUP     _confluent-ksql-<ksql-service-id>    PREFIXED
ALLOW      READ                 TOPIC     <ksql-service-id>                    PREFIXED
ALLOW      READ                 TOPIC     _confluent-ksql-<ksql-service-id>    PREFIXED
ALLOW      READ                 GROUP     _confluent-ksql-<ksql-service-id>    PREFIXED
ALLOW      WRITE                TOPIC     <ksql-service-id>                    PREFIXED
ALLOW      WRITE                TOPIC     _confluent-ksql-<ksql-service-id>    PREFIXED
ALLOW      WRITE                GROUP     _confluent-ksql-<ksql-service-id>    PREFIXED
ALLOW      DELETE               TOPIC     <ksql-service-id>                    PREFIXED
ALLOW      DELETE               TOPIC     _confluent-ksql-<ksql-service-id>    PREFIXED
ALLOW      DELETE               GROUP     _confluent-ksql-<ksql-service-id>    PREFIXED
ALLOW      DESCRIBE             TOPIC     ``*``                                LITERAL
ALLOW      DESCRIBE             GROUP     ``*``                                LITERAL
ALLOW      DESCRIBE_CONFIGS     TOPIC     ``*``                                LITERAL
ALLOW      DESCRIBE_CONFIGS     GROUP     ``*``                                LITERAL
========== ==================== ========= ==================================== =========

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
    - Resource name, where the name is either specific, for example ``users``, or the wildcard ``*``, meaning all resources of this type. The name is case-sensitive.

    The ``CLUSTER`` resource type does not require a resource name because it refers to the entire Kafka cluster.

An example ACL might ``ALLOW`` ``user Jane`` to ``READ`` the ``TOPIC`` named ``users``.

Here, user Jane would be allowed to read from the topic *users* only.
Jane would not be allowed to read from similarly named topics such as *user*, *users-europe*, *Users* etc.

The ACLs described below list a ``RESOURCE_TYPE``, resource name, and ``OPERATION``. All ACLs described are ``ALLOW`` ACLs, where
the principal is the user the KSQL server has authenticated as, with the Apache Kafka cluster, or an appropriate group
that includes the authenticated KSQL user.

.. tip:: For more information about ACLs, see :ref:`kafka_authorization`.

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

.. tip:: For more information about interactive and non-interactive queries, see :ref:`restrict-ksql-interactive`.

----------------------------------------------
Configuring |c3-short| Monitoring Interceptors
----------------------------------------------

This configuration enables SASL and SSL for the :ref:`monitoring interceptors <controlcenter_clients>` that integrate KSQL
with |c3-short|.

::

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
