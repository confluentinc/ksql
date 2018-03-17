.. _ksql-security:

Securing KSQL
=============

To connect to a secured Kafka cluster, Kafka client applications must provide their security credentials. In the same way,
KSQL can be configured so that the KSQL servers are authenticated and authorized, and data communication is encrypted when
communicating with the Kafka cluster. KSQL can be configured for:

- :ref:`SSL for encryption <kafka_ssl_encryption>`
- :ref:`SASL for authentication <kafka_sasl_auth>`
- :ref:`HTTPS for Confluent Schema Registry <schema_registry_http_https>`

-----------------------------
Configuring Security for KSQL
-----------------------------

Here are the general steps to configure KSQL security:

#.  Add your configuration settings to the ``/etc/ksql/ksql-server.properties`` file.

    .. code:: bash

        # General KSQL configuration
        bootstrap.servers=localhost:9092
        listeners=http://localhost:8080

        # Global SSL and SASL configuration for KSQL
        ssl.truststore.location=/etc/kafka/secrets/kafka.client.truststore.jks
        ssl.truststore.password=confluent
        ssl.keystore.location=/etc/kafka/secrets/kafka.client.keystore.jks
        ssl.keystore.password=confluent
        ssl.key.password=confluent
        ssl.endpoint.identification.algorithm=HTTPS
        security.protocol=SASL_SSL
        sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="client" password="client-secret";
        sasl.mechanism=PLAIN

        # Schema Registry using HTTPS
        ksql.schema.registry.url=https://schemaregistry:8082

        # Producer Confluent Monitoring Interceptors for Control Center streams monitoring
        producer.interceptor.classes=io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor
        producer.confluent.monitoring.interceptor.ssl.truststore.location=/etc/kafka/secrets/kafka.client.truststore.jks
        producer.confluent.monitoring.interceptor.ssl.truststore.password=confluent
        producer.confluent.monitoring.interceptor.ssl.keystore.location=/etc/kafka/secrets/kafka.client.keystore.jks
        producer.confluent.monitoring.interceptor.ssl.keystore.password=confluent
        producer.confluent.monitoring.interceptor.ssl.key.password=confluent
        producer.confluent.monitoring.interceptor.security.protocol=SASL_SSL
        producer.confluent.monitoring.interceptor.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="client" password="client-secret";
        producer.confluent.monitoring.interceptor.sasl.mechanism=PLAIN

        # Consumer Confluent Monitoring Interceptors for Control Center streams monitoring
        consumer.interceptor.classes=io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor
        consumer.confluent.monitoring.interceptor.ssl.truststore.location=/etc/kafka/secrets/kafka.client.truststore.jks
        consumer.confluent.monitoring.interceptor.ssl.truststore.password=confluent
        consumer.confluent.monitoring.interceptor.ssl.keystore.location=/etc/kafka/secrets/kafka.client.keystore.jks
        consumer.confluent.monitoring.interceptor.ssl.keystore.password=confluent
        consumer.confluent.monitoring.interceptor.ssl.key.password=confluent
        consumer.confluent.monitoring.interceptor.security.protocol=SASL_SSL
        consumer.confluent.monitoring.interceptor.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="client" password="client-secret";
        consumer.confluent.monitoring.interceptor.sasl.mechanism=PLAIN

    The exact settings you need depend on what SASL mechanism your Kafka cluster is using and how your SSL certificates
    are signed. For full details, see the `Security section of the Kafka documentation <http://kafka.apache.org/documentation.html#security>`__.

#.  Start KSQL server with your configuration file specified.

    .. code:: bash

        $ <path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties

    .. tip:: After KSQL is started, you can view your settings with the ``SHOW PROPERTIES;`` KSQL command.

---------------------------------------------------
Using KSQL with a secured Confluent Schema Registry
---------------------------------------------------

If you are using KSQL with HTTPS to Confluent Schema Registry, you must set the ``ksql.schema.registry.url`` to HTTPS
(as shown in the example configuration) and set the ``KSQL_OPTS`` environment variable to define the credentials to use
when communicating with the Confluent Schema Registry:

.. code:: bash

    # Define KSQL security credentials when communicating with the Confluent Schema Registry via HTTPS
    $ export KSQL_OPTS="-Djavax.net.ssl.trustStore=/etc/kafka/secrets/kafka.client.truststore.jks
                        -Djavax.net.ssl.trustStorePassword=confluent
                        -Djavax.net.ssl.keyStore=/etc/kafka/secrets/kafka.client.keystore.jks
                        -Djavax.net.ssl.keyStorePassword=confluent"

-------------------------------------------------
Using KSQL with a Kafka Cluster Secured with ACLs
-------------------------------------------------

You can use KSQL with Apache Kafka clusters that are secured with ACLs. The behavior depends on whether the cluster is
interactive or non-interactive.

.. tip:: For more information about ACLs see :ref:`kafka_authorization` and for more information about interactive and
         non-interactive queries, see :ref:`streams_developer-guide_interactive-queries`.

^^^^^^^^^^^^^^^^^^^^^^^^^
Interactive KSQL clusters
^^^^^^^^^^^^^^^^^^^^^^^^^

Interactive KSQL clusters require that the KSQL user has open access to create, read, write and delete topics and
use any consumer group.

The required ACLs are:

- *DESCRIBE_CONFIGS* permission on the *CLUSTER*.
- *CREATE* permission on the *CLUSTER*.
- *DESCRIBE*, *READ*, *WRITE* and *DELETE* permissions on the *<any>* *TOPIC*.
- *DESCRIBE* and *READ* permissions  on the *<any>* *GROUP*.

It is still possible to restrict the KSQL user from accessing specific resources using *DENY* ACLs. For example, you can add a
*DENY* ACL to stop KSQL queries from accessing a topic that contains sensitive data.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Non-Interactive KSQL clusters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Non-interactive KSQL clusters run with much more restrictive ACLs, though it currently requires a bit of effort to
determine what ACLs are required. This will be improved in future KSQL releases.

Standard ACLs
    The KSQL user always requires:

    - *DESCRIBE_CONFIGS* permission on the *CLUSTER*.
    - *DESCRIBE* permission on the *__consumer_offsets* topic.

    If you want KSQL to create internal and sink topics then the KSQL user should also be granted:

    - *CREATE* permission on the *CLUSTER*.

Source topics
    KSQL users require *DESCRIBE* and *READ* permissions for each source and input topic. The topic should already exist
    when KSQL is started.

Sink topics
    KSQL users require *DESCRIBE* and *WRITE* permissions ror each sink and output topic. If the topic does not already
    exist, the user also requires *CREATE* permissions on the *CLUSTER*.

Change-log and repartition topics
    The set of changelog and repartitioning topics that KSQL requires depends on the queries being executed. The easiest
    way to determine the list of required topics is to first run the queries on an unsecured Kafka cluster and list the topics
    that are created.

    All changelog and repartition topics are prefixed with  ``<value of ksql.service.id property>_query_<query id>_`` where
    the default of ``ksql.service.id`` is ``ksql_``.

    KSQL users require a minimum of *DESCRIBE*, *READ* and *WRITE* permissions for each changelog and repartition *TOPIC*.

    If the KSQL user does not have *CREATE* permissions on the *CLUSTER*, then all changelog and repartition topics must
    already exist, with the same number of partitions as the source topic, and ``replication.factor`` replicas.

Consumer groups
    The set of consumer groups that KSQL requires depends on the queries that are being executed. The easiest way to
    determine the list of consumer groups is to first run the queries on an open Kafka cluster and list the groups created.

    Consumer group names are formatted like ``<value of ksql.service.id property>_query_<query id>``, where the default
    of ``ksql.service.id`` is ``ksql_``.

    KSQL users require a minimum of *DESCRIBE* and *READ* permissions for *GROUP*.


^^^^^^^^^^
Learn More
^^^^^^^^^^

See the blog post `Secure Stream Processing with Apache Kafka, Confluent Platform and KSQL <https://www.confluent.io/blog/secure-stream-processing-apache-kafka-ksql/>`__
and try out the :ref:`Monitoring Kafka streaming ETL deployments <cp-demo>` tutorial.

