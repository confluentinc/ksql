.. _ksql-security:

Securing KSQL
=============

To connect to a secured Kafka cluster, Kafka client applications must provide their security credentials. In the same way,
KSQL is configured so that the KSQL servers are authenticated and authorized, and data communication is encrypted when
communicating with the Kafka cluster. KSQL can be configured for:

- :ref:`SSL for encryption <kafka_ssl_encryption>`
- :ref:`SASL for authentication <kafka_sasl_auth>`
- :ref:`HTTPS for Confluent Schema Registry <schema_registry_http_https>`

Here are the steps to configure KSQL security:

#.  Add these configuration settings to the ``/etc/ksql/ksql-server.properties`` file.

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

#.  Start KSQL server with your configuration file specified.

    .. code:: bash

        $ <path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties

    .. tip:: After KSQL is started, you can view your settings with the ``SHOW PROPERTIES;`` KSQL command.

#.  If you are using KSQL with HTTPS to Confluent Schema Registry, you must set the ``ksql.schema.registry.url`` to HTTPS
    (as shown in the example configuration) and set the ``KSQL_OPTS`` environment variable to define the credentials to use
    when communicating with the Confluent Schema Registry:

    .. code:: bash

        # Define KSQL security credentials when communicating with the Confluent Schema Registry via HTTPS
        $ export KSQL_OPTS="-Djavax.net.ssl.trustStore=/etc/kafka/secrets/kafka.client.truststore.jks
                            -Djavax.net.ssl.trustStorePassword=confluent
                            -Djavax.net.ssl.keyStore=/etc/kafka/secrets/kafka.client.keystore.jks
                            -Djavax.net.ssl.keyStorePassword=confluent"

----------
Next Steps
----------

See the blog post `Secure Stream Processing with Apache Kafka, Confluent Platform and KSQL <https://www.confluent.io/blog/secure-stream-processing-apache-kafka-ksql/>`__
and try out the :ref:`Monitoring Kafka streaming ETL deployments <cp-demo>` tutorial.

