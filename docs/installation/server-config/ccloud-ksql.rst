.. _install_ksql-ccloud:

Using KSQL with |ccloud|
========================

You can connect KSQL to your |cp| Kafka cluster in |ccloud|.

The KSQL servers must be configured to use |ccloud|. The KSQL CLI does not require configuration.

**Prerequisites**

- :ref:`Confluent Platform <installing_cp>`

#.  Customize your ``/etc/ksql/ksql-server.properties`` properties file.

    .. code:: bash

        # a comma separated list of the the ccloud broker endpoints.
        # eg. r0.great-app.confluent.aws.prod.cloud:9092,r1.great-app.confluent.aws.prod.cloud:9093,r2.great-app.confluent.aws.prod.cloud:9094
        bootstrap.servers=<broker-endpoint1, broker-endpoint2, broker-endpoint3>
        #
        ksql.sink.replicas=3
        #
        replication.factor=3
        #
        security.protocol=SASL_SSL
        #
        sasl.mechanism=PLAIN
        #
        sasl.jaas.config=\
            org.apache.kafka.common.security.plain.PlainLoginModule required \
            username="<confluent cloud access key>" \
            password="<confluent cloud secret>";
        # Recommended performance settings
        producer.retries=2147483647
        producer.confluent.batch.expiry.ms=9223372036854775807
        producer.request.timeout.ms=300000
        producer.max.block.ms=9223372036854775807

#.  Restart the KSQL server. The steps do this are dependent on your environment.

    - If you are using the Confluent CLI, see :ref:`cli-command-reference`.
    - If you are running |cp| in a production environment, use ``ksql-server-stop && ksql-server-start``.


For more information, see :ref:`cloud-quickstart`.
