.. _tuning_ksql:

When deploying KSQL to production, the following settings are recommended in your ``/etc/ksql/ksql-server.properties`` file:

.. code:: bash

    # Ensures that transient failures will not result in data loss.
    producer.retries=2147483647

    # Ensures that queries will not terminate if the underlying Kafka cluster is unavailable for a period of time.
    producer.confluent.batch.expiry.ms=9223372036854775807

    # Allows more frequent retries of requests when there are failures, enabling quicker recovery.
    producer.request.timeout.ms=300000

    # If allows KSQL to pause processing if the underlying kafka cluster is unavailable.
    producer.max.block.ms=9223372036854775807

