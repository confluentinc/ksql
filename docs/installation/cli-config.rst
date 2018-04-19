.. _install_cli-config:

Configuring KSQL CLI
====================

You can connect the KSQL CLI to one KSQL server per cluster.

.. important:: There is no automatic failover of your CLI session to another KSQL server if the original server that the
               CLI is connected to becomes unavailable. Any persistent queries you executed will continue to run in the
               KSQL cluster.

To connect the KSQL CLI to a cluster, run this command with your KSQL server URL specified (default is ``http://localhost:8088``):

.. code:: bash

    $ <path-to-confluent>/bin/ksql <ksql-server-URL>

Configuring Per-session Properties
----------------------------------

You can set the properties by using the KSQL CLI startup script argument ``/bin/ksql <server> --config-file <path/to/file>``
or by using the SET statement from within the KSQL CLI session. For more information, see :ref:`install_ksql-cli`.

Here are some common KSQL CLI properties that you can customize:

- :ref:`ksql.streams.auto.offset.reset <ksql-auto-offset-reset>`
- :ref:`ksql.streams.cache.max.bytes.buffering <streams_developer-guide_optional-configs>`
- :ref:`ksql.streams.num.stream.threads <streams_developer-guide_optional-configs>`
- :ref:`ksql.sink.partitions <ksql-sink-partitions>`
- :ref:`ksql.sink.replicas <ksql-sink-replicas>`



