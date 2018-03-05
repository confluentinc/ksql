.. _ksql_operations:

KSQL Operations
===============


=============================
Capacity planning and scaling
=============================

TBD

======================
Command Line Utilities
======================

These utilities are located in ``<path-to-confluent/bin``.

ksql-print-metrics
ksql-datagen
ksql-node


============
Healthchecks
============

- The REST API supports a “server info” request at `http://<server>:8080/info <http://<server>:8080/info>`_.
- Check runtime stats for the KSQL server that you are connected to:

    - Run ``ksql-print-metrics`` on a server. For example, see this `blog post <https://www.confluent.io/blog/ksql-january-release-streaming-sql-apache-kafka/>`_.


=======
Logging
=======

By default KSQL server logs are written to ``/tmp/ksql-logs/``. 

- ``ksql.log`` -- Contains REST API log output (i.e. Jetty, parsing queries, problems with malformed data).
- ``ksql-streams.log`` -- Contains KStreams logging output for running queries.


======================
Monitoring and Metrics
======================

KSQL includes JMX (Java Management Extensions) metrics which give insights into what is happening inside your KSQL servers.
These metrics include the number of messages, the total throughput, throughput distribution, error rate, and more.

The ``ksql-print-metrics`` command line utility collects these metrics and prints them to the console. You can invoke this
utility from your terminal:

.. code:: bash

    $ <path-to-confluent>/bin/ksql-print-metrics

Your output should resemble:

.. code:: bash

    messages-consumed-avg: 96416.96196183885
    messages-consumed-min: 88900.3329377909
    error-rate: 0.0
    num-persistent-queries: 2.0
    messages-consumed-per-sec: 193024.78294586178
    messages-produced-per-sec: 193025.4730374501
    num-active-queries: 2.0
    num-idle-queries: 0.0
    messages-consumed-max: 103397.81191436431

===============
Troubleshooting
===============

TBD
