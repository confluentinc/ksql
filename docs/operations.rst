.. _ksql_operations:

KSQL Operations
===============

================================================
Local Development and Testing with Confluent CLI
================================================

For development and testing purposes, you can use Confluent CLI to spin up services on a single host. For more information,
see the :ref:`quickstart`.

.. include:: ../../includes/installation.rst
    :start-line: 34
    :end-line: 37

===================================
Starting and Stopping KSQL Clusters
===================================

TBD

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

For more information about Kafka Streams metrics, see :ref:`streams_monitoring`.

=============================
Capacity Planning and Scaling
=============================

TBD

===============
Troubleshooting
===============

------------------------------------
SELECT query hangs and doesn’t stop?
------------------------------------
This is a continuous streaming query so it will not stop unless you type ``Ctrl + C``.

--------------------------------------------------
No results from ``SELECT * FROM`` table or stream?
--------------------------------------------------
This is caused by the offset being set to ‘latest’ by default, and no new records are received. To fix, do one of the
following:

- Run this command: ``SET ‘auto.offset.reset’ = ‘earliest’;``. For more information, see :ref:`common-configs`.
- Write new records to the input topics.

-----------------------------------------------------------
Can’t create a stream from the output of windowed aggregate
-----------------------------------------------------------
The output of a windowed aggregate is a record per grouping key and per window, and is not a single record. This is not
currently supported in KSQL.

-----------------------------------------
KSQL doesn’t clean up its internal topics
-----------------------------------------
Make sure that your Kafka cluster is configured with ``delete.topic.enable=true``. For more information, see :cp-javadoc:`deleteTopics|clients/javadocs/org/apache/kafka/clients/admin/AdminClient.html`.





