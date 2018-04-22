.. _ksql_quickstart-local:

Writing Streaming Queries Against Kafka Using KSQL (Local)
==========================================================

This tutorial demonstrates a simple workflow using KSQL to write streaming queries against messages in Kafka.

To get started, you must start a Kafka cluster, including |zk| and a Kafka broker. KSQL will then query messages from
this Kafka cluster. KSQL is installed in the |cp| by default.

**Prerequisites:**

- :ref:`Confluent Platform <installation>` is installed and running. This installation includes a Kafka broker, KSQL, |c3-short|,
  |zk|, Schema Registry, REST Proxy, and Kafka Connect.
- If you installed |cp| via TAR or ZIP, navigate into the installation directory. The paths and commands used throughout
  this tutorial assume that your are in this installation directory.
- Java: Minimum version 1.8. Install Oracle Java JRE or JDK >= 1.8 on your local machine

.. include:: ../includes/ksql-includes.rst
      :start-line: 42
      :end-line: 76

.. include:: ../includes/ksql-includes.rst
      :start-line: 338
      :end-line: 349

.. include:: ../includes/ksql-includes.rst
      :start-line: 76
      :end-line: 82

.. include:: ../includes/ksql-includes.rst
      :start-line: 82
      :end-line: 327

Confluent Platform
------------------

If you are running the |cp|, you can stop it with this
command.

.. code:: bash

    $ <path-to-confluent>/bin/confluent stop

