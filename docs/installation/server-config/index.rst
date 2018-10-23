.. _ksql-server-config:

Configuring KSQL Server
=======================

.. toctree:: 
    :maxdepth: 1

    security
    config-reference
    avro-schema

KSQL configuration parameters can be set for KSQL server and queries as well as for the underlying Kafka Streams and
Kafka Clients (producer and consumer).

.. include:: ../../../../includes/installation-types-zip-tar.rst

.. _set-ksql-server-properties:

------------------------------
Setting KSQL Server Parameters
------------------------------

You can specify KSQL server configuration parameters by using the server configuration file (``ksql-server.properties``)
or the ``KSQL_OPTS`` environment variable. Properties set with ``KSQL_OPTS`` take precedence over those specified in the
KSQL configuration file. A recommended approach is to configure a common set of properties using the KSQL configuration
file and override specific properties as needed, using the ``KSQL_OPTS`` environment variable.

KSQL Server Configuration File
------------------------------

By default the KSQL server configuration file is located at ``<path-to-confluent>/etc/ksql/ksql-server.properties``.
The file follows the syntax conventions of
`Java properties files <https://docs.oracle.com/javase/tutorial/essential/environment/properties.html>`__.

.. code:: bash

    <property-name>=<property-value>

For example:

.. code:: bash

    bootstrap.servers=localhost:9092
    listeners=http://localhost:8088

After you have updated the server configuration file, you can start the KSQL server with the configuration file
specified.

.. code:: bash

    $ <path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties

For more information, see :ref:`ksql-param-reference`.

KSQL_OPTS Environment Variable
------------------------------

You can override KSQL server configuration parameters by using the ``KSQL_OPTS`` environment variable. The properties are
standard Java system properties. For example, to set ``ksql.streams.num.streams.threads`` to ``1``:

.. code:: bash

    $ KSQL_OPTS="-Dksql.streams.num.streams.threads=1" <path-to-confluent>/bin/ksql-server-start \
      <path-to-confluent>/etc/ksql/ksql-server.properties

You can specify multiple parameters at the same time. For example, to configure ``ksql.streams.auto.offset.reset`` and ``ksql.streams.num.stream.threads``:

.. code:: bash

    $ KSQL_OPTS="-Dksql.streams.auto.offset.reset=earliest -Dksql.streams.num.stream.threads=1" <path-to-confluent>/bin/ksql-server-start \
      <path-to-confluent>/etc/ksql/ksql-server.properties

-----------
JMX Metrics
-----------

.. include:: ../../includes/ksql-includes.rst
    :start-after: enable_JMX_metrics_start
    :end-before: enable_JMX_metrics_end

.. _restrict-ksql-interactive:

-------------------------------------
Non-interactive (Headless) KSQL Usage
-------------------------------------

KSQL supports locked-down, "headless" deployment scenarios where interactive use of the KSQL cluster is disabled.
For example, to allow a team of users to develop and verify their queries interactively on a shared testing
KSQL cluster. But when deploying those queries in your production environment, you want to lock-down access to KSQL
servers, version-control the exact queries, and store them in a .sql file. This will prevent users from interacting
directly with the production KSQL cluster.

You can configure servers to exclusively run a predefined script (``.sql`` file) via the ``--queries-file`` command
line argument, or the ``ksql.queries.file`` setting in the :ref:`KSQL configuration file <ksql-server-config>`. If a
server is running a predefined script, it will automatically disable its REST endpoint and interactive use.

.. tip:: When both the ``ksql.queries.file`` property and the ``--queries-file`` argument are present, the ``--queries-file`` argument will take precedence.

To start the KSQL Server in headless, non-interactive configuration via the ``--queries-file`` command line argument:
    #. Create a predefined script and save as an ``.sql`` file.

    #. Start the KSQL with the predefined script specified via the ``--queries-file`` argument.

       .. code:: bash

            $ <path-to-confluent>/bin/ksql-start-server <path-to-confluent>/etc/ksql/ksql-server.properties \
              --queries-file /path/to/queries.sql

To start the KSQL Server in headless, non-interactive configuration via the ``ksql.queries.file`` in the server configuration file:
   #. Configure the ``ksql-server.properties`` file.  The ``bootstrap.servers`` and ``ksql.queries.file``
      are required. For more information about configuration, see :ref:`ksql-server-config`.

      .. code:: bash

          # Inform the KSQL server where the Kafka cluster can be found:
          bootstrap.servers=localhost:9092

          # Define the location of the queries file to execute
          ksql.queries.file=/path/to/queries.sql

   #. Start the KSQL server with the configuration file specified.

      .. code:: bash

            $ <path-to-confluent>/bin/ksql-start-server <path-to-confluent>/etc/ksql/ksql-server.properties

