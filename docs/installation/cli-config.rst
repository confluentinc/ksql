.. _install_cli-config:

Configuring KSQL CLI
====================

Connecting the CLI to a Cluster
-------------------------------

You can connect the KSQL CLI to one KSQL server per cluster.

.. important:: There is no automatic failover of your CLI session to another KSQL server if the original server that the
               CLI is connected to becomes unavailable. Any persistent queries you executed will continue to run in the
               KSQL cluster.

Configuring Per-session Properties
----------------------------------

Configure KSQL with the ``/etc/ksql/ksql-server.properties`` file. A property remains in effect for the remainder of the KSQL
CLI session, or until you issue another SET statement to change it. The syntax of properties files follow Java conventions.
Here is the basic syntax.

.. code:: java

    <property-name>=<property-value>

Here is an example ``ksql-server.properties`` file:

.. code:: java

        bootstrap.servers=localhost:9092
        ksql.command.topic.suffix=commands
        listeners=http://localhost:8088

After you have configured your properties file, start KSQL with your properties file specified.

.. code:: bash

    $ <path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties

.. tip:: The KSQL server command topic determines the resource pool. By default, KSQL servers use the ``ksql__commands``
         command topic. To assign a server to a different pool, change the ``ksql.command.topic.suffix`` setting. For
         example, if you change to ``ksql.command.topic.suffix = production_commands``, the command topic will be named
         ``ksql__production_commands``.
