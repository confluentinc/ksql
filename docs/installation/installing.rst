.. _install_overview:

Installing KSQL
===============

KSQL is a component of |cp| and the KSQL binaries are located at `https://www.confluent.io/download/ <https://www.confluent.io/download/>`_
as a part of the |cp| bundle.

KSQL must have access to a running Kafka cluster, which can be on prem, |ccloud|, etc.

Docker support
    You can deploy KSQL in Docker, however the current release does not yet ship with ready-to-use KSQL Docker images for
    production. These images are coming soon.

.. _install_ksql-cli:

---------------------
Starting the KSQL CLI
---------------------

The KSQL CLI is a client that connects to the KSQL servers.

You can start the KSQL CLI by providing the connection information to the KSQL server.

.. code:: bash

    $ <path-to-confluent>/bin/ksql http://localhost:8088

After KSQL is started, your terminal should resemble this.

.. include:: ../includes/ksql-includes.rst
    :start-line: 17
    :end-line: 38

.. _install_ksql-server:

------------------------
Starting the KSQL Server
------------------------

The KSQL servers are run separately from the KSQL CLI client and Kafka brokers. You can deploy servers on remote machines,
VMs, or containers and then the CLI connects to these remote servers.

You can add or remove servers from the same resource pool during live operations, to elastically scale query processing. You
can use different resource pools to support workload isolation. For example, you could deploy separate pools for production
and for testing.

You can only connect to one KSQL server at a time. The KSQL CLI does not support automatic failover to another KSQL server.

.. image:: ../img/client-server.png
    :align: center

.. tip:: For development and testing purposes, you can use Confluent CLI to spin up services on a single host. For more
    information, see :ref:`quickstart`.

Follow these instructions to start KSQL server.

#.  Customize the KSQL ``ksql-server.properties`` file.  By default, the configuration file is located at ``/etc/ksql/ksql-server.properties``.
    The required parameters are ``bootstrap.servers`` and ``listeners``. You can also set any property the Kafka Streams
    API or the Kafka producer and consumer would understand. For a description of common configurations, see :ref:`configuring-ksql`.

    .. tip:: KSQL servers that share the same ``command`` topic belong to the same resource pool. By default, KSQL servers
             use the ``ksql__commands`` command topic. To assign a server to a different pool, change the ``ksql.command.topic.suffix``
             setting. For example, if you change to ``ksql.command.topic.suffix = production_commands``, the command topic will be named
             ``ksql__production_commands``.

    Here are the default settings:

    .. code:: bash

        bootstrap.servers=localhost:9092
        ksql.command.topic.suffix=commands
        listeners=http://localhost:8080
        ui.enabled=true

#.  Start a server node with this command:

    .. code:: bash

        $ <path-to-confluent>/bin/ksql-server-start ksql-server.properties






