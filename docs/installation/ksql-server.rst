.. _install_ksql-server:

Starting the KSQL Server
========================

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




