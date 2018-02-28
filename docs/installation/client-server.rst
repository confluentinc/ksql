.. install_ksql-client-server:

Using Client-Server Mode
------------------------

In client-server mode, the KSQL servers are run separately from the KSQL CLI client. You can deploy servers on remote machines,
VMs, or containers and then the CLI connects to these remote servers.

You can add or remove servers from the same resource pool during live operations, to elastically scale query processing. You
can use different resource pools to achieve workload isolation. For example, you can deploy separate pools for production
and for testing.

.. image:: ../img/client-server.png
    :align: center

To run KSQL in client-server mode:

#.  Configure KSQL with the ``/config/ksqlserver.properties`` file.

    .. tip:: KSQL servers that share the same ``command`` topic belong to the same resource pool. By default, KSQL servers use the
    ``ksql__commands`` command topic. To assign a server to a different pool, change the ``ksql.command.topic.suffix`` setting.
    For example, if you change to ``ksql.command.topic.suffix = production_commands``, the command topic will be named
    ``ksql__production_commands``. For more information, see :ref:`configuring-ksql`.

#.  Start a server node with this command:

    .. code:: bash

        $ <path-to-confluent>/bin/ksql-server-start ksqlserver.properties

#.  Start any number of CLIs, specifying the desired KSQL server address as the ``remote`` endpoint:

    .. code:: bash

        $ ./bin/ksql-cli remote http://my-ksql-server:8090



