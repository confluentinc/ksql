.. cli_limitations_start

.. important::
    The :ref:`confluent local <cli-command-reference>` commands are intended for a single-node development environment and
    are not suitable for a production environment. The data that are produced are transient and are intended to be
    temporary. For production-ready workflows, see `Install and Upgrade <https://docs.confluent.io/current/installation/index.html>`__.
.. cli_limitations_end
.. leave this blank space or the rest of includes will not properly function

l
e
a
v
e
t
h
i
s

.. CE_CLI_startup_output

.. code:: bash

    Starting zookeeper
    zookeeper is [UP]
    Starting kafka
    kafka is [UP]
    Starting schema-registry
    schema-registry is [UP]
    Starting kafka-rest
    kafka-rest is [UP]
    Starting connect
    connect is [UP]
    Starting ksql-server
    ksql-server is [UP]
    Starting control-center
    control-center is [UP]

.. COS_CP_CLI_startup_output

.. code:: bash

    Starting zookeeper
    zookeeper is [UP]
    Starting kafka
    kafka is [UP]
    Starting schema-registry
    schema-registry is [UP]
    Starting kafka-rest
    kafka-rest is [UP]
    Starting connect
    connect is [UP]
    Starting ksql-server
    ksql-server is [UP]
.. COS_CP_CLI_startup_output_end

.. ce_cli_stop_destroy_output_start
.. code:: bash

    Stopping control-center
    control-center is [DOWN]
    Stopping ksql-server
    ksql-server is [DOWN]
    Stopping connect
    connect is [DOWN]
    Stopping kafka-rest
    kafka-rest is [DOWN]
    Stopping schema-registry
    schema-registry is [DOWN]
    Stopping kafka
    kafka is [DOWN]
    Stopping zookeeper
    zookeeper is [DOWN]
    Deleting: /var/folders/ty/rqbqmjv54rg_v10ykmrgd1_80000gp/T/confluent.PkQpsKfE
.. ce_cli_stop_destroy_output_stop

.. ce_cli_stop_output_start

::

    Stopping control-center
    control-center is [DOWN]
    Stopping ksql-server
    ksql-server is [DOWN]
    Stopping connect
    connect is [DOWN]
    Stopping kafka-rest
    kafka-rest is [DOWN]
    Stopping schema-registry
    schema-registry is [DOWN]
    Stopping kafka
    kafka is [DOWN]
    Stopping zookeeper
    zookeeper is [DOWN]

.. ce_cli_stop_output_stop
