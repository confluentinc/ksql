.. _ksql_supported_versions:

.. _install_overview:

Installing KSQL
===============

KSQL is a component of |cp| and the KSQL binaries are located at `https://www.confluent.io/download/ <https://www.confluent.io/download/>`_
as a part of the |cp| bundle.

KSQL must have access to a running |ak-tm| cluster, which can be in your data center, in a public cloud, |ccloud|, etc.

Docker support
    You can deploy KSQL by using :ref:`Docker containers <install-ksql-with-docker>`. Starting with |cp| 4.1.2,
    Confluent maintains images at `Docker Hub <https://hub.docker.com/r/confluentinc/cp-ksql-server/>`__.
    To start KSQL containers in configurations like "KSQL Headless Server" and
    "Interactive Server with Interceptors", see
    :ref:`Docker Configuration Parameters <config_reference>`.

Watch the `screencast of Installing and Running KSQL <https://www.youtube.com/embed/icwHpPm-TCA>`_ on YouTube.

---------------------------------------
Supported Versions and Interoperability
---------------------------------------

.. include:: ../includes/ksql-supported-versions.rst


.. _install_ksql-server:

-------------------------
Installation Instructions
-------------------------

Follow the instructions at :ref:`ce-quickstart`.

Also, you can install KSQL individually by using the
:ref:`confluent-ksql package <confluent-ksql-package>`.
For more information, see :ref:`available_packages`.

---------------------------------
Scale Your KSQL Server Deployment
---------------------------------

You can scale KSQL by adding more capacity per server (vertically) or by adding
more servers (horizontally). Also, you can scale KSQL clusters during live operations
without loss of data. For more information, see :ref:`ksql-capacity-planning-scaling`.

.. _start_ksql-server:

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

Follow these instructions to start KSQL server using the ``ksql-server-start`` script.

.. include:: ../../../includes/installation-types-zip-tar.rst

#.  Specify your KSQL server configuration parameters. You can also set any property for the Kafka Streams API, the Kafka
    producer, or the Kafka consumer. The required parameters are ``bootstrap.servers`` and ``listeners``. You can specify
    the parameters in the KSQL properties file or the ``KSQL_OPTS`` environment variable. Properties set with ``KSQL_OPTS``
    take precedence over those specified in the properties file.

    A recommended approach is to configure a common set of properties using the KSQL configuration file and override
    specific properties as needed, using the ``KSQL_OPTS`` environment variable.

    Here are the default settings:

    ::

        bootstrap.servers=localhost:9092
        listeners=http://0.0.0.0:8088

    For more information, see :ref:`ksql-server-config`.

#.  Start a server node with this command:

    .. code:: bash

        $ <path-to-confluent>/bin/ksql-server-start <path-to-confluent>/etc/ksql/ksql-server.properties

    .. tip:: You can view the KSQL server help text by running ``<path-to-confluent>/bin/ksql-server-start --help``.

         ::

                NAME
                        server - KSQL Cluster

                SYNOPSIS
                        server [ {-h | --help} ] [ --queries-file <queriesFile> ] [--]
                                <config-file>

                OPTIONS
                        -h, --help
                            Display help information

                        --queries-file <queriesFile>
                            Path to the query file on the local machine.

                        --
                            This option can be used to separate command-line options from the
                            list of arguments (useful when arguments might be mistaken for
                            command-line options)

                        <config-file>
                            A file specifying configs for the KSQL Server, KSQL, and its
                            underlying Kafka Streams instance(s). Refer to KSQL documentation
                            for a list of available configs.

                            This option may occur a maximum of 1 times

#. Have a look at :ref:`this page <restrict-ksql-interactive>` for instructions on running KSQL in non-interactive (aka headless) mode.

.. _install_ksql-cli:

---------------------
Starting the KSQL CLI
---------------------

The KSQL CLI is a client that connects to the KSQL servers.

You can start the KSQL CLI by providing the connection information to the KSQL server.

.. code:: bash

    $ LOG_DIR=./ksql_logs <path-to-confluent>/bin/ksql http://localhost:8088

.. include:: ../includes/ksql-includes.rst
    :start-after: log_limitations_start
    :end-before: log_limitations_qs_end

After KSQL is started, your terminal should resemble this.

.. include:: ../includes/ksql-includes.rst
    :start-after: CLI_welcome_start
    :end-before: CLI_welcome_end

.. tip:: You can view the KSQL CLI help text by running ``<path-to-confluent>/bin/ksql --help``.

    ::

            NAME
                    ksql - KSQL CLI

            SYNOPSIS
                    ksql [ --config-file <configFile> ] [ {-h | --help} ]
                            [ --output <outputFormat> ]
                            [ --query-row-limit <streamedQueryRowLimit> ]
                            [ --query-timeout <streamedQueryTimeoutMs> ] [--] <server>

            OPTIONS
                    --config-file <configFile>
                        A file specifying configs for Ksql and its underlying Kafka Streams
                        instance(s). Refer to KSQL documentation for a list of available
                        configs.

                    -h, --help
                        Display help information

                    --output <outputFormat>
                        The output format to use (either 'JSON' or 'TABULAR'; can be changed
                        during REPL as well; defaults to TABULAR)

                    --query-row-limit <streamedQueryRowLimit>
                        An optional maximum number of rows to read from streamed queries

                        This options value must fall in the following range: value >= 1


                    --query-timeout <streamedQueryTimeoutMs>
                        An optional time limit (in milliseconds) for streamed queries

                        This options value must fall in the following range: value >= 1


                    --
                        This option can be used to separate command-line options from the
                        list of arguments (useful when arguments might be mistaken for
                        command-line options)

                    <server>
                        The address of the Ksql server to connect to (ex:
                        http://confluent.io:9098)

                        This option may occur a maximum of 1 times
                            
                            
-----------------------------
Configuring KSQL for |ccloud|
-----------------------------

You can use KSQL with a Kafka cluster in |ccloud|. For more information, see :ref:`install_ksql-ccloud`.
