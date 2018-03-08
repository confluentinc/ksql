.. _install_ksql:

Installing KSQL
---------------

KSQL is a component of |cp| and is automatically installed and running when you install |cp|. For |cp| installation,
see the :ref:`quickstart`.

Interoperability
    .. include:: ../../../includes/installation.rst
        :start-line: 84
        :end-line: 95


To start the KSQL CLI, enter this command:

.. code:: bash

    $ <path-to-confluent>/bin/ksql

After KSQL is started, your terminal should resemble this.

.. code:: bash

                          ===========================================
                          =        _  __ _____  ____  _             =
                          =       | |/ // ____|/ __ \| |            =
                          =       | ' /| (___ | |  | | |            =
                          =       |  <  \___ \| |  | | |            =
                          =       | . \ ____) | |__| | |____        =
                          =       |_|\_\_____/ \___\_\______|       =
                          =                                         =
                          =  Streaming SQL Engine for Apache Kafka® =
                          ===========================================

        Copyright 2018 Confluent Inc.

        CLI v0.5, Server v0.5 located at http://localhost:8090

        Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

        ksql>


For advanced installation and configuration, see the following topics.

.. toctree::
    :maxdepth: 3

    client-server
    config-ksql
    upgrading








