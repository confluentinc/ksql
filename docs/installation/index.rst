.. _install_ksql:

Installing KSQL
---------------

KSQL is a component of |cp| and is automatically installed and running when you install |cp|. To get started,
see the :ref:`quickstart`.

Interoperability
    +--------------------+------------------+------------------+------------------+------------------+
    |        KSQL        |        0.1       |        0.2       |        0.3       |        0.4       |
    +====================+==================+==================+==================+==================+
    |    Apache Kafka    | 0.10.1 and later | 0.11.0 and later | 0.11.0 and later | 0.11.0 and later |
    +--------------------+------------------+------------------+------------------+------------------+
    | Confluent Platform | 3.1.0 and later  | 3.3.0 and later  | 3.3.0 and later  | 3.3.0 and later  |
    +--------------------+------------------+------------------+------------------+------------------+


To start the KSQL CLI, enter this command:

.. code:: bash

    $ <path-to-confluent>/bin/ksql

For advanced installation and configuration, see the following topics.

.. toctree::
    :maxdepth: 3

    client-server
    config-ksql
    upgrading








