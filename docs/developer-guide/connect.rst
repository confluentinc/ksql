.. _ksql-connect:

KSQL-Connect Integration
========================

`Kafka Connect`_ is an open source component of Kafka that simplifies loading and exporting data
between Kafka and external systems. KSQL provides functionality to manage and integrate with Kafka
Connect:

- Creating Connectors
- Describing Connectors
- Importing topics created by Connect to KSQL

.. _Kafka Connect: https://docs.confluent.io/current/connect/index.html

Setup
-----

There are two ways to deploy the KSQL-Connect integration:

#. **External**: If a Connect cluster is available, set the ``ksql.connect.url`` property in your
   KSQL server configuration file. The default value for this is ``localhost:8083``.
#. **Embedded**: KSQL can double as a Connect server and will run a `Distributed Connect`_ cluster
   co-located on the KSQL server instance. To do this, supply a connect properties configuration
   file to the server and specify this file in the ``ksql.connect.worker.config`` property.

If you additionally want KSQL to listen for changes in the Connect cluster and automatically import
topics as KSQL tables/streams, specify ``ksql.connect.polling.enable=true`` and provide the name
of the connect config topic as ``ksql.connect.configs.topic``.

.. note:: For environments that need to share connect clusters and provide predictable workloads,
          running Connect externally is the recommended deployment option.

.. _Distributed Connect: https://docs.confluent.io/current/connect/userguide.html#distributed-mode

Plugins
~~~~~~~

KSQL does not ship with connectors pre-installed and requires downloading and install connectors. A
good way to install connectors is through `Confluent Hub`_.

.. _Confluent Hub: https://www.confluent.io/hub/

.. _native-connectors:

Natively Supported Connectors
-----------------------------

While it is possible to create, describe and list connectors of all types, KSQL currently supports
a few connectors more intimately by providing templates to ease creation and custom code to import
topics created by these connectors into KSQL:

- `JDBC Source`_: since the JDBC connector does not automatically populate the key for the Kafka
  messages that it produces, KSQL supplies the ability to pass in ``"key"='<column_name>'`` in the
  ``WITH`` clause to extract a column from the value and make it the key.

.. _JDBC Source: https://docs.confluent.io/current/connect/kafka-connect-jms/index.html

======
Syntax
======

.. _create-connector:

CREATE CONNECTOR
-----------------

**Synopsis**

.. code:: sql

    CREATE SOURCE | SINK CONNECTOR connector_name WITH( property_name = expression [, ...]);

**Description**

Create a new Connector in the Kafka Connect cluster with the configuration passed in the WITH
clause. Note that some connectors have KSQL templates that simplify the configuration - for more
information see :ref:`native-connectors`.

Example:

.. code:: sql

    CREATE SOURCE CONNECTOR `jdbc-connector` WITH(
        "connector.class"='io.confluent.connect.jdbc.JdbcSourceConnector',
        "connection.url"='jdbc:postgresql://localhost:5432/my.db',
        "mode"='bulk',
        "topic.prefix"='jdbc-',
        "table.whitelist"='users',
        "key"='username');

DESCRIBE CONNECTOR
------------------

**Synopsis**

.. code:: sql

    DESCRIBE CONNECTOR connector_name;

Describe a connector. If the connector is one of the supported connectors this will also list the
tables/streams that were automatically imported to KSQL.

Example:

.. code:: sql

    DESCRIBE CONNECTOR "my-jdbc-connector";

Your output should resemble:

::

    Name                 : jdbc-connector
    Class                : io.confluent.connect.jdbc.JdbcSourceConnector
    Type                 : source
    State                : RUNNING
    WorkerId             : 10.200.7.69:8083

     Task ID | State   | Error Trace
    ---------------------------------
     0       | RUNNING |
    ---------------------------------

     KSQL Source Name     | Kafka Topic | Type
    --------------------------------------------
     JDBC_CONNECTOR_USERS | jdbc-users  | TABLE
    --------------------------------------------

SHOW CONNECTORS
---------------

**Synopsis**

.. code:: sql

    SHOW | LIST CONNECTORS;

**Description**

List all connectors in the connect cluster.

.. note:: This does not differentiate connectors created by KSQL with connectors that were created
independently using the Connect API.