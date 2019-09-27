.. _ksql-connect:

KSQL-Connect Integration
========================

{{ site.kconnect-long }} is an open source component of Kafka that simplifies loading and exporting data
between Kafka and external systems. KSQL provides functionality to manage and integrate with
{{ site.kconnect }}:

- Creating Connectors
- Describing Connectors
- Importing topics created by {{ site.kconnect }} to KSQL

Setup
-----

There are two ways to deploy the KSQL-Connect integration:

#. **External**: If a {{ site.kconnect }} cluster is available, set the ``ksql.connect.url`` property in your
   KSQL server configuration file. The default value for this is ``localhost:8083``.
#. **Embedded**: KSQL can double as a {{ site.kconnect }} server and will run a
   :ref:`connect_userguide_distributed_config` cluster co-located on the KSQL server instance. To do
   this, supply a connect properties configuration file to the server and specify this file in the
   ``ksql.connect.worker.config`` property.

.. note:: For environments that need to share connect clusters and provide predictable workloads,
          running {{ site.kconnect }} externally is the recommended deployment option.

Plugins
~~~~~~~

KSQL does not ship with connectors pre-installed and requires downloading and installing connectors.
A good way to install connectors is through `Confluent Hub`_.

.. _Confluent Hub: https://www.confluent.io/hub/

.. _native-connectors:

Natively Supported Connectors
-----------------------------

While it is possible to create, describe and list connectors of all types, KSQL currently supports
a few connectors more natively by providing templates to ease creation and custom code to explore
topics created by these connectors into KSQL:

- :ref:`connect_jdbc`: since the JDBC connector does not automatically populate the key for the
  Kafka messages that it produces, KSQL supplies the ability to pass in ``"key"='<column_name>'`` in
  the ``WITH`` clause to extract a column from the value and make it the key.

======
Syntax
======

.. _create-connector:

CREATE CONNECTOR
----------------

**Synopsis**

.. code:: sql

    CREATE SOURCE | SINK CONNECTOR connector_name WITH( property_name = expression [, ...]);

**Description**

Create a new Connector in the {{ site.kconnect-long }} cluster with the configuration passed in the WITH
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

DROP CONNECTOR
--------------

**Synopsis**

.. code:: sql

    DROP CONNECTOR connector_name;

**Description**

Drop a Connector and delete it from the {{ site.kconnect }} cluster. The topics associated with that cluster
will not be deleted by this command.

DESCRIBE CONNECTOR
------------------

**Synopsis**

.. code:: sql

    DESCRIBE CONNECTOR connector_name;

Describe a connector. If the connector is one of the supported connectors, this will also list the
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

     Related Topics
    ----------------
     jdbc-users
    ----------------

SHOW CONNECTORS
---------------

**Synopsis**

.. code:: sql

    SHOW | LIST CONNECTORS;

**Description**

List all connectors in the {{ site.kconnect }} cluster.

.. note:: This does not differentiate connectors created by KSQL with connectors that were created
independently using the {{ site.kconnect }} API.