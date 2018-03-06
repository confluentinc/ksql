.. _ksql_concepts:

Concepts
========

.. contents::

==========
Components
==========

The main components of KSQL are the CLI, the server, and the experimental web interface.

KSQL CLI
--------

The KSQL CLI allows you to interactively write KSQL queries. Its
interface should be familiar to users of MySQL, Postgres, Oracle, Hive,
Presto, etc.

The KSQL CLI acts as a client to the KSQL server.

KSQL Server
-----------

The KSQL server runs the engine that executes KSQL queries, which
includes the data processing as well as reading data from and writing
data to the target Kafka cluster.

Servers can run in containers, virtual machines, and bare-metal machines. You can add or remove multiple servers in the
same resource pool to elastically scale query processing in or out. You can use different resource pools to achieve
workload isolation.

KSQL Experimental UI
--------------------
This version of KSQL includes an [experimental web interface](https://github.com/confluentinc/ksql-experimental-ui). This
interface provides a simple visual wrapper for the interactive KSQL CLI. The experimental web interface is only intended
for local development and testing. The KSQL CLI is recommended for production environments.

* By default, the KSQL Experimental UI runs on every KSQL Server and is accessible at ``http://ksqlserver:8080``
* To disable the experimental UI you must add ``ui.enabled=false`` to the server's configuration file ``ksqlserver.properties``.
* The UI shares the port with KSQL's REST API, which you can configure via the ``listeners`` configuration property.

===========
Terminology
===========

When using KSQL, the following terminology is used.

Stream
------

A stream is an unbounded sequence of structured data (“facts”). For
example, we could have a stream of financial transactions such as “Alice
sent $100 to Bob, then Charlie sent $50 to Bob”. Facts in a stream are
immutable, which means new facts can be inserted to a stream, but
existing facts can never be updated or deleted. Streams can be created
from a Kafka topic or derived from an existing table. A stream’s underlying data is durably stored (persisted) within a
Kafka topic on the Kafka brokers.

Table
-----

A table is a view of a stream, or another table, and represents a
collection of evolving facts. For example, we could have a table that
contains the latest financial information such as “Bob’s current account
balance is $150”. It is the equivalent of a traditional database table
but enriched by streaming semantics such as windowing. Facts in a table
are mutable, which means new facts can be inserted to the table, and
existing facts can be updated or deleted. Tables can be created from a
Kafka topic or derived from existing streams and tables. In both cases,
a table’s underlying data is durably stored (persisted) within a Kafka
topic on the Kafka brokers.

.. _modes-of-operation:

==================
Modes of operation
==================

Standalone mode
---------------

In certain deployment scenarios you may want to prevent interactive use of a KSQL cluster.
For example, you want to allow a team of users to develop and verify their queries on a shared testing KSQL cluster.
But when putting those queries to production you prefer to lock-down access to KSQL servers,
version-control the exact queries and storing them in a .sql file, and prevent users from interacting directly with the production KSQL cluster.

KSQL supports such locked-down deployment scenarios: you can configure servers to run a pre-defined script
(.sql file) via the --queries-file command line argument or the ksql.queries.file setting in the KSQL configuration file.
A server running such a pre-defined script will automatically disable its REST endpoint and thus disable interactive use.

To run KSQL in standalone mode:

-  Start the KSQL server in standalone mode via the command line argument:

  .. code:: bash

	$ ./bin/ksql-start-server path/to/ksql-server.properties --queries-file path/to/queries.sql

-  Start the KSQL server in standalone mode by adding `ksql.queries.file` property to `ksql-server.properties`

  .. code:: bash

  $ cat ksql-server.properties
  # You must set at least the following two properties:

  # Inform the KSQL Server where the Kafka cluster can be found:
  bootstrap.servers=localhost:9092

  # Define the location of the queries file to execute
  ksql.queries.file=path/to/queries.sql


  $ ./bin/ksql-start-server path/to/ksql-server.properties

Note: if both the `ksql.queries.file` property and the `--queries-file` argument are present, the
`--queries-file` argument will take precedence.


Client-server mode
------------------

In client-server mode, the KSQL servers are run separately from the KSQL CLI client. You can deploy servers on remote machines,
VMs, or containers and then the CLI connects to these remote servers.

You can add or remove servers from the same resource pool during live operations, to elastically scale query processing. You
can use different resource pools to achieve workload isolation. For example, you can deploy separate pools for production
and for testing.

.. image:: img/client-server.png

To run KSQL in client-server mode:

-  Start any number of server nodes:

   Start a server node with the command:

   .. code:: bash

   $ ./bin/ksql-server-start ksql-server.properties

   Where `ksql-server.properies` defines the :ref:`custom
   settings <configuring-ksql>` the KSQL should be started with and points
   KSQL at a specific Kafka cluster (see Streams :ref:`bootstrap servers <streams_developer-guide_required-configs>` setting):

   .. code:: bash

      $ cat ksql-server.properties
      # You must set at least the following two properties:

      # Inform the KSQL Server where the Kafka cluster can be found:
      bootstrap.servers=localhost:9092

      # The hostname/port on which this server will listen for client connections:
      listeners=http://0.0.0.0:8090

   KSQL servers that share the same ``command`` topic belong to the same resource pool. By default, a KSQL server uses the
   ``ksql__commands`` command topic. To assign a server to a different pool, change the ``ksql.command.topic.suffix`` setting in its configuration:

   .. code:: bash

      # Default value: `commands`.
      #
      # Changing this to `production_commands` as shown below will result in
      # the command topic named `ksql__production_commands` being used.
      ksql.command.topic.suffix = production_commands


-  Start any number of CLIs, specifying the desired KSQL server address
   as the ``remote`` endpoint:

   .. code:: bash

       $ ./bin/ksql http://my-ksql-server:8090

