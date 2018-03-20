.. _install_ksql-standalone:

Using KSQL in Standalone Mode
=============================

KSQL supports locked-down deployment scenarios where you can restrict interactive use of the KSQL cluster.

You can prevent interactive use of a KSQL cluster. For example, you want to allow a team of users to develop and verify
their queries on a shared testing KSQL cluster. But when putting those queries to production you prefer to lock-down access
to KSQL servers, version-control the exact queries and storing them in a .sql file, and prevent users from interacting
directly with the production KSQL cluster.

You can configure servers to run a predefined script (.sql file) via the ``--queries-file`` command line argument, or the
``ksql.queries.file`` setting in the :ref:`KSQL configuration file <common-configs>`. If a server is running a predefined
script, it will automatically disable its REST endpoint and interactive use.

.. tip::
    If the ``ksql.queries.file`` property and the ``--queries-file`` argument are present, the ``--queries-file`` argument will take precedence.

Start the KSQL server in standalone mode via the command line argument
    #. Create a predefined script and save as an ``.sql`` file.

    #. Start the KSQL in standalone mode with the predefined script specified via the ``--queries-file`` argument.

       .. code:: bash

            $ <path-to-confluent>/bin/ksql-start-server <path-to-confluent>/etc/ksql/ksql-server.properties \
              --queries-file <path-to-queries-file>.sql

Start the KSQL server in standalone mode via the ``ksql-server.properties`` file
   #. Configure the ``ksql-server.properties`` file.  The ``bootstrap.servers`` and ``ksql.queries.file``
      are required. For more information about configuration, see :ref:`common-configs`.

      .. code:: bash

          # Inform the KSQL Server where the Kafka cluster can be found:
          bootstrap.servers=localhost:9092

          # Define the location of the queries file to execute
          ksql.queries.file=path/to/queries.sql


   #. Start the KSQL in standalone mode with the properties file specified.

      .. code:: bash

            $ <path-to-confluent>/bin/ksql-start-server <path-to-confluent>/etc/ksql/ksql-server.properties




