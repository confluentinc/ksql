.. _troubleshoot-ksql:

This guide contains tips and tricks for troubleshooting KSQL problems.


SELECT query hangs and doesn’t stop?
************************************

Queries in KSQL, including non-persistent queries, like ``SELECT * FROM myTable``,
are continuous streaming queries. Streaming queries don't stop unless you end them
explicitly. in the KSQL CLI, press CTRL+C To stop a non-persistent query.


No results from ``SELECT * FROM`` table or stream?
**************************************************

This is typically caused by the query being configured to process only newly arriving data instead, and no new input records are being received. To fix, do one of the following:

- Run this command: ``SET 'auto.offset.reset' = 'earliest';``. For more information, see :ref:`install_cli-config` and
  :ref:`ksql-server-config`.
- Write new records to the input topics.

------------------------------------------------------------
Can’t create a stream from the output of windowed aggregate?
------------------------------------------------------------
The output of a windowed aggregate is a record per grouping key and per window, and is not a single record. This is not
currently supported in KSQL.

------------------------------------------
KSQL doesn’t clean up its internal topics?
------------------------------------------
Make sure that your Kafka cluster is configured with ``delete.topic.enable=true``. For more information, see :cp-javadoc:`deleteTopics|clients/javadocs/org/apache/kafka/clients/admin/AdminClient.html`.

----------------------------------------
KSQL CLI doesn’t connect to KSQL server? 
----------------------------------------
The following warning may occur when you start the KSQL CLI.   

.. code:: bash

    **************** WARNING ******************
    Remote server address may not be valid:
    Error issuing GET to KSQL server

    Caused by: java.net.SocketException: Connection reset
    Caused by: Connection reset
    *******************************************

Also, you may see a similar error when you create a KSQL query by using the
CLI.

.. code:: bash

    Error issuing POST to KSQL server
    Caused by: java.net.SocketException: Connection reset
    Caused by: Connection reset

In both cases, the CLI can't connect to the KSQL server, which may be caused by
one of the following conditions.

- KSQL CLI isn't connected to the correct KSQL server port.
- KSQL server isn't running.
- KSQL server is running but listening on a different port.

Check the port that KSQL CLI is using
-------------------------------------

Ensure that the KSQL CLI is configured with the correct KSQL server port.
By default, the server listens on port ``8088``. For more info, see 
:ref:`Starting the KSQL CLI <install_ksql-cli>`.

Check the KSQL server configuration
-----------------------------------

In the KSQL server configuration file, check that the list of listeners
has the host address and port configured correctly. Look for the ``listeners``
setting:

.. code:: bash

    listeners=http://localhost:8088

For more info, see :ref:`Starting KSQL Server <start_ksql-server>`.

Check for a port conflict
-------------------------

There may be another process running on the port that the KSQL server listens
on. Use the following command to check the process that's running on the port
assigned to the KSQL server. This example checks the default port, which is
``8088``.  

.. code:: bash

    netstat -anv | egrep -w .*8088.*LISTEN

Your output should resemble:

.. code:: bash

    tcp4  0 0  *.8088       *.*    LISTEN      131072 131072    46314      0

In this example, ``46314`` is the PID of the process that's listening on port
``8088``. Run the following command to get info on the process.

.. code:: bash

    ps -wwwp <pid>

Your output should resemble:

.. code:: bash

    io.confluent.ksql.rest.server.KsqlServerMain ./config/ksql-server.properties

If the ``KsqlServerMain`` process isn't shown, a different process has taken the
port that ``KsqlServerMain`` would normally use. Check the assigned listeners in 
the KSQL server configuration, and restart the KSQL CLI with the correct port.

------------------------------------------------
Replicated topic with Avro schema causes errors? 
------------------------------------------------

Confluent Replicator renames topics during replication, and if there are
associated Avro schemas, they aren't automatically matched with the renamed
topics.

In the KSQL CLI, the ``PRINT`` statement for a replicated topic works, which shows
that the Avro schema ID exists in the Schema Registry, and KSQL can deserialize
the Avro message. But ``CREATE STREAM`` fails with a deserialization error:

.. code:: bash

    CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH (kafka_topic='pageviews.replica', value_format='AVRO');

    [2018-06-21 19:12:08,135] WARN task [1_6] Skipping record due to deserialization error. topic=[pageviews.replica] partition=[6] offset=[1663] (org.apache.kafka.streams.processor.internals.RecordDeserializer:86)
    org.apache.kafka.connect.errors.DataException: pageviews.replica
            at io.confluent.connect.avro.AvroConverter.toConnectData(AvroConverter.java:97)
            at io.confluent.ksql.serde.connect.KsqlConnectDeserializer.deserialize(KsqlConnectDeserializer.java:48)
            at io.confluent.ksql.serde.connect.KsqlConnectDeserializer.deserialize(KsqlConnectDeserializer.java:27)

The solution is to register schemas manually against the replicated subject name for the topic:

.. code:: bash

    # Original topic name = pageviews
    # Replicated topic name = pageviews.replica
    curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "{\"schema\": $(curl -s http://localhost:8081/subjects/pageviews-value/versions/latest | jq '.schema')}" http://localhost:8081/subjects/pageviews.replica-value/versions

----------------------
Check KSQL server logs 
----------------------
If you're still having trouble, check the KSQL server logs for errors. 

.. code:: bash

    confluent log ksql-server


Look for logs in the default directory at ``/usr/local/logs`` or in the
``LOG_DIR`` that you assign when you start the KSQL CLI. For more info, see 
:ref:`Starting the KSQL CLI <install_ksql-cli>`.