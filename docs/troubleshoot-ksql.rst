.. _troubleshoot-ksql:

Troubleshoot KSQL issues
########################

This guide contains tips and tricks for troubleshooting KSQL problems.

SELECT query hangs and doesn’t stop
***********************************

Queries in KSQL, including non-persistent queries, like ``SELECT * FROM myTable``,
are continuous streaming queries. Streaming queries don't stop unless you end them
explicitly. In the KSQL CLI, press CTRL+C to stop a non-persistent query. To stop a
persistent query created by CREATE STREAM AS SELECT or CREATE TABLE AS SELECT,
use the TERMINATE statement. 

SELECT query returns no results
*******************************

Sometimes, when you run a KSQL query, like ``SELECT * FROM my-stream``, no
results are returned, and the KSQL CLI seems to hang.

First, press CTRL+C to stop printing the query and return to the console.

Next, follow this checklist to diagnose why your query returns no results:

* Is the query based on the wrong source topic?
* Is the source topic populated with data?
* Are new messages arriving in the source topic?
* Is KSQL consuming from an offset beyond the available data?
* Does the data match the query predicate?
* Are deserialization errors occurring while reading the data?

Check the stream's underlying Kafka topic
=========================================

Use the DESCRIBE EXTENDED statement to verify the source topic for the stream.
For example, if you have a ``pageviews`` stream on a Kafka topic named
``pageviews``, your output should resemble:

.. code:: sql

    DESCRIBE EXTENDED PAGEVIEWS;
    
Your output should resemble:

::
    
    Name                 : PAGEVIEWS
    [...]
    Kafka topic          : pageviews (partitions: 1, replication: 1)

Check the Kafka topic for data
==============================

Your query results may be empty because the underlying Kafka toppic isn't 
populated with data. Use the :ref:`kafkacat-usage` to consume messages and
print a summary.

.. code:: bash

    docker run --network ksql-troubleshooting_default --tty --interactive --rm \
              confluentinc/cp-kafkacat \
              kafkacat -b kafka:39092 \
              -C -t pageviews \
              -o beginning

If the topic is empty, your output should resemble:

.. code:: text

    % Reached end of topic pageviews [0] at offset 0

Read from the beginning of the topic
====================================

If |kcat| prints messages, the topic is populated, but it may not be receiving
*new* messages. By default, KSQL reads from the end of a topic, and if no new
messages are being written to the topic, your queries won't return any results.

To check your query, you can set the KSQL CLI to read from the beginning of
your topics by assigning the ``auto.offset.reset`` property to ``earliest``:

.. code:: sql

    SET 'auto.offset.reset'='earliest';
    
Your output should resemble:

::
    
    Successfully changed local property 'auto.offset.reset' from 'null' to 'earliest'

Run your query again and verify that you're getting results from the beginning
of the topic. If your query reaches the latest offset, and no new messages arrive,
it will appear to hang, but it's just waiting for the next message. Press CTRL+C
to stop the query.

Check the query predicate
=========================

If you're confident that you're querying the right Kafka topic, that the topic
is populated, and that you're reading data from before the latest offset, try
checking your query.

Your query may be filtering out all records because its predicate is too
restrictive. Try removing WHERE and HAVING clauses and running your query
again. 

.. _ksql-deserialization-errors:

Check for deserialization errors
================================

If KSQL can't deserialize message data, it won't write any SELECT results.
Use the DESCRIBE EXTENDED statement to check that the VALUE_FORMAT of the
stream matches the format of the records that |kcat| prints for your topic.

.. code:: sql
     
    DESCRIBE EXTENDED pageviews;
    
Your output should resemble:

::

    Name                 : PAGEVIEWS
    [...]
    Value format         : DELIMITED

Here is some example output from |kcat| for a DELIMITED topic:

.. code:: text

    1541463125587,User_2,Page_74
    1541463125823,User_2,Page_92
    1541463125931,User_3,Page_44
    % Reached end of topic pageviews [0] at offset 1538
    1541463126232,User_1,Page_28
    % Reached end of topic pageviews [0] at offset 1539
    1541463126637,User_7,Page_64
    % Reached end of topic pageviews [0] at offset 1540
    1541463126786,User_1,Page_83
    ^C

:ref:`ksql-check-server-logs` for serialization errors. For example, if your
query specifies JSON for the VALUE_FORMAT, and the underlying topic isn't
formatted as JSON, you'll see ``JsonParseException`` warnings in the KSQLServer
log:

.. code:: text

    [2018-09-17 12:29:09,929] WARN task [0_10] Skipping record due to deserialization error. topic=[_confluent-metrics] partition=[10] offset=[70] (org.apache.kafka.streams.processor.internals.RecordDeserializer:86)
     org.apache.kafka.common.errors.SerializationException: KsqlJsonDeserializer failed to deserialize data for topic: _confluent-metrics
     Caused by: com.fasterxml.jackson.core.JsonParseException: Unexpected character ((CTRL-CHAR, code 127)): expected a valid value (number, String, array, object, 'true', 'false' or 'null')

KSQL CLI doesn’t connect to KSQL server
***************************************

The following warning may occur when you start the KSQL CLI.

.. code:: text

    **************** WARNING ******************
    Remote server address may not be valid:
    Error issuing GET to KSQL server

    Caused by: java.net.SocketException: Connection reset
    Caused by: Connection reset
    *******************************************

Also, you may see a similar error when you create a KSQL query by using the
CLI.

.. code:: text

    Error issuing POST to KSQL server
    Caused by: java.net.SocketException: Connection reset
    Caused by: Connection reset

In both cases, the CLI can't connect to the KSQL server, which may be caused by
one of the following conditions.

- KSQL CLI isn't connected to the correct KSQL server port.
- KSQL server isn't running.
- KSQL server is running but listening on a different port.

Check the port that KSQL CLI is using
=====================================

Ensure that the KSQL CLI is configured with the correct KSQL server port.
By default, the server listens on port ``8088``. For more info, see 
:ref:`Starting the KSQL CLI <install_ksql-cli>`.

Check the KSQL server configuration
===================================

In the KSQL server configuration file, check that the list of listeners
has the host address and port configured correctly. Look for the ``listeners``
setting:

.. code:: text

    listeners=http://localhost:8088

For more info, see :ref:`Starting KSQL Server <start_ksql-server>`.

Check for a port conflict
=========================

There may be another process running on the port that the KSQL server listens
on. Use the following command to check the process that's running on the port
assigned to the KSQL server. This example checks the default port, which is
``8088``.  

.. code:: bash

    netstat -anv | egrep -w .*8088.*LISTEN

Your output should resemble:

.. code:: text

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

View the message count for a KSQL query
***************************************

You can check the health of a KSQL query by viewing the number of messages that
it has processed and counting how many processing failures have occurred.

Use the DESCRIBE EXTENDED statement to see metrics like ``total-messages`` and
``failed-messages-per-sec``, for example:

.. code:: sql

    DESCRIBE EXTENDED GOOD_RATINGS;
    
Your output should resemble:

::
    
    [...]
    Local runtime statistics
    ------------------------
    messages-per-sec:      1.10 total-messages:     2898 last-message: 9/17/18 1:48:47 PM UTC
     failed-messages:         0 failed-messages-per-sec:         0 last-failed: n/a
    (Statistics of the local KSQL server interaction with the Kafka topic GOOD_RATINGS)

The displayed metrics are local to the server where the DESCRIBE statement runs.

An increasing number of ``failed-messages`` may indicate problems with your query.
Typical sources of processing failures are :ref:`deserialization errors <ksql-deserialization-errors>`.

Can’t create a stream from the output of a windowed aggregate
*************************************************************

The output of a windowed aggregate is a record per grouping key and per window,
and is not a single record. This is not currently supported in KSQL.

KSQL doesn’t clean up its internal topics
*****************************************

Make sure that your Kafka cluster is configured with ``delete.topic.enable=true``.
For more information, see :cp-javadoc:`deleteTopics|clients/javadocs/org/apache/kafka/clients/admin/AdminClient.html`.

Replicated topic with Avro schema causes errors 
***********************************************

Confluent Replicator renames topics during replication, and if there are
associated Avro schemas, they aren't automatically matched with the renamed
topics.

In the KSQL CLI, the ``PRINT`` statement for a replicated topic works, which
shows that the Avro schema ID exists in the Schema Registry, and KSQL can
deserialize the Avro message. But ``CREATE STREAM`` fails with a deserialization
error:

.. code:: sql

    CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH (kafka_topic='pageviews.replica', value_format='AVRO');

If you have serialization errors, your output should resemble:

::

    [2018-06-21 19:12:08,135] WARN task [1_6] Skipping record due to deserialization error. topic=[pageviews.replica] partition=[6] offset=[1663] (org.apache.kafka.streams.processor.internals.RecordDeserializer:86)
    org.apache.kafka.connect.errors.DataException: pageviews.replica
            at io.confluent.connect.avro.AvroConverter.toConnectData(AvroConverter.java:97)
            at io.confluent.ksql.serde.connect.KsqlConnectDeserializer.deserialize(KsqlConnectDeserializer.java:48)
            at io.confluent.ksql.serde.connect.KsqlConnectDeserializer.deserialize(KsqlConnectDeserializer.java:27)

The solution is to register schemas manually against the replicated subject
name for the topic:

.. code:: bash

    # Original topic name = pageviews
    # Replicated topic name = pageviews.replica
    curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "{\"schema\": $(curl -s http://localhost:8081/subjects/pageviews-value/versions/latest | jq '.schema')}" http://localhost:8081/subjects/pageviews.replica-value/versions

.. _ksql-check-server-logs:

Check the KSQL server logs 
**************************

If you're still having trouble, check the KSQL server logs for errors. 

.. code:: bash

    confluent log ksql-server

KSQL writes most of its log messages to stdout by default.

Look for logs in the default directory at ``/usr/local/logs`` or in the
``LOG_DIR`` that you assign when you start the KSQL CLI. For more info, see 
:ref:`Starting the KSQL CLI <install_ksql-cli>`.

If you installed Confluent Platform by using RPM/DEB packages, the logs are 
in ``/var/log/confluent/``.

If you’re running KSQL by using Docker, the output is in the container logs,
for example:

.. code:: bash

    docker logs <container-id>
    docker-compose logs ksql-server

