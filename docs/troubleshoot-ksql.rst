.. _troubleshoot-ksql:

Troubleshoot KSQL issues
###########################

This guide contains troubleshooting information for many KSQL issues.

SELECT query does not stop
**************************

KSQL queries streams continuously and must be stopped explicitly. In the CLI,
use Ctrl-C to stop non-persistent queries, like ``SELECT * FROM myTable``.
To stop a persistent query created by CREATE STREAM AS SELECT or
CREATE TABLE AS SELECT, use the TERMINATE statement: ``TERMINATE query_id;``.
For more information, see :ref:`ksql-terminate`.

SELECT query returns no results
*******************************

If a KSQL query returns no results and the CLI hangs, use ``Ctrl-C`` to stop the query and then review the following topics to diagnose the issue.

Verify that the query is based on the correct source topic
==========================================================

Use the ``DESCRIBE EXTENDED`` statement to view the |ak-tm| source topic for the stream. For example, if you have a ``pageviews`` stream on a Kafka topic named ``pageviews``, enter the following statement in the CLI:

.. code:: sql

    DESCRIBE EXTENDED PAGEVIEWS;

Example output showing the source topic:

::

    Name                 : PAGEVIEWS
    [...]
    Kafka topic          : pageviews (partitions: 1, replication: 1)


Verify that the source topic is populated with data
===================================================

Your query results may be empty because the Kafka source topic is not populated with data. Use the |kcat| to consume messages and print a summary.

.. code:: bash

    docker run --network ksql-troubleshooting_default --tty --interactive --rm \
              confluentinc/cp-kafkacat \
              kafkacat -b kafka:39092 \
              -C -t pageviews \
              -o beginning

Example output showing an empty source topic:

.. code:: text

    % Reached end of topic pageviews [0] at offset 0


Verify that new messages are arriving at the source topic
=========================================================

The topic is populated if the |kcat| prints messages. However, it may not be receiving *new* messages. By default, KSQL reads from the end of a topic. A query does not return results if no new messages are being written to the topic.

To check your query, you can set KSQL to read from the beginning of a topic by assigning the ``auto.offset.reset`` property to ``earliest`` using following statement:

.. code:: sql

    SET 'auto.offset.reset'='earliest';

Example output showing a successful change:

::

    Successfully changed local property 'auto.offset.reset' from 'null' to 'earliest'

Run your query again. You should get results from the beginning of the topic. Note that the query may appear to hang if the query reaches the latest offset and no new messages arrive. The query is simply waiting for the next message. Use ``Ctrl-C`` to stop the query.


Verify that the query predicate is not too restrictive
======================================================

If the previous solutions do not resolve the issue, your query may be filtering out all records because its predicate is too restrictive. Remove ``WHERE`` and ``HAVING`` clauses and run your query again.

.. _ksql-deserialization-errors:


Verify that there are no deserialization errors
===============================================

KSQL will not write query results if it is not able to deserialize message data. Use the ``DESCRIBE EXTENDED`` statement to check that the ``VALUE_FORMAT`` of the stream matches the format of the records that |kcat| prints for your topic. Enter the following statement in the CLI:

.. code:: sql

    DESCRIBE EXTENDED pageviews;

Example output:

::

    Name                 : PAGEVIEWS
    [...]
    Value format         : DELIMITED

Example output from |kcat| for a DELIMITED topic:

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
query specifies JSON for the ``VALUE_FORMAT``, and the underlying topic is not
formatted as JSON, you'll see ``JsonParseException`` warnings in the KSQL server log. For example:

.. code:: text

    [2018-09-17 12:29:09,929] WARN task [0_10] Skipping record due to deserialization error. topic=[_confluent-metrics] partition=[10] offset=[70] (org.apache.kafka.streams.processor.internals.RecordDeserializer:86)
     org.apache.kafka.common.errors.SerializationException: KsqlJsonDeserializer failed to deserialize data for topic: _confluent-metrics
     Caused by: com.fasterxml.jackson.core.JsonParseException: Unexpected character ((CTRL-CHAR, code 127)): expected a valid value (number, String, array, object, 'true', 'false' or 'null')


KSQL CLI does not connect to KSQL server
****************************************

The following warning may occur when you start the CLI.

.. code:: text

    **************** WARNING ******************
    Remote server address may not be valid:
    Error issuing GET to KSQL server

    Caused by: java.net.SocketException: Connection reset
    Caused by: Connection reset
    *******************************************

A similar error may display when you create a KSQL query using the CLI.

.. code:: text

    Error issuing POST to KSQL server
    Caused by: java.net.SocketException: Connection reset
    Caused by: Connection reset

In both cases, the CLI is not able to connect to the KSQL server. Review the following topics to diagnose the issue.


Verify that the KSQL CLI is using the correct port
==================================================

By default, the server listens on port ``8088``. See
:ref:`Starting the KSQL CLI <install_ksql-cli>` for more information.


Verify that the KSQL server configuration is correct
====================================================

In the KSQL server configuration file, check that the list of listeners
has the host address and port configured correctly. Search for the ``listeners``
setting in the file and verify it is set correctly.

.. code:: text

    listeners=http://localhost:8088

See :ref:`Starting KSQL Server <start_ksql-server>` for more information.


Verify that there are no port conflicts
=======================================

There may be another process running on the port that the KSQL server listens
on. Use the following command to get the Process ID (PID) for the process running on the port
assigned to the KSQL server. The command below checks the default ``8088`` port.

.. code:: bash

    netstat -anv | egrep -w .*8088.*LISTEN

Example output:

.. code:: text

    tcp4  0 0  *.8088       *.*    LISTEN      131072 131072    46314      0

In this example, ``46314`` is the PID of the process that is listening on port
``8088``. Run the following command to get information about process ``46314``.

.. code:: bash

    ps -wwwp 46314

Example output:

.. code:: bash

    io.confluent.ksql.rest.server.KsqlServerMain ./config/ksql-server.properties

If the ``KsqlServerMain`` process is not shown, a different process has taken the
port that ``KsqlServerMain`` would normally use. Search for the ``listeners``
setting in the KSQL server configuration file and get the correct port. Start the CLI using the correct port.

See :ref:`Starting KSQL Server <start_ksql-server>` and :ref:`Starting the KSQL CLI <install_ksql-cli>` for more information.

Cannot create a stream from the output of a windowed aggregate
***************************************************************

Window aggregation is not currently supported in KSQL.


KSQL does not clean up internal topics
*****************************************

Make sure that your Kafka cluster is configured with ``delete.topic.enable=true``.
See :cp-javadoc:`deleteTopics|clients/javadocs/org/apache/kafka/clients/admin/AdminClient.html` for more information.


Replicated topic with Avro schema causes errors
***********************************************

The Confluent Replicator renames topics during replication. If there are
associated Avro schemas, they are not automatically matched with the renamed
topics after replication completes.

Using the ``PRINT`` statement for a replicated topic shows that the Avro schema ID exists in the Schema Registry. KSQL can
deserialize the Avro message, however the ``CREATE STREAM`` statement fails with a deserialization error. For example:

.. code:: sql

    CREATE STREAM pageviews_original (viewtime bigint, userid varchar, pageid varchar) WITH (kafka_topic='pageviews.replica', value_format='AVRO');

Example output with a deserialization error:

::

    [2018-06-21 19:12:08,135] WARN task [1_6] Skipping record due to deserialization error. topic=[pageviews.replica] partition=[6] offset=[1663] (org.apache.kafka.streams.processor.internals.RecordDeserializer:86)
    org.apache.kafka.connect.errors.DataException: pageviews.replica
            at io.confluent.connect.avro.AvroConverter.toConnectData(AvroConverter.java:97)
            at io.confluent.ksql.serde.connect.KsqlConnectDeserializer.deserialize(KsqlConnectDeserializer.java:48)
            at io.confluent.ksql.serde.connect.KsqlConnectDeserializer.deserialize(KsqlConnectDeserializer.java:27)

The solution is to register Avro schemas manually against the replicated subject name for the topic. For example:

.. code:: bash

    # Original topic name = pageviews
    # Replicated topic name = pageviews.replica
    curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "{\"schema\": $(curl -s http://localhost:8081/subjects/pageviews-value/versions/latest | jq '.schema')}" http://localhost:8081/subjects/pageviews.replica-value/versions

.. _ksql-snappy-messages:

Snappy encoded messages don't decompress
****************************************

If you don't have write access to the ``/tmp`` directory because it's set to
``noexec``, you need to pass in a directory path for ``snappy`` that you have
write access to:

::

    -Dorg.xerial.snappy.tempdir=/path/to/newtmp

.. _ksql-check-server-logs:


Check for message processing failures
*************************************

You can check the health of a KSQL query by viewing the number of messages that
it has processed and counting how many processing failures have occurred.

Use the ``DESCRIBE EXTENDED`` statement to see ``total-messages`` and
``failed-messages-per-sec`` to get message processing metrics. Note that the metrics are local to the server where the DESCRIBE statement runs.

.. code:: sql

    DESCRIBE EXTENDED GOOD_RATINGS;

Example output:

::

    [...]
    Local runtime statistics
    ------------------------
    messages-per-sec:      1.10 total-messages:     2898 last-message: 9/17/18 1:48:47 PM UTC
     failed-messages:         0 failed-messages-per-sec:         0 last-failed: n/a
    (Statistics of the local KSQL server interaction with the Kafka topic GOOD_RATINGS)

An increasing number of ``failed-messages`` may indicate problems with your query.
See :ref:`deserialization errors <ksql-deserialization-errors>` for typical sources of processing failures.


Check the KSQL server logs
**************************

Check the KSQL server logs for errors using the command:

.. code:: bash

    confluent log ksql-server

KSQL writes most of its log messages to ``stdout`` by default.

Look for logs in the default directory at ``/usr/local/logs`` or in the
``LOG_DIR`` that you assigned when starting the CLI. See
:ref:`Starting the KSQL CLI <install_ksql-cli>` for more information.

If you installed the Confluent Platform using RPM or Debian packages, the logs are
in ``/var/log/confluent/``.

If you’re running KSQL using Docker, the output is in the container logs,
for example:

.. code:: bash

    docker logs <container-id>
    docker-compose logs ksql-server