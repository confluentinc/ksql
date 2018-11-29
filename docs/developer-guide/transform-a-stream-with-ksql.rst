.. _transform-a-stream-with-ksql:

Transform a Stream With KSQL 
############################

KSQL enables *streaming transformations*, which you can use to convert 
streaming data from one format to another in real time. With a streaming
transformation, not only is every record that arrives on the source stream
converted, but you can configure KSQL so that all previously existing records
in the stream are converted.

.. include:: ../includes/ksql-includes.rst
    :start-after: offsetreset_start
    :end-before: offsetreset_end

Transform a Stream By Using the WITH Clause
*********************************************

These are the aspects of a stream that you can change when you transform
to a new stream:

* The data format for message values
* The number of partitions
* The number of replicas
* The timestamp field and/or the timestamp format
* The new stream's underlying Kafka topic name

For this example, imagine that you want to create a new stream by
transforming a ``pageviews`` stream in the following way:

*  The ``viewtime`` column value is used as the record timestamp
   in the new stream’s underlying Kafka topic.
*  The new stream’s Kafka topic has five partitions.
*  The data in the new stream is in JSON format.
*  A new column is added that shows the message timestamp in human-readable
   string format.
*  The ``userid`` column is the key for the new stream.

The following statement generates a new stream, named
``pageviews_transformed``, that has the specified properties:

.. code:: sql

    CREATE STREAM pageviews_transformed \
      WITH (TIMESTAMP='viewtime', \
            PARTITIONS=5, \
            VALUE_FORMAT='JSON') AS \
      SELECT viewtime, \
             userid, \
             pageid, \
             TIMESTAMPTOSTRING(viewtime, 'yyyy-MM-dd HH:mm:ss.SSS') AS timestring \
      FROM pageviews \
      PARTITION BY userid;

Content-based Routing
********************* 

Frequently, you need to route messages from a source stream to multiple
destination streams, based on conditions in the data. This is
*content-based routing* or *data routing*.

Use the WHERE clause to select a subset of data. To route streams with different
criteria to other streams that are backed by different underlying Kafka topics,
write multiple KSQL queries with different WHERE clauses.

In this example, two streams are derived from a ``pageviews`` stream, both with
different users selected into the output.

.. code:: sql

    CREATE STREAM pageviews_for_first_two_users AS \
      SELECT viewtime, \
             userid, \
             pageid \
      FROM pageviews \
      WHERE userid='User_1' OR userid='User_2' \
      PARTITION BY userid;

.. code:: sql

    CREATE STREAM pageviews_for_other_users AS \
      SELECT viewtime, \
             userid, \
             pageid \
      FROM pageviews \
      WHERE userid<>'User_1' AND userid<>'User_2' \
      PARTITION BY userid;

Next Steps
**********

Here are some examples of useful streaming transformations in the
`Stream Processing Cookbook <https://www.confluent.io/stream-processing-cookbook>`__:

* `Data Routing <https://www.confluent.io/stream-processing-cookbook/ksql-recipes/data-routing>`__
* `Changing Data Serialization Format from Avro to CSV <https://www.confluent.io/stream-processing-cookbook/ksql-recipes/changing-data-serialization-format-avro-csv>`__
* `Changing Data Serialization Format from JSON to Avro <https://www.confluent.io/stream-processing-cookbook/ksql-recipes/changing-data-serialization-format-json-avro>`__
* `Changing Data Serialization Format from Delimited (CSV) to Avro <https://www.confluent.io/stream-processing-cookbook/ksql-recipes/changing-data-serialization-format-delimited-csv-avro>`__