.. _transform-a-stream-with-ksql:

Transform a KSQL Stream
#######################

For this example, imagine you want to create a new stream by
transforming ``pageviews`` in the following way:

-  The ``viewtime`` column value is used as the Kafka message timestamp
   in the new stream’s underlying Kafka topic.
-  The new stream’s Kafka topic has 5 partitions.
-  The data in the new stream is in JSON format.
-  Add a new column that shows the message timestamp in human-readable
   string format.
-  The ``userid`` column is the key for the new stream.

The following statement will generate a new stream,
``pageviews_transformed`` with the above properties:

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

Use a ``[ WHERE condition ]`` clause to select a subset of data. If you
want to route streams with different criteria to different streams
backed by different underlying Kafka topics, e.g. content-based routing,
write multiple KSQL statements as follows:

.. code:: sql

    CREATE STREAM pageviews_transformed_priority_1 \
      WITH (TIMESTAMP='viewtime', \
            PARTITIONS=5, \
            VALUE_FORMAT='JSON') AS \
      SELECT viewtime, \
             userid, \
             pageid, \
             TIMESTAMPTOSTRING(viewtime, 'yyyy-MM-dd HH:mm:ss.SSS') AS timestring \
      FROM pageviews \
      WHERE userid='User_1' OR userid='User_2' \
      PARTITION BY userid;

.. code:: sql

    CREATE STREAM pageviews_transformed_priority_2 \
          WITH (TIMESTAMP='viewtime', \
                PARTITIONS=5, \
                VALUE_FORMAT='JSON') AS \
      SELECT viewtime, \
             userid, \
             pageid, \
             TIMESTAMPTOSTRING(viewtime, 'yyyy-MM-dd HH:mm:ss.SSS') AS timestring \
      FROM pageviews \
      WHERE userid<>'User_1' AND userid<>'User_2' \
      PARTITION BY userid;