.. _ksql_key_constraints:

============
Message Keys
============

The ``CREATE STREAM`` and ``CREATE TABLE`` statements, which read data from a Kafka topic into a stream or table, allow you to specify a field/column in the Kafka message value that corresponds to the Kafka message key by setting the ``KEY`` property of the ``WITH`` clause.

Example:

.. code:: sql

    CREATE TABLE users (registertime BIGINT, gender VARCHAR, regionid VARCHAR, userid VARCHAR)
      WITH (KAFKA_TOPIC='users', VALUE_FORMAT='JSON', KEY = 'userid');


The ``KEY`` property is:

- Required for tables.
- Optional for streams. Here, KSQL uses it as an optimization hint to determine if repartitioning can be avoided when performing aggregations and joins.

In either case, when setting ``KEY`` you must be sure that *both* of the following conditions are true:

1. For every record, the contents of the Kafka message key must be the same as the contents of the columm set in ``KEY`` (which is derived from a field in the Kafka message value).
2. ``KEY`` must be set to a column of type ``VARCHAR`` aka ``STRING``.

If these conditions are not met, then the results of aggregations and joins may be incorrect. However, if your data doesn't meet these requirements, you can still use KSQL with a few extra steps. The following section explains how.

=============================================================
What To Do If Your Key Is Not Set or Is In A Different Format
=============================================================

Streams
-------

For streams, just leave out the ``KEY`` property from the ``WITH`` clause. KSQL will take care of repartitioning the stream for you using the value(s) from the ``GROUP BY`` columns for aggregates, and the join predicate for joins.

Tables
------

For tables, you can still use KSQL if the message key is not also present in the Kafka message value or if it is not in
the required format as long as *one* of the following statements is true:

- The message key is a `unary function <https://en.wikipedia.org/wiki/Unary_function>`__ of the value in the desired key
  column.
- It is ok for the messages in the topic to be re-ordered before being inserted into the table.

First create a stream to have KSQL write the message key, and then declare the table on the output topic of this stream:

Example:

- Goal: You want to create a table from a topic, which is keyed by userid of type INT.
- Problem: The message key is present as a field/column (aptly named userid) in the message value but in the wrong
  format (INT instead of VARCHAR).

.. code:: sql

    -- Create a stream on the original topic
    CREATE STREAM users_with_wrong_key_format (userid INT, username VARCHAR, email VARCHAR)
      WITH (KAFKA_TOPIC='users', VALUE_FORMAT='JSON');

    -- Derive a new stream with the required key changes.
    -- 1) The CAST statement converts the key to the required format.
    -- 2) The PARTITION BY clause re-partitions the stream based on the new, converted key.
    CREATE STREAM users_with_proper_key
      WITH(KAFKA_TOPIC='users-with-proper-key') AS
      SELECT CAST(userid as VARCHAR) as userid_string, username, email
      FROM users_with_wrong_key_format
      PARTITION BY userid_string;

    -- Now you can create the table on the properly keyed stream.
    CREATE TABLE users_table (userid_string VARCHAR, username VARCHAR, email VARCHAR)
      WITH (KAFKA_TOPIC='users-with-proper-key',
            VALUE_FORMAT='JSON',
            KEY='userid_string');


Example:

- Goal: You want to create a table from a topic, which is keyed by userid of type INT.
- Problem: The message key is not present as a field/column in the topic's message values.

.. code:: sql

    -- Create a stream on the original topic.
    -- The topic is keyed by userid, which is available as the implicit column ROWKEY
    -- in the `users_with_missing_key` stream. Note how the explicit column definitions
    -- only define username and email but not userid.
    CREATE STREAM users_with_missing_key (username VARCHAR, email VARCHAR)
      WITH (KAFKA_TOPIC='users', VALUE_FORMAT='JSON');

    -- Derive a new stream with the required key changes.
    -- 1) The contents of ROWKEY (message key) are copied into the message value as the userid_string column,
    --    and the CAST statement converts the key to the required format.
    -- 2) The PARTITION BY clause re-partitions the stream based on the new, converted key.
    CREATE STREAM users_with_proper_key
      WITH(KAFKA_TOPIC='users-with-proper-key') AS
      SELECT CAST(ROWKEY as VARCHAR) as userid_string, username, email
      FROM users_with_missing_key
      PARTITION BY userid_string;

    -- Now you can create the table on the properly keyed stream.
    CREATE TABLE users_table (userid_string VARCHAR, username VARCHAR, email VARCHAR)
      WITH (KAFKA_TOPIC='users-with-proper-key',
            VALUE_FORMAT='JSON',
            KEY='userid_string');

