.. _ksql_key_constraints:

============
Message Keys
============

The `CREATE STREAM` and `CREATE TABLE` statements allow the user to specify the column that corresponds to the Kafka message key by setting the `KEY` property of the `WITH` clause. For example:

.. code:: sql

    CREATE TABLE users (registertime bigint, gender varchar, regionid varchar, userid varchar) WITH (KAFKA_TOPIC=‘users', VALUE_FORMAT=‘JSON', KEY = 'userid');


In the case of tables, the `KEY` property is required.

In the case of streams, the `KEY` property is optional. KSQL uses it as a hint to determine if a repartition can be avoided when performing aggregations and joins.

In either case, when setting `KEY` the user must be sure that *both* of the following statements are true:

    - `KEY` must be set to a column of type `STRING` or `VARCHAR`.
    - The value in the message key must be the same as the value in the column set in `KEY` for every record.

If those constraints are not met, then the results of aggregation and join queries may be incorrect. However, if your data doesn't meet these requirements, you can still use KSQL with a couple extra steps. The following section explains how.

=============================================================
What To Do If Your Key Is Not Set or Is In A Different Format
=============================================================

Streams
-------

For streams, just leave out the `KEY` property from the `WITH` clause. KSQL will take care of repartitioning the stream for you using the value(s) from the `GROUP BY` columns for aggregates, and the join predicate for joins.

Tables
------

For tables, you can still use KSQL if the record key is not in the required format as long as *one* of the following statements is true:

    - the record key is a unary function of the value in the desired key column.
    - it is ok for the records in the topic to be reordered before being inserted into the table

To do so, first create a stream to have KSQL write the record key, and then declare the table on the output topic of the stream:

.. code:: sql

    CREATE STREAM users_stream (userid INT, username STRING, city STRING, email STRING) WITH (KAFKA_TOPIC=‘users’, VALUE_FORMAT=‘JSON’);
    CREATE STREAM users_with_key WITH(KAFKA_TOPIC=‘users_with_key’) AS SELECT CAST(userid as STRING) as userid_string, username, city, email FROM users_stream PARTITION BY userid_string;
    CREATE TABLE users_table (userid_string STRING, username STRING, city STRING, email STRING) WITH (KAFKA_TOPIC=‘users_with_key’, VALUE_FORMAT=‘JSON’, KEY=‘userid_string’);

