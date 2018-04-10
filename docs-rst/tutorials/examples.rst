.. _ksql_examples:

KSQL Examples
=============

These examples use a ``pageviews`` stream and a ``users`` table.

.. contents:: Contents
    :local:
    :depth: 2


Creating streams
----------------

Prerequisite:
    The corresponding Kafka topics must already exist in your Kafka cluster.

Create a stream with three columns on the Kafka topic that is named ``pageviews``. It is important to instruct KSQL the format
of the values that are stored in the topic. In this example, the values format is ``DELIMITED``.

.. code:: sql

    CREATE STREAM pageviews \
      (viewtime BIGINT, \
       userid VARCHAR, \
       pageid VARCHAR) \
      WITH (KAFKA_TOPIC='pageviews-topic', \
            VALUE_FORMAT='DELIMITED');

**Associating Kafka message keys:** The above statement does not make
any assumptions about the Kafka message key in the underlying Kafka
topic. However, if the value of the message key in Kafka is the same as
one of the columns defined in the stream in KSQL, you can provide such
information in the WITH clause. For instance, if the Kafka message key
has the same value as the ``pageid`` column, you can write the CREATE
STREAM statement as follows:

.. code:: sql

    CREATE STREAM pageviews \
      (viewtime BIGINT, \
       userid VARCHAR, \
       pageid VARCHAR) \
     WITH (KAFKA_TOPIC='pageviews-topic', \
           VALUE_FORMAT='DELIMITED', \
           KEY='pageid');

**Associating Kafka message timestamps:** If you want to use the value
of one of the columns as the Kafka message timestamp, you can provide
such information to KSQL in the WITH clause. The message timestamp is
used in window-based operations in KSQL (such as windowed aggregations)
and to support event-time based processing in KSQL. For instance, if you
want to use the value of the ``viewtime`` column as the message
timestamp, you can rewrite the above statement as follows:

.. code:: sql

    CREATE STREAM pageviews \
      (viewtime BIGINT, \
       userid VARCHAR, \
       pageid VARCHAR) \
      WITH (KAFKA_TOPIC='pageviews-topic', \
            VALUE_FORMAT='DELIMITED', \
            KEY='pageid', \
            TIMESTAMP='viewtime');

Creating tables
---------------

Prerequisite:
    The corresponding Kafka topics must already exist in your Kafka cluster.

Create a table with several columns. In this example, the table has columns with primitive data
types, a column of ``array`` type, and a column of ``map`` type:

.. code:: sql

    CREATE TABLE users \
      (registertime BIGINT, \
       gender VARCHAR, \
       regionid VARCHAR, \
       userid VARCHAR, \
       interests array<VARCHAR>, \
       contact_info map<VARCHAR, VARCHAR>) \
      WITH (KAFKA_TOPIC='users-topic', \
            VALUE_FORMAT='JSON',
            KEY = 'userid');

Note that specifying KEY is required in table declaration, see :ref:`ksql_key_constraints`

Working with streams and tables
-------------------------------

Now that you have the ``pageviews`` stream and ``users`` table, take a
look at some example queries that you can write in KSQL. The focus is on
two types of KSQL statements: CREATE STREAM AS SELECT (a.k.a CSAS) and CREATE TABLE
AS SELECT (a.k.a CTAS). For these statements KSQL persists the results of the query
in a new stream or table, which is backed by a Kafka topic.

Transforming
~~~~~~~~~~~~

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

Joining
~~~~~~~

The following query creates a new stream by joining the
``pageviews_transformed`` stream with the ``users`` table:

.. code:: sql

    CREATE STREAM pageviews_enriched AS \
      SELECT pv.viewtime, \
             pv.userid AS userid, \
             pv.pageid, \
             pv.timestring, \
             u.gender, \
             u.regionid, \
             u.interests, \
             u.contact_info \
      FROM pageviews_transformed pv \
      LEFT JOIN users u ON pv.userid = users.userid;

Note that by default all the Kafka topics will be read from the current
offset (aka the latest available data); however, in a stream-table join,
the table topic will be read from the beginning.

Aggregating, windowing, and sessionization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now assume that you want to count the number of pageviews per region.
Here is the query that would perform this count:

.. code:: sql

    CREATE TABLE pageviews_per_region AS \
      SELECT regionid, \
             count(*) \
      FROM pageviews_enriched \
      GROUP BY regionid;

The above query counts the pageviews from the time you start the query
until you terminate the query. Note that we used CREATE TABLE AS SELECT
statement here since the result of the query is a KSQL table. The
results of aggregate queries in KSQL are always a table because it
computes the aggregate for each key (and possibly for each window per
key) and *updates* these results as it processes new input data.

KSQL supports aggregation over WINDOW too. Let’s rewrite the above query
so that we compute the pageview count per region every 1 minute:

.. code:: sql

    CREATE TABLE pageviews_per_region_per_minute AS \
      SELECT regionid, \
             count(*) \
      FROM pageviews_enriched \
      WINDOW TUMBLING (SIZE 1 MINUTE) \
      GROUP BY regionid;

If you want to count the pageviews for only “Region_6” by female users
for every 30 seconds, you can change the above query as the following:

.. code:: sql

    CREATE TABLE pageviews_per_region_per_30secs AS \
      SELECT regionid, \
             count(*) \
      FROM pageviews_enriched \
      WINDOW TUMBLING (SIZE 30 SECONDS) \
      WHERE UCASE(gender)='FEMALE' AND LCASE(regionid)='region_6' \
      GROUP BY regionid;

UCASE and LCASE functions in KSQL are used to convert the values of
gender and regionid columns to upper and lower case, so that you can
match them correctly. KSQL also supports LIKE operator for prefix,
suffix and substring matching.

KSQL supports HOPPING windows and SESSION windows too. The following
query is the same query as above that computes the count for hopping
window of 30 seconds that advances by 10 seconds:

.. code:: sql

    CREATE TABLE pageviews_per_region_per_30secs10secs AS \
      SELECT regionid, \
             count(*) \
      FROM pageviews_enriched \
      WINDOW HOPPING (SIZE 30 SECONDS, ADVANCE BY 10 SECONDS) \
      WHERE UCASE(gender)='FEMALE' AND LCASE (regionid) LIKE '%_6' \
      GROUP BY regionid;

The next statement counts the number of pageviews per region for session
windows with a session inactivity gap of 60 seconds. In other words, you
are *sessionizing* the input data and then perform the
counting/aggregation step per region.

.. code:: sql

    CREATE TABLE pageviews_per_region_per_session AS \
      SELECT regionid, \
             count(*) \
      FROM pageviews_enriched \
      WINDOW SESSION (60 SECONDS) \
      GROUP BY regionid;

Working with arrays and maps
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``interests`` column in the ``users`` table is an ``array`` of
strings that represents the interest of each user. The ``contact_info``
column is a string-to-string ``map`` that represents the following
contact information for each user: phone, city, state, and zipcode.

The following query will create a new stream from ``pageviews_enriched``
that includes the first interest of each user along with the city and
zipcode for each user:

.. code:: sql

    CREATE STREAM pageviews_interest_contact AS \
      SELECT interests[0] AS first_interest, \
             contact_info['zipcode'] AS zipcode, \
             contact_info['city'] AS city, \
             viewtime, \
             userid, \
             pageid, \
             timestring, \
             gender, \
             regionid \
      FROM pageviews_enriched;

.. _running-ksql-command-line:

Running Single KSQL Statements From the Command Line
----------------------------------------------------

In addition to using the KSQL CLI or launching KSQL servers with the ``--queries-file`` configuration, you can also execute
KSQL statements from directly your terminal. This can be useful for scripting.

The following examples show common usage:

-   This example uses the Bash `here document <http://tldp.org/LDP/abs/html/here-docs.html>`__ (``<<``) to run KSQL CLI commands.

    .. code:: bash

        $ ksql <<EOF
        > SHOW TOPICS;
        > SHOW STREAMS;
        > exit
        > EOF

-   This example uses a Bash `here string <http://tldp.org/LDP/abs/html/x17837.html>`__ (``<<<``) to run KSQL CLI commands on
    an explicitly defined KSQL server endpoint.

    .. code:: bash

        $ ksql http://localhost:8088 <<< "SHOW TOPICS;
        SHOW STREAMS;
        exit"

-   This example creates a stream from a predefined script (``application.sql``) using the ``RUN SCRIPT`` command and
    then runs a query by using the Bash `here document <http://tldp.org/LDP/abs/html/here-docs.html>`__ (``<<``) feature.

    .. code:: bash

        $ cat /path/to/local/application.sql
        CREATE STREAM pageviews_copy AS SELECT * FROM pageviews;

    .. code:: bash

        $ ksql http://localhost:8088 <<EOF
        > RUN SCRIPT '/path/to/local/application.sql';
        > exit
        > EOF

    .. note:: The ``RUN SCRIPT`` command only supports a subset of KSQL CLI commands, including running DDL statements
              (CREATE STREAM, CREATE TABLE), persistent queries (CREATE STREAM AS SELECT, CREATE TABLE AS SELECT), and
              setting configuration options (SET statement). Other statements and commands such as ``SHOW TOPICS``and
              ``SHOW STREAMS`` will be ignored.
