.. _join-streams-and-tables:

Join Event Streams with KSQL
############################

You can use KSQL to merge streams of data in real time by using a SQL-like
*join* syntax. A KSQL join and a relational database join are similar in that
they both combine data from two sources based on common values. The result of
a KSQL join is a new stream or table that's populated with the column values
that you specify in a SELECT statement.

With KSQL, you don’t need to write the low-level logic around joining streams
and tables, so you can focus on the business logic for combining your streaming
data.

You can join streams and tables in these ways:

* Join two streams to create a new stream.
* Join two tables to create a new table.
* Join a stream and a table to create a new stream.

JOIN Clause
***********

The KSQL JOIN clause has the familiar syntax of a SQL JOIN clause.
The following example creates a ``pageviews_enriched`` stream, which is a
combination of a ``pageviews`` stream and a ``users`` table:

.. code:: sql

   CREATE STREAM pageviews_enriched AS
     SELECT users.userid AS userid, pageid, regionid, gender FROM pageviews
     LEFT JOIN users ON pageviews.userid = users.userid;


For the full code example, see :ref:`ksql_quickstart-docker`.

Here's an example stream-stream join that combines a ``shipments`` stream with
an ``orders`` stream, within a time window. The resulting ``late_orders`` stream
detects late orders by matching ``shipments`` rows with ``orders`` rows that
occur within a two-hour window. If there's no match, the right-hand side of the
join result is NULL, which indicates that the order wasn't shipped within the
expected time of two hours.

.. code:: sql

   CREATE STREAM late_orders AS
     SELECT o.orderid, o.itemid FROM orders o
     FULL OUTER JOIN shipments s WITHIN 2 HOURS
     ON s.orderid = o.orderid WHERE s.orderid IS NULL;

Joins and Windows
*****************

KSQL enables grouping records that have the same key for stateful operations,
like joins, into *windows*. You specify a retention period for the window, and
this retention period controls how long KSQL will wait for out-of-order and
late-arriving records. If a record arrives after the window’s retention period
has passed, the record is discarded and isn’t processed in that window.

Only stream-stream joins are windowed.

Windows are tracked per record key. In join operations, KSQL uses a windowing
*state store* to store all of the records received so far within the defined
window boundary. Old records in the state store are purged after the specified
window retention period.

For more information on windows, see :ref:`windows_in_ksql_queries`.

Join Requirements
*****************

Your KSQL applications must meet specific requirements for joins to be successful. 

Co-partitioned data
    Input data must be co-partitioned when joining. This ensures that input
    records with the same key, from both sides of the join, are delivered to
    the same stream task during processing. It’s your responsibility to ensure
    data co-partitioning when joining. For more information, see :ref:`partition-data-to-enable-joins`.

KEY property
    If you set the KEY property when you create a table, ensure that both of the
    following conditions are true:

    * For every record, the contents of the message key of the |ak-tm| message itself must be
      the same as the contents of the column set in KEY.
    * The KEY property must be set to a column of type VARCHAR or STRING.

    For more information, see :ref:`ksql_key_requirements`.

Join Capabilities
*****************

KSQL supports a large set of join operations for streams and tables, including
INNER, LEFT OUTER, and FULL OUTER. Frequently, LEFT OUTER is shortened to LEFT JOIN,
and FULL OUTER is shortened to OUTER JOIN.

.. note:: RIGHT OUTER JOIN isn’t supported. Instead, swap the operands and use LEFT JOIN.

The following table shows the supported combinations.

+---------------+--------------+-----------+------------+---------------+
|               | Type         | INNER     | LEFT OUTER | FULL OUTER    |
+===============+==============+===========+============+===============+
| Stream-Stream | Windowed     | Supported | Supported  | Supported     |                                         
+---------------+--------------+-----------+------------+---------------+
| Table-Table   | Non-windowed | Supported | Supported  | Supported     |
+---------------+--------------+-----------+------------+---------------+
| Stream-Table  | Non-windowed | Supported | Supported  | Not supported |
+---------------+--------------+-----------+------------+---------------+

Stream-Stream Joins
*******************

KSQL supports INNER, LEFT OUTER, and FULL OUTER joins between streams.

All of these operations support out-of-order records.

Joins between streams are always windowed joins. A new input record on one side
produces a join output for each matching record on the other side, and there
can be multiple such matching records within a join window.

Joins cause data re-partitioning of a stream only if the stream was marked
for re-partitioning. If both streams are marked, both are re-partitioned.

LEFT OUTER joins will contain leftRecord-NULL records in the result stream,
which means that the join contains NULL values for fields selected from the
right-hand stream where no match is made.

FULL OUTER joins will contain leftRecord-NULL or NULL-rightRecord records in
the result stream, which means that the join contains NULL values for fields
coming from a stream where no match is made.

Semantics of Stream-Stream Joins
================================

The semantics of the various stream-stream join variants are shown in the
following table. In the table, each row represents a new incoming record.
The following assumptions apply: 

* All records have the same key. 
* All records belong to a single join window.
* All records are processed in timestamp order.

When new input is received, the join is triggered under the conditions listed
in the table. Input records with a NULL key or a NULL value are ignored and
don’t trigger the join.

+-----------+---------------+---------------+--------------------------------+--------------------------------+--------------------------------+
| Timestamp | Left Stream   | Right Stream  | INNER JOIN                     | LEFT JOIN                      | OUTER JOIN                     |
+===========+===============+===============+================================+================================+================================+
|  1        | null          |               |                                |                                |                                |
+-----------+---------------+---------------+--------------------------------+--------------------------------+--------------------------------+
|  2        |               | null          |                                |                                |                                |
+-----------+---------------+---------------+--------------------------------+--------------------------------+--------------------------------+
|  3        | A             |               |                                | [A, null]                      | [A, null]                      |
+-----------+---------------+---------------+--------------------------------+--------------------------------+--------------------------------+
|  4        |               | a             | [A, a]                         | [A, a]                         | [A, a]                         |
+-----------+---------------+---------------+--------------------------------+--------------------------------+--------------------------------+
|  5        | B             |               | [B, a]                         | [B, a]                         | [B, a]                         |
+-----------+---------------+---------------+--------------------------------+--------------------------------+--------------------------------+
|  6        |               | b             | [A, b], [B, b]                 | [A, b], [B, b]                 | [A, b], [B, b]                 |
+-----------+---------------+---------------+--------------------------------+--------------------------------+--------------------------------+
|  7        | null          |               |                                |                                |                                |
+-----------+---------------+---------------+--------------------------------+--------------------------------+--------------------------------+
|  8        |               | null          |                                |                                |                                |
+-----------+---------------+---------------+--------------------------------+--------------------------------+--------------------------------+
|  9        | C             |               | [C, a], [C, b]                 | [C, a], [C, b]                 | [C, a], [C, b]                 |
+-----------+---------------+---------------+--------------------------------+--------------------------------+--------------------------------+
| 10        |               | c             | [A, c], [B, c], [C, c]         | [A, c], [B, c], [C, c]         | [A, c], [B, c], [C, c]         |
+-----------+---------------+---------------+--------------------------------+--------------------------------+--------------------------------+
| 11        |               | null          |                                |                                |                                |
+-----------+---------------+---------------+--------------------------------+--------------------------------+--------------------------------+
| 12        | null          |               |                                |                                |                                |
+-----------+---------------+---------------+--------------------------------+--------------------------------+--------------------------------+
| 13        |               | null          |                                |                                |                                |
+-----------+---------------+---------------+--------------------------------+--------------------------------+--------------------------------+
| 14        |               | d             | [A, d], [B, d], [C, d]         | [A, d], [B, d], [C, d]         | [A, d], [B, d], [C, d]         |
+-----------+---------------+---------------+--------------------------------+--------------------------------+--------------------------------+
| 15        | D             |               | [D, a], [D, b], [D, c], [D, d] | [D, a], [D, b], [D, c], [D, d] | [D, a], [D, b], [D, c], [D, d] |
+-----------+---------------+---------------+--------------------------------+--------------------------------+--------------------------------+

Stream-Table Joins
******************

KSQL only supports INNER and LEFT joins between a stream and a table.

Stream-table joins are always non-windowed joins. You can perform table lookups
against a table when a new record arrives on the stream. Only events arriving on
the stream side trigger downstream updates and produce join output. Updates on
the table side don’t produce updated join output.

Out-of-order records aren't supported, which means that KSQL processes all records
in offset order and doesn't check for out-of-order records.

Stream-table joins cause data re-partitioning of the stream only if the stream
was marked for re-partitioning.

.. important:: KSQL currently provides best-effort on time synchronization,
               but there are no guarantees, which can cause missing results
               or leftRecord-NULL results.

Semantics of Stream-Table Joins
===============================

The semantics of the various stream-table join variants are shown in the
following table. In the table, each row represents a new incoming record.
The following assumptions apply: 

* All records have the same key. 
* All records are processed in timestamp order.

Input records for the stream with a NULL key or a NULL value are ignored and
don’t trigger the join.

Only input records for the left-side stream trigger the join. Input records for
the right-side table update only the internal right-side join state.

Input records for the table with a NULL value are interpreted as *tombstones*
for the corresponding key, which indicate the deletion of the key from the table.
Tombstones don’t trigger the join.

+-----------+--------------+------------------+--------------+------------+
| Timestamp | Left Stream  | Right Table      | INNER JOIN   | LEFT JOIN  |
+===========+==============+==================+==============+============+
|  1        | null         |                  |              |            |
+-----------+--------------+------------------+--------------+------------+
|  2        |              | null (tombstone) |              |            |
+-----------+--------------+------------------+--------------+------------+
|  3        | A            |                  |              | [A, null]  |
+-----------+--------------+------------------+--------------+------------+
|  4        |              | a                |              |            |
+-----------+--------------+------------------+--------------+------------+
|  5        | B            |                  | [B, a]       | [B, a]     |
+-----------+--------------+------------------+--------------+------------+
|  6        |              | b                |              |            |
+-----------+--------------+------------------+--------------+------------+
|  7        | null         |                  |              |            |
+-----------+--------------+------------------+--------------+------------+
|  8        |              | null (tombstone) |              |            |
+-----------+--------------+------------------+--------------+------------+
|  9        | C            |                  |              | [C, null]  |
+-----------+--------------+------------------+--------------+------------+
| 10        |              | c                |              |            |
+-----------+--------------+------------------+--------------+------------+
| 11        |              | null             |              |            |
+-----------+--------------+------------------+--------------+------------+
| 12        | null         |                  |              |            |
+-----------+--------------+------------------+--------------+------------+
| 13        |              | null             |              |            |
+-----------+--------------+------------------+--------------+------------+
| 14        |              | d                |              |            |
+-----------+--------------+------------------+--------------+------------+
| 15        | D            |                  | [D, d]       | [D, d]     |
+-----------+--------------+------------------+--------------+------------+

For stream-table joins, KSQL assumes that the joining stream and table follow
the event-time ordering exactly. Follow these steps to ensure that joins are
synchronized:

#. Start the query, which starts consumers.
#. Populate the table completely. This ensures that the table items exist when
   the stream events come in to trigger the join.
#. Populate the stream completely.

Table-Table Joins
*****************

KSQL supports INNER, LEFT OUTER, and FULL OUTER joins between tables. Joins
matching multiple records (one-to-many) aren't supported.

Table-table joins are always non-windowed joins. 

Out-of-order records are not supported, which means that KSQL processes all
records in offset order and does not check for out-of-order records.

Table-table joins are eventually consistent.

.. important:: KSQL currently provides best-effort on time synchronization,
               but there are no guarantees, which can cause missing results
               or leftRecord-NULL results.

Semantics of Table-Table Joins
==============================

The semantics of the various table-table join variants are shown in the
following table. In the table, each row represents a new incoming record.
The following assumptions apply: 

* All records have the same key.
* All records are processed in timestamp order.

Input records with a NULL value are interpreted as tombstones for the
corresponding key, which indicate the deletion of the key from the table.
Tombstones don’t trigger the join. When an input tombstone is received, an output
tombstone is forwarded directly to the join result table, if the corresponding
key exists already in the join result table.

+-----------+------------------+------------------+-------------------+------------------+------------------+
| Timestamp | Left Table       | Right Table      | INNER JOIN        | LEFT JOIN        | OUTER JOIN       |
+===========+==================+==================+===================+==================+==================+
|  1        | null (tombstone) |                  |                   |                  |                  |
+-----------+------------------+------------------+-------------------+------------------+------------------+
|  2        |                  | null (tombstone) |                   |                  |                  |
+-----------+------------------+------------------+-------------------+------------------+------------------+
|  3        | A                |                  |                   | [A, null]        | [A, null]        |
+-----------+------------------+------------------+-------------------+------------------+------------------+
|  4        |                  | a                | [A, a]            | [A, a]           | [A, a]           |
+-----------+------------------+------------------+-------------------+------------------+------------------+
|  5        | B                |                  | [B, a]            | [B, a]           | [B, a]           |
+-----------+------------------+------------------+-------------------+------------------+------------------+
|  6        |                  | b                | [B, b]            | [B, b]           | [B, b]           |
+-----------+------------------+------------------+-------------------+------------------+------------------+
|  7        | null (tombstone) |                  | null (tombstone)  | null (tombstone) | [null, b]        |
+-----------+------------------+------------------+-------------------+------------------+------------------+
|  8        |                  | null (tombstone) |                   |                  | null (tombstone) |
+-----------+------------------+------------------+-------------------+------------------+------------------+
|  9        | C                |                  |                   | [C, null]        | [C, null]        |
+-----------+------------------+------------------+-------------------+------------------+------------------+
| 10        |                  | c                | [C, c]            | [C, c]           | [C, c]           |
+-----------+------------------+------------------+-------------------+------------------+------------------+
| 11        |                  | null (tombstone) | null (tombstone)  | [C, null]        | [C, null]        |
+-----------+------------------+------------------+-------------------+------------------+------------------+
| 12        | null (tombstone) |                  |                   | null (tombstone) | null (tombstone) |
+-----------+------------------+------------------+-------------------+------------------+------------------+
| 13        |                  | null (tombstone) |                   |                  |                  |
+-----------+------------------+------------------+-------------------+------------------+------------------+
| 14        |                  | d                |                   |                  | [null, d]        |
+-----------+------------------+------------------+-------------------+------------------+------------------+
| 15        | D                |                  | [D, d]            | [D, d]           | [D, d]           |
+-----------+------------------+------------------+-------------------+------------------+------------------+



