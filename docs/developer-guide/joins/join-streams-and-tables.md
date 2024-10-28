---
layout: page
title: Join Event Streams
tagline: Merge event streams in real time
description: Learn how to use ksqlDB to merge streams of events in real time.
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/joins/join-streams-and-tables.html';
</script>

You can use ksqlDB to merge streams of events in real time by using the JOIN
statement, which has a SQL join syntax. A ksqlDB join and a relational
database join are similar in that they both combine data from two or more sources
based on common values. The result of a ksqlDB join is a new stream or table
that's populated with the column values that you specify in a SELECT statement.

With ksqlDB, you don't need to write the low-level logic around joining
streams and tables, so you can focus on the business logic for combining
your streaming data.

You can join streams and tables in these ways:

-   Join multiple streams to create a new stream.
-   Join multiple tables to create a new table.
-   Join multiple streams and tables to create a new stream.

JOIN Clause
-----------

The ksqlDB JOIN clause has the familiar syntax of a SQL JOIN clause. The
following example creates a `pageviews_enriched` stream, which is a
combination of a `pageviews` stream and a `users` table:

```sql
CREATE STREAM pageviews_enriched AS
  SELECT 
     users.userid AS userid, 
     pageid, 
     regionid, 
     gender 
  FROM pageviews
    LEFT JOIN users ON pageviews.userid = users.userid
  EMIT CHANGES;
```

!!! note
    Currently, the join expression must be a single column equal comparison.
    Non-equi joins, or joins on multiple columns are not supported. 

When you join two streams, you must specify a WITHIN clause for matching
records that both occur within a specified time interval. For valid time
units, see [Time Units](/reference/sql/time/#time-units).

Here's an example stream-stream-stream join that combines `orders`, `payments` and `shipments` 
streams. The resulting ``shipped_orders`` stream contains all orders paid within 1 hour of when
the order was placed, and shipped within 2 hours of the payment being received. 

```sql
   CREATE STREAM shipped_orders AS
     SELECT 
        o.id as orderId,
        o.itemid as itemId,
        s.id as shipmentId,
        p.id as paymentId
     FROM orders o
        INNER JOIN payments p WITHIN 1 HOURS ON p.id = o.id
        INNER JOIN shipments s WITHIN 2 HOURS ON s.id = o.id;
```

Joins and Windows
-----------------

ksqlDB enables grouping records that have the same key for stateful operations,
like joins, into *windows*. A window has a _grace period_, which means that a
record can arrive out-of-order for a specified period of time, and it's still
joined correctly based on its timestamp.

If a record arrives after the window's grace period has passed, the record is
discarded and isn't processed.

For more information on grace periods, see
[Out-of-order events](../../concepts/time-and-windows-in-ksqldb-queries.md#out-of-order-events).

!!! note
    Only stream-stream joins are windowed.

Windows are tracked per record key. In join operations, ksqlDB uses a
windowing *state store* to store all of the records received so far
within the defined window boundary. Old records in the state store are
purged after the window grace period passed.

For more information on windows, see
[Windows in ksqlDB Queries](../../concepts/time-and-windows-in-ksqldb-queries.md#windows-in-sql-queries).

Join Requirements
-----------------

Your ksqlDB applications must meet specific requirements for joins to be
successful.

### Co-partitioned data

For most joins, input data must be co-partitioned when joining. This ensures that
input records with the same key, from both sides of the join, are
delivered to the same stream task during processing. For some cases, ksqlDB
may repartition the data automatically, if required. But there are some cases
for which it's your responsibility to ensure data co-partitioning when joining. The only
join that does not require co-partitioning is a foreign-key table-table join. For more
information, see [Partition Data to Enable Joins](partition-data.md).

Join Capabilities
-----------------

ksqlDB supports a large set of join operations for streams and tables,
including INNER, LEFT OUTER, and FULL OUTER. Frequently, LEFT OUTER is
shortened to LEFT JOIN, and FULL OUTER is shortened to OUTER JOIN.

The following table shows the supported combinations.

|               | Type         | INNER     | LEFT OUTER | RIGHT OUTER   | FULL OUTER    |
|---------------|--------------|-----------|------------|---------------|---------------|
| Stream-Stream | Windowed     | Supported | Supported  | Supported     | Supported     |
| Table-Table   | Non-windowed | Supported | Supported  | Supported     | Supported     |
| Stream-Table  | Non-windowed | Supported | Supported  | Not Supported | Not Supported |

Stream-Stream Joins
-------------------

ksqlDB supports INNER, LEFT OUTER, RIGHT OUTER, and FULL OUTER joins between streams.

All of these operations support out-of-order records.

To join two streams, you must specify a windowing scheme by using the
WITHIN clause. A new input record on one side produces a join output for
each matching record on the other side, and there can be multiple such
matching records within a join window.

Joins cause data re-partitioning of a stream only if the stream was
marked for re-partitioning. If both streams are marked, both are
re-partitioned.

!!! important
    {{ site.ak }} guarantees the relative order of any two messages from
    one source partition only if they are also both in the same partition
    *after* the repartition. Otherwise, {{ site.ak }} is likely to interleave
    messages. The use case will determine if these ordering guarantees are
    acceptable.

LEFT OUTER joins will contain leftRecord-NULL records in the result
stream, which means that the join contains NULL values for fields
selected from the right-hand stream where no match is made.

RIGHT OUTER joins will contain rightRecord-NULL records in the result
stream, which means that the join contains NULL values for fields
selected from the left-hand stream where no match is made.

FULL OUTER joins will contain leftRecord-NULL or NULL-rightRecord
records in the result stream, which means that the join contains NULL
values for fields coming from a stream where no match is made.

### Semantics of Stream-Stream Joins

The semantics of the various stream-stream join variants are shown in
the following table. In the table, each row represents a new incoming
record. The following assumptions apply:

-   All records have the same key.
-   All records are processed in timestamp order.
-   The join window is 15 seconds, and the grace period is 5 seconds.

When new input is received, the join is triggered under the conditions
listed in the table. Input records with a NULL key or a NULL value are
ignored and don't trigger the join.

!!! important
    If you don't specify a grace period, left/outer join results are emitted eagerly,
    and the observed result might differ from the result shown below.

| Timestamp | Left Stream | Right Stream | INNER JOIN                     | LEFT JOIN                      | FULL JOIN                      | OUTER JOIN                     |
|-----------|-------------|--------------|--------------------------------|--------------------------------|--------------------------------|--------------------------------|
| 1         | null        |              |                                |                                |                                |                                |
| 2         |             | null         |                                |                                |                                |                                |
| 3         | A           |              |                                |                                |                                |                                |
| 4         |             | a            | [A, a]                         | [A, a]                         | [A, a]                         | [A, a]                         |
| 5         | B           |              | [B, a]                         | [B, a]                         | [B, a]                         | [B, a]                         |
| 6         |             | b            | [A, b], [B, b]                 | [A, b], [B, b]                 | [A, b], [B, b]                 | [A, b], [B, b]                 |
| 7         | null        |              |                                |                                |                                |                                |
| 8         |             | null         |                                |                                |                                |                                |
| 9         | C           |              | [C, a], [C, b]                 | [C, a], [C, b]                 | [C, a], [C, b]                 | [C, a], [C, b]                 |
| 10        |             | c            | [A, c], [B, c], [C, c]         | [A, c], [B, c], [C, c]         | [A, c], [B, c], [C, c]         | [A, c], [B, c], [C, c]         |                                |
| 11        |             | null         |                                |                                |                                |                                |
| 12        | null        |              |                                |                                |                                |                                |
| 13        |             | null         |                                |                                |                                |                                |
| 14        |             | d            | [A, d], [B, d], [C, d]         | [A, d], [B, d], [C, d]         | [A, d], [B, d], [C, d]         | [A, d], [B, d], [C, d]         |
| 15        | D           |              | [D, a], [D, b], [D, c], [D, d] | [D, a], [D, b], [D, c], [D, d] | [D, a], [D, b], [D, c], [D, d] | [D, a], [D, b], [D, c], [D, d] |
| ...       |             |              |                                |                                |                                |                                |
| 40        | E           |              |                                |                                |                                |                                |
| ...       |             |              |                                |                                |                                |                                |
| 60        | F           |              |                                | [E,null]                       |                                | [E,null]                       |
| ...       |             |              |                                |                                |                                |                                |
| 80        |             | f            |                                | [F,null]                       | [F,null]                       | [F,null]                       |
| ...       |             |              |                                |                                |                                |                                |
| 100       | G           |              |                                |                                | [null,f]                       | [null,f]                       |

Stream-Table Joins
------------------

ksqlDB supports INNER and LEFT joins between a stream and a table.
An OUTER join is not available because it can't be defined with sound semantics.

Stream-table joins are always non-windowed joins. You can perform table
lookups against a table when a new record arrives on the stream. Only
events arriving on the stream side trigger downstream updates and
produce join output. Updates on the table side don't produce updated
join output.

Stream-table joins cause data re-partitioning of the stream only if the
stream was marked for re-partitioning.

!!! important
      ksqlDB currently provides best-effort on time synchronization, but there
      are no guarantees, which can cause missing results or leftRecord-NULL
      results.

### Semantics of Stream-Table Joins

The semantics of the various stream-table join variants are shown in the
following table. In the table, each row represents a new incoming
record. The following assumptions apply:

-   All records have the same key.
-   All records are processed in timestamp order.

Only input records for the left-side stream trigger the join. Input
records for the right-side table update only the internal right-side
join state.

Input records for the table with a NULL value are interpreted as
*tombstones* for the corresponding key, which indicate the deletion of
the key from the table. Tombstones don't trigger the join.

| Timestamp | Left Stream | Right Table      | INNER JOIN | LEFT JOIN |
|-----------|-------------|------------------|------------|-----------|
| 1         | null        |                  |            |           |
| 2         |             | null (tombstone) |            |           |
| 3         | A           |                  |            | [A, null] |
| 4         |             | a                |            |           |
| 5         | B           |                  | [B, a]     | [B, a]    |
| 6         |             | b                |            |           |
| 7         | null        |                  |            |           |
| 8         |             | null (tombstone) |            |           |
| 9         | C           |                  |            | [C, null] |
| 10        |             | c                |            |           |
| 11        |             | null             |            |           |
| 12        | null        |                  |            |           |
| 13        |             | null             |            |           |
| 14        |             | d                |            |           |
| 15        | D           |                  | [D, d]     | [D, d]    |

Notice that the INNER JOIN doesn't result in any output if the table-side
does not already contain a value for the key, even if the table-side is
later populated. For the LEFT JOIN the same scenario results in an output 
of leftRecord-NULL.  It is therefore important that the table data is 
loaded _before_ the stream event is received. 

ksqlDB attempts to process both sides of a join in event-time order, 
but it can't offer strong guarantees, especially in the presence of 
out-of-order rows. 

To maximise join predictability, ensure historic table data is available in the 
source topic, the query is running, and ksqlDB has had enough time to process 
the table data _before_ starting to produce to your stream.

!!! tip
    If no records are showing for a stream-table join, you may need to set the
    `max.task.idle.ms` config to make stream records wait for table records
    before being processed so they can be joined. 

Table-Table Joins
-----------------

ksqlDB supports primary-key (1:1) as well as foreign-key (1:N) joins between tables.
Many-to-many (N:M) joins are not supported currently.
For a foreign-key join, you can use any left table column in the join condition
to join it with the primary-key of the right table.

For primary-key joins INNER, LEFT OUTER, and FULL OUTER joins are supported.
For foreign-key joins INNER and LEFT OUTER joins are supported.

Table-table joins are always non-windowed joins.

Table-table joins are eventually consistent.

!!! important
      ksqlDB currently provides best-effort on time synchronization, but there
      are no guarantees, which can cause missing results or leftRecord-NULL
      results.

### Semantics of Primary-Key Table-Table Joins

The semantics of the various 1:1 table-table join variants are shown in the
following table. In the table, each row represents a new incoming
record. The following assumptions apply:

-   All records have the same key.
-   All records are processed in timestamp order.

Input records with a `null` value are interpreted as tombstones for the
corresponding key, which indicate the deletion of the key from the table.
When an input tombstone is received, an output tombstone may be forwarded
to the join result table, if the corresponding join result exists in the result table.


| Timestamp | Left Table       | Right Table      | INNER JOIN       | LEFT JOIN        | RIGHT JOIN       | OUTER JOIN       |
|-----------|------------------|------------------|------------------|------------------|------------------|------------------|
| 1         | null (tombstone) |                  |                  |                  |                  |                  |
| 2         |                  | null (tombstone) |                  |                  |                  |                  |
| 3         | A                |                  |                  | [A, null]        |                  | [A, null]        |
| 4         |                  | a                | [A, a]           | [A, a]           | [A, a]           | [A, a]           |
| 5         | B                |                  | [B, a]           | [B, a]           | [B, a]           | [B, a]           |
| 6         |                  | b                | [B, b]           | [B, b]           | [B, b]           | [B, b]           |
| 7         | null (tombstone) |                  | null (tombstone) | null (tombstone) | [null, b]        | [null, b]        |
| 8         |                  | null (tombstone) |                  |                  | null (tombstone) | null (tombstone) |
| 9         | C                |                  |                  | [C, null]        |                  | [C, null]        |
| 10        |                  | c                | [C, c]           | [C, c]           | [C, c]           | [C, c]           |
| 11        |                  | null (tombstone) | null (tombstone) | [C, null]        | null (tombstone) | [C, null]        |
| 12        | null (tombstone) |                  |                  | null (tombstone) |                  | null (tombstone) |
| 13        |                  | null (tombstone) |                  |                  |                  |                  |
| 14        |                  | d                |                  |                  | [null, d]        | [null, d]        |
| 15        | D                |                  | [D, d]           | [D, d]           | [D, d]           | [D, d]           |
| 16        |                  |                  |                  |                  |                  |                  |
| 17        |                  | d                | [D, d]           | [D, d]           | [D, d]           | [D, d]           |

### Semantics of Foreign-Key Table-Table Joins

The semantics of the various 1:N table-table join variants are shown in the
following table. In the table, each row represents a new incoming
record. The following assumptions apply:

-   The primary-key of the left and output table is denoted in upper-case letters.
-   The foreign-key from the left table and the primary-key of the right table is denoted in lower-case letters.
-   All records are processed in timestamp order.

Input records with a `null` value are interpreted as tombstones for the
corresponding key, which indicate the deletion of the key from the table.
When an input tombstone is received, one or multiple output tombstones
may be forwarded to the join result table, if a corresponding join result
exists in the result table.


| Timestamp | Left Table           | Right Table          | INNER JOIN                                 | LEFT JOIN                            |
|-----------|----------------------|----------------------|--------------------------------------------|--------------------------------------|
| 1         | [A] null (tombstone) |                      |                                            |                                      |
| 2         |                      | [a] null (tombstone) |                                            |                                      |
| 3         | [A,a,1]              |                      |                                            | [A,a,1,NULL,NULL]                    |
| 4         |                      | [a,10]               | [A,a,1,a,10]                               | [A,a,1,a,10]                         |
| 5         |                      | [a,11]               | [A,a,1,a,11]                               | [A,a,1,a,11]                         |
| 6         | [A,a,2]              |                      | [A,a,2,a,11]                               | [A,a,2,a,11]                         |
| 7         | [B,a,3]              |                      | [B,a,3,a,11]                               | [B,a,3,a,11]                         |
| 8         |                      | [a,12]               | [A,a,2,a,12], [B,a,3,a,12]                 | [A,a,2,a,12], [B,a,3,a,12]           |
| 9         |                      | [c,13]               |                                            |                                      |
| 10        | [C,c,4]              |                      | [C,c,4,c,13]                               | [C,c,4,c,13]                         |
| 11        | [C] null (tombstone) |                      | [C] null (tombstone)                       | [C] null (tombstone)                 |
| 12        |                      | [c] null (tombstone) |                                            |                                      |
| 13        |                      | [a] null (tombstone) | [A] null (tombstone), [B] null (tombstone) | [A,a,2,NULL,NULL], [B,a,3,NULL,NULL] |
| 14        | [B] null (tombstone) |                      |                                            | [B] null (tombstone)                 |

N-Way Joins
-----------

ksqlDB supports joining more than two sources in a single statement. These
joins are semantically equivalent to joining N sources consecutively, and
the order of the joins is controlled by the order in which the joins are
written.

Consider the following query as an example, where `A` is a stream of events
and `B` and `C` are both tables:
```sql
CREATE STREAM joined AS 
  SELECT * 
  FROM A
    JOIN B ON A.id = B.product_id
    JOIN C ON A.id = C.purchased_id;
```

The output of this query is a stream, and the intermediate join result
would is the stream `A ⋈ B`. If `C` were a stream instead of a table, you would 
rewrite the join accordingly, by adding a `WITHIN` clause because joining `A ⋈ B`
with `C` is a stream-stream join:

```sql
CREATE STREAM joined AS 
  SELECT * 
  FROM A
    JOIN B ON A.id = B.product_id
    JOIN C WITHIN 10 SECONDS ON A.id = C.purchased_id;
```

### Limitations of N-Way Joins

- The limitations and restrictions described in the previous sections to each intermediate 
  step in N-way joins. For example, `FULL OUTER` joins between streams and tables are
  not supported. This means that if any stage in the N-way join resolves to a `FULL OUTER`
  join between a stream and a table the entire query fails:

  ```sql
  --- This JOIN fails with the following exception:
  --- Join between invalid operands requested: left type: KTABLE, right type: KSTREAM
  CREATE STREAM joined AS 
    SELECT * 
    FROM A
      JOIN B WITHIN 10 SECONDS ON A.id = B.product_id
      FULL OUTER JOIN C ON A.id = C.purchased_id;
  ```

- Although ksqlDB supports N-way joins in general, it's not possible to have a
  `FULL OUTER` N-way table join in a single query. You must break the query into
  multiple queries, with each doing a single `FULL OUTER` table-table join.
