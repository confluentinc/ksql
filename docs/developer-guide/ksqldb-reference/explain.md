---
layout: page
title: EXPLAIN
tagline:  ksqlDB EXPLAIN statement
description: Syntax for the EXPLAIN statement in ksqlDB
keywords: ksqlDB, execution plan, expression, query, statement
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/explain.html';
</script>

EXPLAIN
=======

Synopsis
--------

```sql
EXPLAIN (sql_expression|query_id);
```

Description
-----------

Show the execution plan for a SQL expression or, given the ID of a
running query, show the execution plan plus additional runtime
information and metrics. Statements such as DESCRIBE EXTENDED, for
example, show the IDs of queries related to a stream or table.

For more information, see
[ksqlDB Query Lifecycle](/operate-and-deploy/how-it-works#ksqldb-query-lifecycle).

Example
-------

The following statement shows the execution plan for a push query named
`ctas_ip_sum` on a topic named `CLICKSTREAM`.

```sql
EXPLAIN ctas_ip_sum;
```

Your output should resemble:

```
Type                 : QUERY
SQL                  : CREATE TABLE IP_SUM as SELECT ip,  sum(bytes)/1024 as kbytes FROM CLICKSTREAM window SESSION (300 second) GROUP BY ip EMIT CHANGES;


Local runtime statistics
------------------------
messages-per-sec:     104.38   total-messages:       14238     last-message: 12/14/17 4:30:42 PM GMT
    failed-messages:          0      last-failed:         n/a
(Statistics of the local Ksql Server interaction with the Kafka topic IP_SUM)

Execution plan
--------------
    > [ PROJECT ] Schema: [IP : STRING , KBYTES : INT64].
            > [ AGGREGATE ] Schema: [CLICKSTREAM.IP : STRING , CLICKSTREAM.BYTES : INT64 , KSQL_AGG_VARIABLE_0 : INT64].
                    > [ PROJECT ] Schema: [CLICKSTREAM.IP : STRING , CLICKSTREAM.BYTES : INT64].
                            > [ REKEY ] Schema: [CLICKSTREAM.ROWTIME : INT64 , CLICKSTREAM.ROWPARTITION : INT32 , CLICKSTREAM.ROWOFFSET : INT64 , CLICKSTREAM.IP : STRING , CLICKSTREAM._TIME : INT64 , CLICKSTREAM.TIME : STRING , CLICKSTREAM.REQUEST : STRING , CLICKSTREAM.STATUS : INT32 , CLICKSTREAM.USERID : INT32 , CLICKSTREAM.BYTES : INT64 , CLICKSTREAM.AGENT : STRING].
                                    > [ SOURCE ] Schema: [CLICKSTREAM.ROWTIME : INT64 , CLICKSTREAM.ROWPARTITION : INT32 , CLICKSTREAM.ROWOFFSET : INT64 , CLICKSTREAM._TIME : INT64 , CLICKSTREAM.TIME : STRING , CLICKSTREAM.IP : STRING , CLICKSTREAM.REQUEST : STRING , CLICKSTREAM.STATUS : INT32 , CLICKSTREAM.USERID : INT32 , CLICKSTREAM.BYTES : INT64 , CLICKSTREAM.AGENT : STRING].


Processing topology
-------------------
Sub-topologies:
    Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [clickstream])
        --> KSTREAM-MAP-0000000001
    Processor: KSTREAM-MAP-0000000001 (stores: [])
        --> KSTREAM-TRANSFORMVALUES-0000000002
        <-- KSTREAM-SOURCE-0000000000
```

