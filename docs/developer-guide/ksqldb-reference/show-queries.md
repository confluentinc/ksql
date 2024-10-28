---
layout: page
title: SHOW QUERIES
tagline:  ksqlDB SHOW QUERIES statement
description: Syntax for the SHOW QUERIES statement in ksqlDB
keywords: ksqlDB, list, query
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/show-queries.html';
</script>

SHOW QUERIES
============

Synopsis
--------

```sql
SHOW | LIST QUERIES [EXTENDED];
```

Description
-----------

`SHOW QUERIES` lists queries running in the cluster.

`SHOW QUERIES EXTENDED` lists queries running in the cluster in more detail.

Query Status
-----------

* `RUNNING`: the query was either just started, or has been running without errors.
* `ERROR`: the query has entered an error state.
* `UNRESPONSIVE`: the host running the query returned an error when requesting the query status.

Query Type
-----------

* `PERSISTENT`: these queries run on every node and materialize new state.
* `PUSH`: these queries are owned by the client and are terminated when the session ends.

Example
-------

```sql
ksql> show queries;

 Query ID    | Query Type       | Status    | Sink Name | Sink Kafka Topic | Query String                                                                                                                                
------------------------------------------------------------------------------------------------------------
 CSAS_TEST_0 | PERSISTENT       | RUNNING:2 | TEST      | TEST             | CREATE STREAM TEST WITH (KAFKA_TOPIC='TEST', PARTITIONS=1, REPLICAS=1) AS SELECT *FROM KSQL_PROCESSING_LOG KSQL_PROCESSING_LOG EMIT CHANGES; 
------------------------------------------------------------------------------------------------------------
For detailed information on a Query run: EXPLAIN <Query ID>;
```


```sql
ksql> show queries extended;

ID                   : CSAS_TEST_0
Query Type           : PERSISTENT
SQL                  : CREATE STREAM TEST WITH (KAFKA_TOPIC='TEST', PARTITIONS=1, REPLICAS=1) AS SELECT *
FROM KSQL_PROCESSING_LOG KSQL_PROCESSING_LOG
EMIT CHANGES;
Host Query Status    : {192.168.1.6:8088=RUNNING, 192.168.1.6:8089=RUNNING}

 Field   | Type                                                                                                                                                                                                                                                                                                                                    
-------------------------------------------------------------------------------------
 LOGGER  | VARCHAR(STRING)                                                                                                                                                                                                                                                                                                                         
 LEVEL   | VARCHAR(STRING)                                                                                                                                                                                                                                                                                                                         
 TIME    | BIGINT                                                                                                                                                                                                                                                                                                                                  
 MESSAGE | STRUCT<TYPE INTEGER, DESERIALIZATIONERROR STRUCT<ERRORMESSAGE VARCHAR(STRING), RECORDB64 VARCHAR(STRING), CAUSE ARRAY<VARCHAR(STRING)>, topic VARCHAR(STRING)>, RECORDPROCESSINGERROR STRUCT<ERRORMESSAGE VARCHAR(STRING), RECORD VARCHAR(STRING), CAUSE ARRAY<VARCHAR(STRING)>>, PRODUCTIONERROR STRUCT<ERRORMESSAGE VARCHAR(STRING)>, KAFKASTREAMSTHREADERROR STRUCT<THREADNAME VARCHAR(STRING), ERRORMESSAGE VARCHAR(STRING), CAUSE ARRAY<VARCHAR(STRING)>>>
-------------------------------------------------------------------------------------

Sources that this query reads from: 
-----------------------------------
KSQL_PROCESSING_LOG

For source description please run: DESCRIBE [EXTENDED] <SourceId>

Sinks that this query writes to: 
-----------------------------------
TEST

For sink description please run: DESCRIBE [EXTENDED] <SinkId>

Execution plan      
--------------      
 > [ SINK ] | Schema: LOGGER STRING, LEVEL STRING, TIME BIGINT, MESSAGE STRUCT<TYPE INTEGER, DESERIALIZATIONERROR STRUCT<ERRORMESSAGE STRING, RECORDB64 STRING, CAUSE ARRAY<STRING>, `topic` STRING>, RECORDPROCESSINGERROR STRUCT<ERRORMESSAGE STRING, RECORD STRING, CAUSE ARRAY<STRING>>, PRODUCTIONERROR STRUCT<ERRORMESSAGE STRING>> | Logger: CSAS_TEST_0.TEST
                 > [ PROJECT ] | Schema: LOGGER STRING, LEVEL STRING, TIME BIGINT, MESSAGE STRUCT<TYPE INTEGER, DESERIALIZATIONERROR STRUCT<ERRORMESSAGE STRING, RECORDB64 STRING, CAUSE ARRAY<STRING>, `topic` STRING>, RECORDPROCESSINGERROR STRUCT<ERRORMESSAGE STRING, RECORD STRING, CAUSE ARRAY<STRING>>, PRODUCTIONERROR STRUCT<ERRORMESSAGE STRING>> | Logger: CSAS_TEST_0.Project
                                 > [ SOURCE ] | Schema: LOGGER STRING, LEVEL STRING, TIME BIGINT, MESSAGE STRUCT<TYPE INTEGER, DESERIALIZATIONERROR STRUCT<ERRORMESSAGE STRING, RECORDB64 STRING, CAUSE ARRAY<STRING>, `topic` STRING>, RECORDPROCESSINGERROR STRUCT<ERRORMESSAGE STRING, RECORD STRING, CAUSE ARRAY<STRING>>, PRODUCTIONERROR STRUCT<ERRORMESSAGE STRING>>, ROWTIME BIGINT, ROWPARTITION INTEGER, ROWOFFSET BIGINT | Logger: CSAS_TEST_0.KsqlTopic.Source


Processing topology 
------------------- 
Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [default_ksql_processing_log])
      --> KSTREAM-TRANSFORMVALUES-0000000001
    Processor: KSTREAM-TRANSFORMVALUES-0000000001 (stores: [])
      --> Project
      <-- KSTREAM-SOURCE-0000000000
    Processor: Project (stores: [])
      --> KSTREAM-SINK-0000000003
      <-- KSTREAM-TRANSFORMVALUES-0000000001
    Sink: KSTREAM-SINK-0000000003 (topic: TEST)
      <-- Project
```
