---
layout: page
title: ksqlDB Tutorials and Examples
tagline: Walkthroughs and example queries
description: Learn how to create ksqlDB applications 
keywords: ksqldb, query, application, quickstart, tutorial, walkthrough, how to
---

- [Quickstart](ksqldb-quickstart.md)
- [Build a materialized view/cache](materialized.md)
- [Build a streaming ETL pipeline](etl.md)
- [ksqlDB Examples](examples.md)
- [ksqlDB with Embedded Connect](embedded-connect.md)
- [Integrating with PostgreSQL](connect-integration.md)

Stream Processing Cookbook
--------------------------

The [Stream Processing Cookbook](https://www.confluent.io/product/ksql/stream-processing-cookbook)
contains ksqlDB recipes that provide in-depth tutorials and recommended
deployment scenarios.

ksqlDB with Embedded Connect
-------------------------------

ksqlDB has native integration with {{ site.kconnect }}. While ksqlDB can integrate with a separate [Kafka Connect](https://docs.confluent.io/current/connect/index.html) cluster, it can also run {{ site.kconnect }} embedded within the ksqlDB server, making it unnecessary to run a separate {{ site.kconnect }} cluster. The [embedded Connect tutorial](embedded-connect.md) shows how you can configure ksqlDB to run {{ site.kconnect }} in embedded mode.

ksqlDB Examples
---------------

[These examples](examples.md) provide common ksqlDB usage operations.

You can configure Java streams applications to deserialize and ingest
data in multiple ways, including {{ site.ak }} console producers, JDBC
source connectors, and Java client producers. For full code examples,

Level Up Your KSQL Videos
-------------------------

|                                           Video                                            |                                                                                                  Description                                                                                                   |
| ------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [KSQL Introduction](https://www.youtube.com/embed/C-rUyWmRJSQ)                             | Intro to Kafka stream processing, with a focus on KSQL.                                                                                                                                                        |
| [KSQL Use Cases](https://www.youtube.com/embed/euz0isNG1SQ)                                | Describes several KSQL uses cases, like data exploration, arbitrary filtering, streaming ETL, anomaly detection, and real-time monitoring.                                                                     |
| [KSQL and Core Kafka](https://www.youtube.com/embed/-GpbMAK3Uow)                           | Describes KSQL dependency on core Kafka, relating KSQL to clients, and describes how KSQL uses Kafka topics.                                                                                                   |
| [Installing and Running KSQL](https://www.youtube.com/embed/icwHpPm-TCA)                   | How to get KSQL, configure and start the KSQL server, and syntax basics.                                                                                                                                       |
| [KSQL Streams and Tables](https://www.youtube.com/embed/DPGn-j7yD68)                       | Explains the difference between a STREAM and TABLE, shows a detailed example, and explains how streaming queries are unbounded.                                                                                |
| [Reading Kafka Data from KSQL](https://www.youtube.com/embed/EzVZOUt9JsU)                  | How to explore Kafka topic data, create a STREAM or TABLE from a Kafka topic, identify fields. Also explains metadata like ROWTIME and TIMESTAMP, and covers different formats like Avro, JSON, and Delimited. |
| [Streaming and Unbounded Data in KSQL](https://www.youtube.com/embed/4ccg1AFeNB0)          | More detail on streaming queries, how to read topics from the beginning, the differences between persistent and non-persistent queries, how do streaming queries end.                                          |
| [Enriching data with KSQL](https://www.youtube.com/embed/9_Gwe6qJrjI)                      | Scalar functions, changing field types, filtering data, merging data with JOIN, and rekeying streams.                                                                                                          |
| [Aggregations in KSQL](https://www.youtube.com/embed/db5SsmNvej4)                          | How to aggregate data with KSQL, different types of aggregate functions like COUNT, SUM, MAX, MIN, TOPK, etc, and windowing and late-arriving data.                                                            |
| [Taking KSQL to Production](https://www.youtube.com/embed/f3wV8W_zjwE)                     | How to use KSQL in streaming ETL pipelines, scale query processing, isolate workloads, and secure your entire deployment.                                                                                      |
| [Insert Into](https://www.youtube.com/watch?v=z508VDdtp_M)                                 | A brief tutorial on how to use INSERT INTO in KSQL by Confluent.                                                                                                                                               |
| [Struct (Nested Data)](https://www.youtube.com/watch?v=TQd5rfFmbhw)                        | A brief tutorial on how to use STRUCT in KSQL by Confluent.                                                                                                                                                    |
| [Stream-Stream Joins](https://www.youtube.com/watch?v=51yLu5FnPYo)                         | A short tutorial on stream-stream joins in KSQL by Confluent.                                                                                                                                                  |
| [Table-Table Joins](https://www.youtube.com/watch?v=-eMXWeBfK7U)                           | A short tutorial on table-table joins in KSQL by Confluent.                                                                                                                                                    |
| [Monitoring KSQL in Confluent Control Center](https://www.youtube.com/watch?v=3o7MzCri4e4) | Monitor performance and end-to-end message delivery of your KSQL queries.                                                                                                                                      |

Page last revised on: {{ git_revision_date }}
