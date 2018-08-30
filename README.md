# ![KSQL rocket](ksq-lrocket.png) KSQL - Streaming SQL for Apache Kafka

> **KSQL is now GA and officially supported by Confluent Inc. [Get started with KSQL today](#getting-started).**

KSQL is the streaming SQL engine for Apache Kafka.

KSQL is an open source streaming SQL engine for Apache Kafka. It provides a simple and completely interactive SQL interface for stream processing on Kafka; no need to write code in a programming language such as Java or Python. KSQL is open-source (Apache 2.0 licensed), distributed, scalable, reliable, and real-time. It supports a wide range of powerful stream processing operations including aggregations, joins, windowing, sessionization, and much more.

Click here to watch a screencast of the KSQL demo on YouTube.
<a href="https://www.youtube.com/watch?v=illEpCOcCVg" target="_blank"><img src="screencast.jpg" alt="KSQL screencast"></a></p>

<a name="getting-started"></a>
# Getting Started and Download

<a name="stable-releases"></a>
## Stable Releases

Stable releases are published every four months and are officially supported by [Confluent](http://www.confluent.io/).

1. [Download latest stable KSQL](https://www.confluent.io/download/), which is included in the Enterprise and
   Open Source editions of Confluent Platform.
2. Follow the [Quick Start](https://docs.confluent.io/current/quickstart.html).
3. Read the [KSQL Documentation](https://docs.confluent.io/current/ksql/docs/), notably the
   [KSQL Tutorials and Examples](https://docs.confluent.io/current/ksql/docs/tutorials/), which include Docker-based
   variants.


<a name="preview-releases"></a>
## Preview Releases

In addition to supported [stable KSQL releases](#stable-releases), we also provide monthly preview releases.
We encourage you to try them in development and testing environments and to take advantage of
[Confluent Community resources](#community) to get help and share feedback.

* [Download latest KSQL Preview](https://www.confluent.io/preview-release).

# Documentation

See [KSQL documentation](https://docs.confluent.io/current/ksql/docs/) for the latest stable release.


# Use Cases and Examples

## Streaming ETL

Apache Kafka is a popular choice for powering data pipelines.  KSQL makes it simple to transform data within the
pipeline, readying messages to cleanly land in another system.

```sql
CREATE STREAM vip_actions AS
  SELECT userid, page, action
  FROM clickstream c
  LEFT JOIN users u ON c.userid = u.user_id
  WHERE u.level = 'Platinum';
```


## Anomaly Detection

KSQL is a good fit for identifying patterns or anomalies on real-time data. By processing the stream as data arrives
you can identify and properly surface out of the ordinary events with millisecond latency.

```sql
CREATE TABLE possible_fraud AS
  SELECT card_number, count(*)
  FROM authorization_attempts
  WINDOW TUMBLING (SIZE 5 SECONDS)
  GROUP BY card_number
  HAVING count(*) > 3;
```


## Monitoring

Kafka's ability to provide scalable ordered messages with stream processing make it a common solution for log data
monitoring and alerting. KSQL lends a familiar syntax for tracking, understanding, and managing alerts.

```sql
CREATE TABLE error_counts AS
  SELECT error_code, count(*)
  FROM monitoring_stream
  WINDOW TUMBLING (SIZE 1 MINUTE)
  WHERE  type = 'ERROR'
  GROUP BY error_code;
```


# Latest News

* [KSQL May 2018 Preview Release available](https://www.confluent.io/blog/may-preview-release-advancing-ksql-and-schema-registry/), Jun 2018
  -- new KSQL Docker images (for server and for CLI), support for INSERT INTO statement, KSQL editor auto-completion, and more
* [KSQL April 2018 Preview Release available](https://www.confluent.io/blog/introducing-confluent-platform-preview-releases/),
  May 2018
* [Confluent Platform 4.1 with Production-Ready KSQL Now Available](https://www.confluent.io/blog/confluent-platform-4-1-with-production-ready-ksql-now-available/), Apr 2018
* [Press Release: KSQL GA announced for early April 2018](https://www.confluent.io/press-release/confluent-makes-ksql-available-confluent-platform-announces-general-availability/)
* [We ❤ syslogs: Real-time syslog Processing with Apache Kafka and KSQL—Part 2: Event-Driven Alerting with Slack](https://www.confluent.io/blog/real-time-syslog-processing-with-apache-kafka-and-ksql-part-2-event-driven-alerting-with-slack/), Apr 2018
* [We ❤ syslogs: Real-time syslog Processing with Apache Kafka and KSQL—Part 1: Filtering](https://www.confluent.io/blog/real-time-syslog-processing-apache-kafka-ksql-part-1-filtering), Apr 2018
* [KSQL in Action: Enriching CSV Events with Data from RDBMS into AWS](https://www.confluent.io/blog/ksql-in-action-enriching-csv-events-with-data-from-rdbms-into-AWS/), Mar 2018
* [KSQL February 2018 Preview Release available](https://www.confluent.io/blog/ksql-february-release-streaming-sql-for-apache-kafka/)
  -- bug fixes, performance and stability improvements
* [Secure Stream Processing with Apache Kafka, Confluent Platform and KSQL](https://www.confluent.io/blog/secure-stream-processing-apache-kafka-ksql/), Feb 2018
  -- stream processing examples using KSQL that show how companies are using Apache Kafka to grow their business and to
  analyze data in real time; how to secure KSQL and the entire Confluent Platform with encryption, authentication, and
  authorization
* [KSQL in Action: Real-Time Streaming ETL from Oracle Transactional Data](https://www.confluent.io/blog/ksql-in-action-real-time-streaming-etl-from-oracle-transactional-data), Feb 2018
  -- replacing batch extracts with event streams, and batch transformation with in-flight transformation; we take a
  stream of data from a transactional system built on Oracle, transform it, and stream the results into Elasticsearch
* [KSQL January 2018 Preview Release available](https://www.confluent.io/blog/ksql-january-release-streaming-sql-apache-kafka/)
  -- improved data exploration with `PRINT TOPIC`, `SHOW TOPICS`; improved analytics with `TOPK`, `TOPKDISTINCT`
  aggregations; operational improvements (command line tooling for metrics); distributed failure testing in place
* [KSQL December 2017 Preview Release available](https://www.confluent.io/blog/ksql-december-release)
  -- support for Avro and [Confluent Schema Registry](https://github.com/confluentinc/schema-registry); easy data
  conversion between Avro, JSON, Delimited data; joining streams and tables across different data formats; operational
  improvements (`DESCRIBE EXTENDED`, `EXPLAIN`, and new metrics); optimizations (faster server startup and recovery
  times, better resource utilization)


<a name="community"></a>
# Join the Community

You can get help, learn how to contribute to KSQL, and find the latest news by [connecting with the Confluent community](https://www.confluent.io/contact-us-thank-you/).

* Ask a question in the #ksql channel in our public [Confluent Community Slack](https://slackpass.io/confluentcommunity). Account registration is free and self-service.
* Join the [Confluent Google group](https://groups.google.com/forum/#!forum/confluent-platform).


# Contributing

Contributions to the code, examples, documentation, etc. are very much appreciated.

- Report issues and bugs directly in [this GitHub project](https://github.com/confluentinc/ksql/issues).
- Learn how to work with the KSQL source code, including building and testing KSQL as well as contributing code changes
  to KSQL by reading our [Development and Contribution guidelines](CONTRIBUTING.md).


# License

The project is [licensed](LICENSE) under the Apache License, version 2.0.

*Apache, Apache Kafka, Kafka, and associated open source project names are trademarks of the [Apache Software Foundation](https://www.apache.org/).*
