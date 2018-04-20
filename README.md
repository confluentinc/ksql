# ![KSQL rocket](ksq-lrocket.png) KSQL - Streaming SQL for Apache Kafka

> **Important:** We recently [announced the General Availability](https://www.confluent.io/press-release/confluent-makes-ksql-available-confluent-platform-announces-general-availability/) of KSQL. It will be available for download in early April at http://confluent.io/ksql. Until then you can download the latest KSQL Developer Preview release at https://github.com/confluentinc/ksql/releases.

KSQL is an open source streaming SQL engine for Apache Kafka. It provides a simple and completely interactive SQL interface for stream processing on Kafka; no need to write code in a programming language such as Java or Python. KSQL is open-source (Apache 2.0 licensed), distributed, scalable, reliable, and real-time. It supports a wide range of powerful stream processing operations including aggregations, joins, windowing, sessionization, and much more.

Click here to watch a screencast of the KSQL demo on YouTube.
<a href="https://www.youtube.com/watch?v=illEpCOcCVg" target="_blank"><img src="screencast.jpg" alt="KSQL screencast"></a></p>

# Getting Started
If you are ready to see the power of KSQL, try out these:

- [KSQL Quick Start](https://docs.confluent.io/current/ksql/docs/quickstart/): Demonstrates a simple workflow using KSQL to write streaming queries against data in Kafka.
- [Clickstream Analysis Demo](https://docs.confluent.io/current/ksql/docs/ksql-clickstream-demo/): Shows how to build an application that performs real-time user analytics.

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

# Latest news

* [KSQL Jan 2018 release available](https://www.confluent.io/blog/ksql-january-release-streaming-sql-apache-kafka/)
  -- improved data exploration with `PRINT TOPIC`, `SHOW TOPICS`; improved analytics with `TOPK`, `TOPKDISTINCT`
  aggregations; operational improvements (command line tooling for metrics); distributed failure testing in place
* [KSQL Dec 2017 release available](https://www.confluent.io/blog/ksql-december-release)
  -- support for Avro and [Confluent Schema Registry](https://github.com/confluentinc/schema-registry); easy data
  conversion between Avro, JSON, Delimited data; joining streams and tables across different data formats; operational
  improvements (`DESCRIBE EXTENDED`, `EXPLAIN`, and new metrics); optimizations (faster server startup and recovery
  times, better resource utilization)
* [KSQL Nov 2017 release available](https://www.confluent.io/blog/november-update-ksql-developer-preview-available/)
  -- focus on community-raised issues and requests (369 pull requests, 50 closed issues)


# Documentation
You can find the KSQL documentation at [docs.confluent.io](https://docs.confluent.io/current/ksql/docs/index.html).

# Join the Community
Whether you need help, want to contribute, or are just looking for the latest news, you can find out how to [connect with your fellow Confluent community members here](https://www.confluent.io/contact-us-thank-you/).

* Ask a question in the #ksql channel in our public [Confluent Community Slack](https://slackpass.io/confluentcommunity). Account registration is free and self-service.
* Join the [Confluent Google group](https://groups.google.com/forum/#!forum/confluent-platform).

# Contributing
Contributions to the code, examples, documentation, etc, are very much appreciated. For more information, see the [contribution guidelines](contributing.md).

- Report issues and bugs directly in [this GitHub project](https://github.com/confluentinc/ksql/issues).

# Issues
Report issues in [this GitHub project](https://github.com/confluentinc/ksql/issues).

# License
The project is licensed under the Apache License, version 2.0.

*Apache, Apache Kafka, Kafka, and associated open source project names are trademarks of the [Apache Software Foundation](https://www.apache.org/).*
