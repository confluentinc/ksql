# ![KSQL rocket](ksq-lrocket.png) KSQL - Streaming SQL for Apache Kafka

> *Important: This release is a **developer preview** and is free and open-source from Confluent under the Apache 2.0 license. Do not run KSQL against a production cluster.*

KSQL is an open source streaming SQL engine for Apache Kafka. It provides a simple and completely interactive SQL interface for stream processing on Kafka; no need to write code in a programming language such as Java or Python. KSQL is open-source (Apache 2.0 licensed), distributed, scalable, reliable, and real-time. It supports a wide range of powerful stream processing operations including aggregations, joins, windowing, sessionization, and much more.

Click here to watch a screencast of the KSQL demo on YouTube.
<a href="https://youtu.be/A45uRzJiv7I" target="_blank"><img src="screencast.jpg" alt="KSQL screencast"></a></p>
<!-- [![KSQL screencast](screencast.jpg)](https://youtu.be/A45uRzJiv7I) -->

# Quick Start
If you are ready to see the power of KSQL, try out these:

- [KSQL Quick Start](/docs/quickstart#quick-start): Demonstrates a simple workflow using KSQL to write streaming queries against data in Kafka.
- [Clickstream Analysis Demo](/ksql-clickstream-demo#clickstream-analysis): Shows how to build an application that performs real-time user analytics.

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
CREATE STREAM possible_fraud AS
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


# Documentation
You can [find the KSQL documentation here](/docs#ksql-documentation).

# Join the Community
Whether you need help, want to contribute, or are just looking for the latest news, you can find out how to [connect with your fellow Confluent community members here](https://www.confluent.io/contact-us-thank-you/).

* Ask a question in the #ksql channel in our public [Confluent Community Slack](https://slackpass.io/confluentcommunity). Account registration is free and self-service.
* Join the [Confluent Google group](https://groups.google.com/forum/#!forum/confluent-platform).

# Contributing
Contributions to the code, examples, documentation, etc, are very much appreciated. For more information, see the [contribution guidelines](/docs/contributing.md).

- Report issues and bugs directly in [this GitHub project](https://github.com/confluentinc/ksql/issues).

# License
The project is licensed under the Apache License, version 2.0.

*Apache, Apache Kafka, Kafka, and associated open source project names are trademarks of the [Apache Software Foundation](https://www.apache.org/).*

