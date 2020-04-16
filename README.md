# ![KSQL rocket](ksql-rocket.png) ksqlDB

### The event streaming database purpose-built for stream processing applications

# Overview

ksqlDB is an event streaming database for Apache Kafka. It is **distributed**, **scalable**, **reliable**, and **real-time**. ksqlDB combines the power of real-time stream processing with the approachable feel of a relational database through a familiar, lightweight SQL syntax. ksqlDB offers these core primitives:

* **[Streams](https://docs.ksqldb.io/en/latest/concepts/collections/streams/) and [tables](https://docs.ksqldb.io/en/latest/concepts/collections/tables/)** - Create relations with schemas over your Apache Kafka topic data
* **[Materialized views](https://docs.ksqldb.io/en/latest/concepts/materialized-views/)** - Define real-time, incrementally updated materialized views over streams using SQL
* **[Push queries](https://docs.ksqldb.io/en/latest/concepts/queries/push/)**- Continuous queries that push incremental results to clients in real time
* **[Pull queries](https://docs.ksqldb.io/en/latest/concepts/queries/pull/)** - Query materialized views on demand, much like with a traditional database
* **[Connect](https://docs.ksqldb.io/en/latest/concepts/connectors)** - Integrate with any [Kafka Connect](https://docs.confluent.io/current/connect/index.html) data source or sink, entirely from within ksqlDB

Composing these powerful primitives enables you to build a complete streaming app with just SQL statements, minimizing complexity and operational overhead. ksqlDB supports a wide range of operations including aggregations, joins, windowing, sessionization, and much more. You can find more ksqlDB tutorials and resources [here](https://kafka-tutorials.confluent.io/).

# Getting Started

* Follow the [ksqlDB quickstart](https://ksqldb.io/quickstart.html) to get started in just a few minutes.
* Read through the [ksqlDB documentation](https://docs.ksqldb.io).
* Take a look at some [ksqlDB tutorials](https://kafka-tutorials.confluent.io/create-stateful-aggregation-count/ksql.html) for examples of common patterns.

# Documentation

See the [ksqlDB documentation](https://docs.ksqldb.io/) for the latest stable release.

# Use Cases and Examples

## Materialized views

ksqlDB allows you to define materialized views over your streams and tables. Materialized views are defined by what is known as a "persistent query". These queries are known as persistent because they maintain their incrementally updated results using a table.

```sql
CREATE TABLE hourly_metrics AS
  SELECT url, COUNT(*)
  FROM page_views
  WINDOW TUMBLING (SIZE 1 HOUR)
  GROUP BY url EMIT CHANGES;

```

Results may be **"pulled"** from materialized views on demand via `SELECT` queries. The following query will return a single row:

```sql
SELECT * FROM hourly_metrics
  WHERE url = 'http://myurl.com' AND WINDOWSTART = '2019-11-20T19:00';
```

Results may also be continuously **"pushed"** to clients via streaming `SELECT` queries. The following streaming query will push to the client all incremental changes made to the materialized view:

```sql
SELECT * FROM hourly_metrics EMIT CHANGES;
```

Streaming queries will run perpetually until they are explicitly terminated.

## Streaming ETL

Apache Kafka is a popular choice for powering data pipelines. ksqlDB makes it simple to transform data within the pipeline, readying messages to cleanly land in another system.

```sql
CREATE STREAM vip_actions AS
  SELECT userid, page, action
  FROM clickstream c
  LEFT JOIN users u ON c.userid = u.user_id
  WHERE u.level = 'Platinum' EMIT CHANGES;
```

## Anomaly Detection

ksqlDB is a good fit for identifying patterns or anomalies on real-time data. By processing the stream as data arrives you can identify and properly surface out of the ordinary events with millisecond latency.

```sql
CREATE TABLE possible_fraud AS
  SELECT card_number, count(*)
  FROM authorization_attempts
  WINDOW TUMBLING (SIZE 5 SECONDS)
  GROUP BY card_number
  HAVING count(*) > 3 EMIT CHANGES;
```

## Monitoring

Kafka's ability to provide scalable ordered messages with stream processing make it a common solution for log data monitoring and alerting. ksqlDB lends a familiar syntax for tracking, understanding, and managing alerts.

```sql
CREATE TABLE error_counts AS
  SELECT error_code, count(*)
  FROM monitoring_stream
  WINDOW TUMBLING (SIZE 1 MINUTE)
  WHERE  type = 'ERROR'
  GROUP BY error_code EMIT CHANGES;
```

## Integration with External Data Sources and Sinks

ksqlDB includes native integration with [Kafka Connect](https://docs.ksqldb.io/en/latest/concepts/connectors) data sources and sinks, effectively providing a unified SQL interface over a [broad variety of external systems](https://www.confluent.io/hub).

The following query is a simple persistent streaming query that will produce all of its output into a topic named `clicks_transformed`:

```sql
CREATE STREAM clicks_transformed AS
  SELECT userid, page, action
  FROM clickstream c
  LEFT JOIN users u ON c.userid = u.user_id EMIT CHANGES;
```

Rather than simply send all continuous query output into a Kafka topic, it is often very useful to route the output into another datastore. ksqlDB's Kafka Connect integration makes this pattern very easy.

The following statement will create a Kafka Connect sink connector that continuously sends all output from the above streaming ETL query directly into Elasticsearch:

```sql
 CREATE SINK CONNECTOR es_sink WITH (
  'connector.class' = 'io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
  'key.converter'   = 'org.apache.kafka.connect.storage.StringConverter',
  'topics'          = 'clicks_transformed',
  'key.ignore'      = 'true',
  'schema.ignore'   = 'true',
  'type.name'       = '',
  'connection.url'  = 'http://elasticsearch:9200');
```

<a name="community"></a>
# Join the Community

For user help, questions or queries about KSQL please use our [user Google Group](https://groups.google.com/forum/#!forum/ksql-users)
or our public Slack channel #ksqldb in [Confluent Community Slack](https://slackpass.io/confluentcommunity)

For discussions about development of KSQL please use our [developer Google Group](https://groups.google.com/forum/#!forum/ksql-dev).
You can also hang out in our developer Slack channel #ksqldb-dev in - [Confluent Community Slack](https://slackpass.io/confluentcommunity) - this is where day to day chat about the development of KSQL happens.
Everyone is welcome!

You can get help, learn how to contribute to KSQL, and find the latest news by [connecting with the Confluent community](https://www.confluent.io/contact-us-thank-you/).

For more general questions about the Confluent Platform please post in the [Confluent Google group](https://groups.google.com/forum/#!forum/confluent-platform).


# Contributing

Contributions to the code, examples, documentation, etc. are very much appreciated.

- Report issues and bugs directly in [this GitHub project](https://github.com/confluentinc/ksql/issues).
- Learn how to work with the KSQL source code, including building and testing KSQL as well as contributing code changes
  to KSQL by reading our [Development and Contribution guidelines](CONTRIBUTING.md).
- One good way to get started is by tackling a [newbie issue](https://github.com/confluentinc/ksql/labels/good%20first%20issue).


# License

The project is licensed under the [Confluent Community License](LICENSE).

*Apache, Apache Kafka, Kafka, and associated open source project names are trademarks of the [Apache Software Foundation](https://www.apache.org/).*
