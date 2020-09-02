---
layout: page
title: ksqlDB Tutorials and Examples
tagline: Walkthroughs and example queries
description: Learn how to create ksqlDB applications 
keywords: ksqldb, query, application, quickstart, tutorial, walkthrough, how to
---

- [Quickstart](https://ksqldb.io/quickstart.html)
- [Build a materialized view/cache](materialized.md)
- [Build a streaming ETL pipeline](etl.md)
- [Build an event-driven microservice](event-driven-microservice.md)
- [ksqlDB Examples](examples.md)
- [ksqlDB with Embedded Connect](embedded-connect.md)
- [Integrating with PostgreSQL](connect-integration.md)

### Stream Processing Cookbook

The [Stream Processing Cookbook](https://www.confluent.io/product/ksql/stream-processing-cookbook)
contains ksqlDB recipes that provide in-depth tutorials and recommended
deployment scenarios.

### ksqlDB with Embedded Connect

ksqlDB has native integration with {{ site.kconnect }}. While ksqlDB can integrate with a separate [Kafka Connect](https://docs.confluent.io/current/connect/index.html) cluster, it can also run {{ site.kconnect }} embedded within the ksqlDB server, making it unnecessary to run a separate {{ site.kconnect }} cluster. The [embedded Connect tutorial](embedded-connect.md) shows how you can configure ksqlDB to run {{ site.kconnect }} in embedded mode.

### ksqlDB Examples

[These examples](examples.md) provide common ksqlDB usage operations.

You can configure Java streams applications to deserialize and ingest
data in multiple ways, including {{ site.ak }} console producers, JDBC
source connectors, and Java client producers. For full code examples,

### ksqlDB Videos


|         Video             |   Description                             |
| ----------------------------------------------------- | ------------------------------------------------------------------------------- |
| [Demo: The Event Streaming Database in Action](https://www.youtube.com/watch?v=D5QMqapzX8o) | Tim Berglund builds a movie rating system with ksqlDB to write movie records into a {{ site.ak }} topic. |
| [Demo: Seamless Stream Processing with Kafka Connect & ksqlDB](https://www.youtube.com/watch?v=4odZGWl-yZo) | Set up and build ksqlDB applications using the AWS source, Azure sink, and MongoDB source connectors in {{ site.ccloud }}. |
| [Introduction to ksqlDB and stream processing](https://www.youtube.com/watch?v=-kFU6mCnOFw) | Vish Srinivasan talks {{ site.ak }} stream processing fundamentals and discusses ksqlDB. |
| [Ask Confluent #16: ksqlDB edition](https://www.youtube.com/watch?v=SHKjuN2iXyk) | Gwen Shapira hosts Vinoth Chandar in a wide-ranging talk on ksqlDB. | 
| [An introduction to ksqlDB](https://www.youtube.com/watch?v=7mGBxG2NhVQ) | Robin Moffatt describes how ksqlDB helps you build scalable and fault-tolerant stream processing systems. |
| [ksqlDB and the Kafka Connect JDBC Sink](https://www.youtube.com/watch?v=ad02yDTAZx0) | Robin Moffatt demonstrates how to use the Kafka Connect JDBC sink. |
| [How to Transform a Stream of Events Using ksqlDB](https://www.youtube.com/watch?v=PaHv4fGq-9k) | Viktor Gamov demonstrates how to transform a stream of movie data. |
