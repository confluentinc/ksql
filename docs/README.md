# KSQL Documentation

| Overview |[Quick Start](/docs/quickstart#quick-start) | [Concepts](/docs/concepts.md#concepts) | [Syntax Reference](/docs/syntax-reference.md#syntax-reference) |[Demo](/ksql-clickstream-demo#clickstream-analysis) | [Examples](/docs/examples.md#examples) | [FAQ](/docs/faq.md#frequently-asked-questions) |
|---|----|-----|----|----|----|----|

> *Important: This release is a **developer preview** and is free and open-source from Confluent under the Apache 2.0 license. Do not run KSQL against a production cluster.*

# Overview
KSQL is an open source streaming SQL engine that implements continuous, interactive queries against Apache Kafka™. It allows you to query, read, write, and process data in Apache Kafka in real-time, at scale using SQL commands. KSQL interacts directly with the [Kafka Streams API](https://kafka.apache.org/documentation/streams/), removing the requirement of building a Java app.

### Use cases
Common KSQL use cases are:

- Fraud detection - identify and act on out of the ordinary data to provide real-time awareness.
- Personalization - create real-time experiences and insight for end users driven by data.
- Notifications - build custom alerts and messages based on real-time data.
- Real-time Analytics - power real-time dashboards to understand what’s happening as it does.
- Sensor data and IoT - understand and deliver sensor data how and where it needs to be.
- Customer 360 - provide a clear, real-time understanding of your customers across every interaction.

KSQL lowers the barriers for using real-time data in your applications. It is powered by a scalable streaming platform without the learning curve or additional management complexity of other stream processing solutions.

## Modes of operation

You can use KSQL in standalone, client-server, application, and embedded modes. See [Concepts](/docs/concepts.md#concepts) for more information.

## Getting Started

* Beginners: Try the [interactive quick start](/docs/quickstart#quick-start). The quick start configures a single instance in a lightweight Docker container or in a Kafka cluster. It demonstrates a simple workflow using KSQL to write streaming queries against data in Kafka.
* Advanced users: Try the [end-to-end Clickstream Analysis demo](/ksql-clickstream-demo#clickstream-analysis).

## Interoperability

This table shows the version compatibility matrix of which Kafka clusters can be used to read from and write into while running KSQL queries.

|        KSQL        |        0.1       |        0.2       |        0.3       |        0.4       |
|:------------------:|:----------------:|:----------------:|:----------------:|:----------------:|
|    Apache Kafka    | 0.10.1 and later | 0.11.0 and later | 0.11.0 and later | 0.11.0 and later |
| Confluent Platform | 3.1.0 and later  | 3.3.0 and later  | 3.3.0 and later  | 3.3.0 and later  | 


