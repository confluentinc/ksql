# KSQL Documentation

| [Overview](/docs/overview.md) | [Installation](/docs/installation.md) | [Quick Start Guide](/docs/installation.md) | [Syntax Reference](/docs/syntax-reference.md) | [Examples](/docs/examples.md) | [FAQ](/docs/faq.md)  |
|----------|--------------|-------------|------------------|------------------|------------------|

# Overview
KSQL is an open source streaming SQL engine that implements continuous, interactive queries against Apache Kafka™. It allows you to query, read, write, and process data in Apache Kafka in real-time, at scale using SQL an SQL-like commands. KSQL interacts directly with the [Kafka Streams API](http://docs.confluent.io/current/streams/concepts.html), removing the requirement of building a Java app. 

> *Important: This release is a *developer preview* and is free and open-source from Confluent under the Apache 2.0 license.*

### Terminology 

#### Stream
A stream is an unbounded sequence of structured values that are stored in a Kafka topic. The structure of the values is specified in a schema. In Kafka streams vocabulary, a KSQL stream is a [KStream](http://docs.confluent.io/current/streams/concepts.html?highlight=kstream#kstream) plus a schema. 

#### Table
A table in KSQL is finite, where the bounds are defined by the size of the key space. The key space is an evolving collection of structured values, where the structure of the values is specified in a schema. These values are stored in a changelog topic in Kafka. In Kafka Streams vocabulary, a KSQL table is a [KTable](http://docs.confluent.io/current/streams/concepts.html?highlight=ktable#ktable) plus a schema.

#### Topic
A topic is a category or feed name where records are published. For more information, see the [Apache Kafka documentation](https://kafka.apache.org/documentation/#intro_topics).

# Quick Start Guide

* Beginners: Try the [interactive quick start](/quickstart/). The quick start configures a single instance in a lightweight Docker container or in a Kafka cluster. It demonstrates a simple workflow using KSQL to write streaming queries against data in Kafka.
* Advanced users: Try the [end-to-end KSQL demo](https://github.com/confluentinc/ksql).

# Installation

Follow [these instructions](../README.md/#initial-cleanup#installation).


