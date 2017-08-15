# KSQL Documentation

| Overview | [Installation](/docs/installation.md) | [Quick Start Guide](/docs/quickstart/) | [Syntax Reference](/docs/syntax-reference.md) | [Examples](/docs/examples.md) | [FAQ](/docs/faq.md)  |
|----------|--------------|-------------|------------------|------------------|------------------|

> *Important: This release is a *developer preview* and is free and open-source from Confluent under the Apache 2.0 license.*

# Overview
KSQL is an open source streaming SQL engine that implements continuous, interactive queries against Apache Kafka™. It allows you to query, read, write, and process data in Apache Kafka in real-time, at scale using SQL an SQL-like commands. KSQL interacts directly with the [Kafka Streams API](http://docs.confluent.io/current/streams/concepts.html), removing the requirement of building a Java app. 

### Terminology 
When using KSQL, the following terminology is used.

#### Stream
A stream is an unbounded sequence of structured values that are stored in a Kafka topic. The structure of the values is specified in a schema. In Kafka streams vocabulary, a KSQL stream is a [KStream](http://docs.confluent.io/current/streams/concepts.html?highlight=kstream#kstream) plus a schema. 

#### Table
A table in KSQL is finite, where the bounds are defined by the size of the key space. The key space is an evolving collection of structured values, where the structure of the values is specified in a schema. These values are stored in a changelog topic in Kafka. In Kafka Streams vocabulary, a KSQL table is a [KTable](http://docs.confluent.io/current/streams/concepts.html?highlight=ktable#ktable) plus a schema.

#### Topic
A topic is a category or feed name where records are published. For more information, see the [Apache Kafka documentation](https://kafka.apache.org/documentation/#intro_topics).

### Modes of operation

You can use KSQL in stand-alone mode or in client-server mode.

In stand-alone mode, both the KSQL client and server components are co-located on the same machine, in the same JVM, and are started together which makes it convenient for local development and testing.

![alt text](https://user-images.githubusercontent.com/2977624/29090610-f4b11096-7c34-11e7-8a63-85c9ead22bc3.png)

In client-server mode, you can run a pool of KSQL servers on remote machines, VMs, or containers and the CLI connects to them over HTTP.

![alt text](https://user-images.githubusercontent.com/2977624/29090617-fab5e930-7c34-11e7-9eee-0554192854d5.png)

# Getting Started

* Beginners: Try the [interactive quick start](/quickstart/). The quick start configures a single instance in a lightweight Docker container or in a Kafka cluster. It demonstrates a simple workflow using KSQL to write streaming queries against data in Kafka.
* Advanced users: Try the [end-to-end KSQL demo](https://github.com/confluentinc/ksql).


