# Concepts

| [Overview](/docs/) |[Quick Start](/docs/quickstart#quick-start-guide) | Concepts | [Syntax Reference](/docs/syntax-reference.md) | [Examples](/docs/examples.md) | [FAQ](/docs/faq.md)  | [Roadmap](/docs/roadmap.md) | [Demo](/docs/demo.md) |
|---|----|-----|----|----|----|----|----|


## Terminology 
When using KSQL, the following terminology is used.

#### Stream
A stream is an unbounded sequence of structured values that are stored in a [Kafka topic](https://kafka.apache.org/documentation/#intro_topics). The structure of the values is specified in a schema. In Kafka streams vocabulary, a KSQL stream is a [KStream](http://docs.confluent.io/current/streams/concepts.html?highlight=kstream#kstream) plus a schema. 

#### Table
A table in KSQL is finite, where the bounds are defined by the size of the key space. The key space is an evolving collection of structured values, where the structure of the values is specified in a schema. These values are stored in a changelog topic in Kafka. In Kafka Streams vocabulary, a KSQL table is a [KTable](http://docs.confluent.io/current/streams/concepts.html?highlight=ktable#ktable) plus a schema.

## Modes of operation

# Application
In application mode, you can put your KSQL queries in a file and share across your Kakfa Streams instances. 

# Client-server
In client-server mode, you can run a pool of KSQL servers on remote machines, VMs, or containers and the CLI connects to them over HTTP.

![alt text](https://user-images.githubusercontent.com/2977624/29090617-fab5e930-7c34-11e7-9eee-0554192854d5.png)

# Embedded
In embedded mode, you can write KSQL code inside of your streams Java app, using the KSQL context object inside of your application.

# Standalone
In stand-alone mode, both the KSQL client and server components are co-located on the same machine, in the same JVM, and are started together which makes it convenient for local development and testing.

![alt text](https://user-images.githubusercontent.com/2977624/29090610-f4b11096-7c34-11e7-8a63-85c9ead22bc3.png)

  

