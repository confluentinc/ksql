ksqlDB is an event streaming database specifically built for Apache Kafka. Although ksqlDB aims to give you a higher-level set of primitives than Kafka has, it's inevitable that it can't (and shouldn't) be entirely abstracted over. This section aims to describe the minimal number of Kafka concepts that you need to use ksqlDB effectively. For more information, consult the official [Apache Kafka documentation](https://kafka.apache.org/documentation/).

## Records

- The unit of data in Kafka is an event, it's manipulated as something known as a record.
- A record is a container for an event. Every record has a few components:
  - Key
  - Value
  - Timestamp
  - Topic
  - Offset
  - Partition
  - Headers

- Key denotes identity
- Value is traditionally the payload
- Timestamp is when it happened
- Topic is which larger collection it's a part of

These are the important ones for ksqlDB. ksqlDB up-levels this a bit by using "rows" instead of records, but generally carries the K/V model forward. Row is just an abstraction over a record so that you can use it a higher-level way. Key/value columns

## Topics

- Topics are named collections of events.
- Let you hold events of interest together.
- Topics have some number of partitions, which are independent, append-only structures.
- Used to get better parallelism and scale

- ksqlDB up-levels topic to "stream" and "table", which is just a topic with a registered schema. Static typing means fewer mistakes.

## Partitions

- independent, append-only structures.
- Offsets
- depends on who keys what
- mostly invisible in ksqlDB, don't need to worry about who consumes which partitions

- still need to control the key, though
- colocated data is processed together
- repartitioning, either implicit or explicit

- ksqlDB uplevels this with group by / partition by

## Producers and consumers

- Clients that publish and subscribe to records.
- Low level, need to build messages, send them places, manage what you send where

- ksqlDB is both. Continuously producing & consuming on your behalf

## Brokers

- Where the data is stored. Servers to the clients. Manage access to data.

- ksqlDB just points to the brokers like any other application

## Serializers

- How data is transported and stored.
- Kafka just speaks bytes
- Separate for key and value
- Great because it doesnt need to keep up with formats
- Roll up at producer, store on broker, unroll at client
- Errors can happen if producer + consumer don't talk correctly.

- ksqlDB up-levels this by supporting a limited number of serializers out of the box
- Maintains metadata about serializers so that you can't mess this up
- Just say what format you want things in, and it just works.

## Schemas

- Kafka is just bytes, but the data has rhyme and reason
- Helpful to make the schema explicit
- Record explicit schemas somewhere
- Confluent Schema Registry helps you do this. Not Apache, but used lots of places.

- ksqlDB speaks with SR behind the scenes to automatically store and retrieve schemas based on your stream/table schemas
- Can also extract schemas automatically for existing topics
- Makes your ksqlDB schemas centrally available to other programs

## Consumer groups

- Allows you to load balance many partitions over many consumers
- Works dynamically. As you gain/lose consumers, load balancing happens automatically

- ksqlDB builds on this. When you deploy a persistent query, it loads balances the work using a consumer group
- Looks at how many source partitions there are and divides them by the number of available servers.
- e.g. 10 partitions, 2 servers = 5 parts per server. Lose a server, and the other one gets all 10. Add 5 servers, and they all have 2.

## Retention

- How long records stay in a topic's partition.
- Affects how long you can "replay" a topic from the earliest information.
- Retention is a nuanced topic, best to read about it in the docs.
- Still important to call out and know what it is.

## Compaction

- Ditch all but the latest record per key
- Used for tables + state maintainence
- Set automatically for you
- Affects things like joins where they are timestamp dependent
