---
layout: page
---

ksqlDB is an event streaming database purpose-built for stream processing applications. The main focus of stream processing is modeling computation over unbounded streams of events. But what is an event anyway?

An event is anything that happened or occurred. It could be something high-level that happened in a business, like the sale of an item or the submission of an invoice. Or it can be something like low-level, like a log line emitted by a web server when a request is received. Anything that happens at a point in time is an event.

Because events are so fundamental to stream processing, they are ksqlDB’s core unit of data. All of ksqlDB’s features are oriented around making it easy to solve problems using events. But while it’s easy to think about individual events, figuring out how to group related events together is a bit more challenging. Fortunately, the idea of storing related events is well-explored territory. Apache Kafka leads the way, which is why ksqlDB is built directly on top of it.

Apache Kafka is a distributed streaming platform for working with events. It’s horizontally scalable, fault-tolerant, and wicked fast. Although working with it directly can be low level, it has an excellent opinionated approach for modeling both individual events and groups of them together. For this reason, ksqlDB heavily borrows some of Kafka’s abstractions. It doesn’t aim to make you learn all of Kafka, but it also doesn’t reinvent the wheel where there’s already something really good to use.

ksqlDB represents events with a simple key/value model, which is very similar to Kafka’s notion of a record. The key represents some form of identity about the event. The value represents information about the event that occurred. This combination of key and value makes it easy to model groups of events, since multiple events with the same key represent the same identity, irrespective of their values. Events in ksqlDB carry more information than just a key and value, though. Similar to Kafka, they also describe the time at which the event was true.

ksqlDB aims to raise the abstraction from working with a lower level stream processor. Most times, an event is instead called a “row” as if it were a row in a relational database. Each row is composed of a series of columns. Most columns represent fields in the value of an event, but there are a few extra columns. In particular, there are the `ROWKEY` and `ROWTIME` columns that represent the key and time of the event. These columns can be found on every row.