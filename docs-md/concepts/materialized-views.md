---
layout: page
keywords: ksqldb, query, select
---

One of the main purposes of tables in any database is to enable efficient queries over the data. We’ve seen how ksqlDB stores events immutability in Kafka with a simple key/value model. How can queries be made efficient under this model? The answer is by leveraging materialized views. How this works requires a bit of an explanation.

Streams and tables share an intimate relationship. A stream is a sequence of events from which a table can be derived. For example, a sequence of credit scores for a loan applicant can change over time. The sequence of credit scores is a stream. But that stream be interpreted as a table to describe the applicant’s current credit score. On the flip side, the table that represents current credit stores is really two things: the current credit scores, and also the sequence of changes to the credit scores for each applicant. This is a profound realization, and much has been written on this stream/table duality.  Many databases throw away the series of changes to a table and only keep the current state around, thus breaking the natural duality.

For our purposes, the important thing that enables efficient queries is access to the current state of applied changes. This is more commonly known as a materialized view. 

More precisely, a materialized view is the result of incrementally-updated aggregation over a stream. When a new event is integrated, the current state of the view evolves into a new state. This transition happens by applying the aggregation function that defines the view with the current state and the new event. In this way, a view is never “fully recomputed” when new events arrive. It simply incrementally adjusts to account for the new information. Thus, queries against materialized views are highly efficient because they simply look up the value of the current state. They do no further work.

In ksqlDB, a table can either be materialized into a view or not. If a table is created directly on top of a Kafka topic, it is not materialized. Non-materialized tables cannot be queried because they would be highly inefficient. On the other hand, if a table is derived from another collection, ksqlDB will materialize its results, and queries can be made against it.

ksqlDB leverages the idea of a stream/table duality by storing both components of each table. The current state of a table is empherally stored locally on a specific server using RocksDB. The series of changes applied to a table is stored durably in a Kafka topic and is replicated across Kafka brokers. If a ksqlDB server with a materialization of a table fails, a new server will rematerialize the table from the changelog in Kafka.
