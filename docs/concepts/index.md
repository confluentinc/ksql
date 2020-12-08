---
layout: page
title: ksqlDB Concepts
tagline: Foundations of ksqlDB
description: Learn about ksqlDB under the hood.
keywords: ksqldb, architecture, collection, query, schema, window, view
---

![Diagram of ksqlDB architecure](../img/ksqldb-architecture.png)

ksqlDB is built on these conceptual pillars:

- [**Collections:** streams and tables](collections/index.md)
- [**Events:** facts with timestamps](events.md)
- [**Queries:** ask questions about materialized views](queries/index.md)
- [**Stream Processing:** handle events in real time](stream-processing.md)
- [**Materialized Views:** enable efficient queries](materialized-views.md)
- [**Functions:** enhance queries with built-in and custom logic](functions.md)
- [**Connectors:** get data in and send data out](connectors.md)
- [**Schemas:** define the structure of your event data](schemas.md)

More in the **Concepts** section:

- [Architecture](ksqldb-architecture.md)
- [Kafka Streams and kslDB](ksqldb-and-kafka-streams.md)
- [Time and Windows](time-and-windows-in-ksqldb-queries.md)
- [Processing Guarantees](processing-guarantees.md)
- [Evolving Production Queries](upgrades.md)
