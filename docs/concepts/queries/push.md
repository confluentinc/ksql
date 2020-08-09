---
layout: page
title: Push Queries
tagline: Query state continuously
description: Learn about continuously updated queries by using the SELECT statement. 
keywords: ksqldb, push, query, select
---

![Illustration of a push query](../../img/ksqldb-push-query.svg)

A push query is a form of query issued by a client that subscribes to a result
as it changes in real-time. A good example of a push query is subscribing to a
particular user's geographic location. The query requests the map coordinates,
and because it's a push query, any change to the location is "pushed" over a
long-lived connection to the client as soon as it occurs. This is useful for
building programmatically controlled microservices, real-time apps, or any sort
of asynchronous control flow.

Push queries are expressed using a SQL-like language. They can be used to query
either streams or tables for a particular key. Also, push queries aren't limited
to key look-ups. They support a full set of SQL, including filters, selects,
group bys, partition bys, and joins.

Push queries enable you to query a stream or materialized table with a
subscription to the results. You can subscribe to the output of any query,
including one that returns a stream. A push query emits refinements to a stream
or materialized table, which enables reacting to new information in real-time.
They’re a good fit for asynchronous application flows. For request/response
flows, see [Pull Query](pull.md).

Execute a push query by sending an HTTP request to the ksqlDB REST API, and
the API sends back a chunked response of indefinite length.

The result of a push query isn't persisted to a backing {{ site.ak }} topic.
If you need to persist the result of a query to a {{ site.ak }} topic, use a
CREATE TABLE AS SELECT or CREATE STREAM AS SELECT statement.

Example push query
==================

Specify a push query by using an EMIT refinement clause in a SELECT statement.

The following statement shows how to select five events from a `pageviews`
stream.  

```sql
SELECT * FROM pageviews EMIT CHANGES LIMIT 5;
```

API Reference
=============

- [SELECT (Push Query)](../../developer-guide/ksqldb-reference/select-push-query.md)
- [SELECT (Pull Query)](../../developer-guide/ksqldb-reference/select-pull-query.md)
