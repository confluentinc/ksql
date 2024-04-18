---
layout: page
title: Queries
tagline: Query streaming data
description: Learn how to query streaming data in ksqlDB
keywords: query, persistent, push, pull
---

There are three kinds of queries in ksqlDB: persistent, push, and pull.
Each gives you different ways to work with rows of events.

## Persistent

Persistent queries are server-side queries that run indefinitely processing
rows of events. You issue persistent queries by deriving
[new streams](/developer-guide/ksqldb-reference/create-stream-as-select/) and
[new tables](/developer-guide/ksqldb-reference/create-table-as-select/)
from existing streams or tables.

## Push

![Illustration of a push query](/img/ksqldb-push-query.svg)

A [push query](/developer-guide/ksqldb-reference/select-push-query) is
a form of query issued by a client that subscribes to a result
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
flows, see [pull Query](/developer-guide/ksqldb-reference/select-pull-query).

Execute a push query by sending an HTTP request to the ksqlDB REST API, and
the API sends back a chunked response of indefinite length.

The result of a push query isn't persisted to a backing {{ site.ak }} topic.
If you need to persist the result of a query to a {{ site.ak }} topic, use a
CREATE TABLE AS SELECT or CREATE STREAM AS SELECT statement.

## Pull

![Illustration of a pull query](/img/ksqldb-pull-query.svg)

A pull query is a form of query issued by a client that retrieves a result as
of "now", like a query against a traditional RDBMS. 

As a dual to the [push query](/developer-guide/ksqldb-reference/select-push-query/) example, a pull query for a geographic
location would ask for the current map coordinates of a particular user.
Because it's a pull query, it returns immediately with a finite result and
closes its connection. This is ideal for rendering a user interface once, at
page load time. It's generally a good fit for any sort of synchronous control
flow.

Pull queries enable you to fetch the current state of a materialized view.
Because materialized views are incrementally updated as new events arrive,
pull queries run with predictably low latency. They're a great match for
request/response flows. For asynchronous application flows, see
[Push Query](/developer-guide/ksqldb-reference/select-push-query/).
Pull queries are expressed using a strict subset of ANSI SQL.

Execute a pull query by sending an HTTP request to the ksqlDB REST API, and
the API responds with a single response.

If you want to learn more about Pull Queries, see [Pull Queries](/developer-guide/ksqldb-reference/select-pull-query).
