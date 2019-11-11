---
layout: page
title: Push and Pull Queries
tagline: Query materialized views
description: Learn how to query materialized views by using the SELECT statement 
keywords: ksqldb, query, select
---

Push and Pull Queries
=====================

TODO: expand on this 

API Reference
-------------

- [SELECT (Push Query)](../developer-guide/ksqldb-reference/select-push-query.md)
- [SELECT (Pull Query)](../developer-guide/ksqldb-reference/select-pull-query.md)

Pull Query
----------

Pulls the current value from the materialized table and terminates. The result
of this statement isn't persisted in a Kafka topic and is printed out only in
the console.

Pull queries enable you to fetch the current state of a materialized view.
Because materialized views are incrementally updated as new events arrive,
pull queries run with predictably low latency. They're a great match for
request/response flows. For asynchronous application flows, see
[Push Query](#push-query).

Execute a pull query by sending an HTTP request to the ksqlDB REST API, and
the API responds with a single response.

Push Query
----------

Push a continuous stream of updates to the KSQL stream or table. The result of
this statement isn't persisted in a Kafka topic and is printed out only in
the console. To stop the continuous query in the CLI press Ctrl+C.
Note that the WINDOW clause can only be used if the `from_item` is a
stream.

Push queries enable you to query a materialized view with a subscription to
the results. Push queries emit refinements to materialized views, which enable
reacting to new information in real-time. They’re a good fit for asynchronous
application flows. For request/response flows, see
[Pull Query](#pull-query).

Execute a push query by sending an HTTP request to the ksqlDB REST API, and
the API sends back a chunked response of indefinite length.



Page last revised on: {{ git_revision_date }}
