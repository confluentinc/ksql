---
layout: page
title: Queries
tagline: Query event streams
description: Learn how to query event streams by using the SELECT statement. 
keywords: ksqldb, query, select, pull, push, materialized view
---

ksqlDB has a rich set of constructs for both storing events in collections and
deriving new ones through stream processing. Sometimes, you need to process
your events in a real-time stream or by aggregating them together into a
materialized table. In these cases, you need a way for your applications or
microservices to leverage these collections. That's where queries come in.

Push and pull queries
---------------------

Queries enable you to ask questions about collections and materialized views.
ksqlDB supports two different kinds of client-issued queries: push and pull.

- [Pull Queries](pull.md) enable you to look up information at a point in time.
- [Push Queries](push.md) enable you to subscribe to a result as it changes in
  real-time. You can subscribe to the output of any query, including those that
  returns a stream or a materialized aggregate table. 

ksqlDB supports both kinds of queries by using SQL over its REST API. Combining
them enables you to build powerful real-time applications.

API Reference
-------------

- [SELECT (Push Query)](../../developer-guide/ksqldb-reference/select-push-query.md)
- [SELECT (Pull Query)](../../developer-guide/ksqldb-reference/select-pull-query.md)


Page last revised on: {{ git_revision_date }}