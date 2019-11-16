---
layout: page
title: Pull Queries
tagline: Query instantaneous state
description: Learn how to use point-in-time queries by using the SELECT statement. 
keywords: ksqldb, pull, query, select
---

Pull queries are a form of query issued by a client that retrieve a result as
of "now". As a dual to the push query example, a pull query for a credit score
would be asking for the current score of a particular user. Because it is a
pull query, it returns immediately with a finite result and closes its
connection. This is ideal for rendering a user interface once at page load
time. It's generally a good fit for any sort of synchronous control flow.

Pull queries are expressed using a strict subset of ANSI SQL. They can only be
used to query tables that have been processed into materialized views.
Currently, pull queries only support looking up events by key. They're executed
by running over the ksqlDB REST API. The result of a pull query isn't persisted
anywhere.

Pull queries enable you to fetch the current state of a materialized view.
Because materialized views are incrementally updated as new events arrive,
pull queries run with predictably low latency. They're a great match for
request/response flows. For asynchronous application flows, see
[Push Query](push.md).

Execute a pull query by sending an HTTP request to the ksqlDB REST API, and
the API responds with a single response.

Page last revised on: {{ git_revision_date }}