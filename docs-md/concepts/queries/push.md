---
layout: page
title: Push Queries
tagline: Query state continuously
description: Learn about continuously updated queries by using the SELECT statement. 
keywords: ksqldb, push, query, select
---

Push queries are a form of query issued by a client that subscribe to a result
as it changes in real-time. A good example of a push query is subscribing to a
particular user’s credit score. The query requests the value of the credit
score. Because it's a push query, any change to the credit score is "pushed"
to the client as soon as it occurs over a long-lived connection. This is useful
for building programmatically controlled microservices, real-time apps, or any
sort of asynchronous control flow.

Push queries are expressed using a SQL-like language. They can be used to query
either streams or tables for a particular key. They’re executed by running over
the ksqlDB REST API. The result of a push query isn't persisted to a backing
{{ site.ak }} topic.

Push queries enable you to query a materialized view with a subscription to
the results. Push queries emit refinements to materialized views, which enable
reacting to new information in real-time. They’re a good fit for asynchronous
application flows. For request/response flows, see
[Pull Query](pull.md).

Execute a push query by sending an HTTP request to the ksqlDB REST API, and
the API sends back a chunked response of indefinite length.

Page last revised on: {{ git_revision_date }}