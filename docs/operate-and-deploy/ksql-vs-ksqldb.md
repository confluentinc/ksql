---
layout: page
title: KSQL versus ksqlDB
tagline: KSQL versus ksqlDB
description: Key differences between KSQL and ksqlDB.
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/operate-and-deploy/ksql-vs-ksqldb.html';
</script>

!!! note
    For the purposes of this topic, "ksqlDB" refers to ksqlDB 0.6.0 and beyond,
    and "KSQL" refers to all previous releases of KSQL (5.3 and lower).

ksqlDB is not backward compatible with previous versions of KSQL. This means
that, ksqlDB doesn't run over an existing KSQL deployment. This topic provides
an overview of each incompatibility, as well as guidance around migrating a
KSQL deployment to ksqlDB.

- [Syntax](#syntax)
- [Storage](#storage)
- [HTTP Response Format](#http-response-format)
- [SHOW TOPICS](#show-topics)

Syntax
------

With the introduction of a new query form, pull queries, ksqlDB's syntax had to
evolve such that pull queries and push (streaming) queries can be expressed
unambiguously. As a result, one of these two fundamental query forms required
new syntax in order to be identified. Although push queries have existed since
KSQL's inception, we ultimately decided to add new syntax to express them in
ksqlDB so that pull query syntax would remain as standard as possible, because
pull queries are much more likely to be used by external SQL-based systems that
may be integrated with ksqlDB. For more information, see
[KLIP-8: Queryable State Stores](https://github.com/confluentinc/ksql/blob/master/design-proposals/klip-8-queryable-state-stores.md).

This syntax addition is incompatible with previous versions of KSQL. This means
that **a streaming query in KSQL will not be interpreted correctly as a streaming query in ksqlDB**.
Streaming queries in ksqlDB require the `EMIT CHANGES` modifier, which informs
ksqlDB that the caller wants to receive incremental changes to the query result
as it runs perpetually.

The following statements show a comparison between a simple streaming query in
KSQL versus an equivalent streaming query in ksqlDB:

```sql
-- KSQL:
SELECT * FROM my_stream;

-- ksqlDB:
SELECT * FROM my_stream EMIT CHANGES;
```

While streaming KSQL queries will not be interpreted as streaming queries in
ksqlDB, we have taken measures to mitigate the impact of this incompatibility
to the greatest extent possible. In particular, **persistent streaming queries
don't require the `EMIT CHANGES` modifier**. All persistent queries are
interpreted implicitly as if they include an `EMIT CHANGES` modifier. This
implicit behavior means that persistent queries created by using KSQL will run
properly, as is, in ksqlDB.

!!! note
    `EMIT CHANGES` will be required for persistent queries in the future. We
    recommend migrating all of your persistent streaming queries to include
    the EMIT CHANGES modifier.

Storage
--------

ksqlDB introduces changes to the underlying materialization storage that are
incompatible with the storage format of previous KSQL versions. ksqlDB will
therefore not be able to run over an existing KSQL deployment.

HTTP Response Format
-----------------------

In previous KSQL versions, push query responses could return multiple JSON
objects that weren't wrapped in an array, so a collection of objects returned
by the server wasn't a valid JSON document. In ksqlDB, the HTTP response format
for [push queries](/concepts/queries) has changed such that all
responses are now valid JSON documents. Push query results returned by a
ksqlDB server are wrapped in a JSON array.

Also, push query HTTP responses in ksqlDB now include a query result schema
as well as the ID assigned to the query.

Finally, `null` fields are no longer included in push query result rows.

SHOW TOPICS
-----------

In ksqlDB, the output of the `SHOW TOPICS` command no longer includes the
"Consumers" and "ConsumerGroups" columns. To obtain this information, use
the [`SHOW TOPICS EXTENDED`](../developer-guide/ksqldb-reference/show-topics.md)
command.
