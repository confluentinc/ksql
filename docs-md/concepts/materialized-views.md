---
layout: page
title: Materialized Views
tagline: Query materialized views
description: Learn how to query materialized views by using the SELECT statement 
keywords: ksqldb, query, select
---

Materialized Views
==================

Materialized views are derived representations of streams or tables.
They effectively allow you to create new collections over existing ones.
Materialized views are perpetually kept up to date in real-time as new
events arrive through the collections they are built over. This means that
you can chain materialized views together to create many representations
of the same data. Materialized views are especially useful for maintaining
aggregated tables of data.

TODO: expand on this

How to create a materialized view over an existing collection.
How to create a materialized view from another materialized view.

Page last revised on: {{ git_revision_date }}
