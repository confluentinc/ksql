---
layout: page
title: Collections
tagline: Streams and tables
description: Learn about ksqlDB's mutable and immutable collections named tables and streams 
keywords: ksqldb, stream, table
---

Collections
===========

Collections provide durable storage for sequences of events. ksqlDB offers
two kinds of collections: streams and tables. Both operate under a simple
key/value model.

Streams
-------

Streams are immutable, append-only collections. They're useful for representing
a series of historical facts. Adding multiple records with the same key means
that they are simply appended to the end of the stream.

Tables
------

Tables are mutable collections. Adding multiple records with the same key means
the table keeps only the value for the last key. They're helpful for modeling
change over time, and they are often used to represent aggregations.


Because ksqlDB leverages {{ site.aktm }} for its storage layer, creating a new
collection equates to defining a stream or a table over a Kafka topic. You can
declare a collection over an existing topic, or you can create a new topic for
the collection at declaration time.

TODO: expand on this

How to create a collection and a topic at the same time.
How to declare a collection over an existing topic.


Page last revised on: {{ git_revision_date }}
