---
layout: page
title: Streams 
tagline: The stream concept in ksqlDB
description: Summary of the stream concept in ksqlDB
keywords: stream, partition, key
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/concepts/streams.html';
</script>

A stream is a partitioned, immutable, append-only collection that represents a
series of historical facts. For example, the rows of a stream could model a
sequence of financial transactions, like "Alice sent $100 to Bob", followed by
"Charlie sent $50 to Bob".

Once a row is inserted into a stream, it can never change. New rows can be
appended at the end of the stream, but existing rows can never be updated or
deleted.

Each row is stored in a particular partition. Every row, implicitly or
explicitly, has a key that represents its identity. All rows with the same key
reside in the same partition.
