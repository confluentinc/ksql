---
layout: page
title: Stream Processing
tagline: Process events by using ksqlDB collections
description: Learn how to process events by using ksqlDB collections. 
keywords: ksqldb, collection, query
---

Creating a collection with an enforced schema over a new or existing
{{ site.aktm }} topic is useful but has limited utility by itself for creating
an application. When you declare a collection, you can only work with the
events in their current form. But a critical part of creating streaming
applications is transforming, filtering, joining, and aggregating events.

In ksqlDB, you manipulate events by deriving new collections from existing
ones and describing the changes between them. When a collection is updated with
a new event, ksqlDB updates the collections that are derived from it in
real-time. This rich form of computing is known formally as stream processing,
because it creates programs that operate continually over unbounded streams of
events, *ad infinitum*. These processes stop only when you explicitly terminate
them.

The general pattern for stream processing in ksqlDB is to create a new
collection by using the `SELECT` statement on an existing collection. The
result of the inner `SELECT` feeds into the outer declared collection. You
don't need to declare a schema when deriving a new collection, because ksqlDB
infers the column names and types from the inner `SELECT` statement. The
`ROWKEY` and `ROWTIME` fields of each row remain, unless you override them in
the `SELECT` statement.

Here are a few examples of deriving between the different collection types.

Derive a new stream from an existing stream
-------------------------------------------

Given the following stream:

```sql
CREATE STREAM rock_songs (artist VARCHAR, title VARCHAR)
    WITH (kafka_topic='rock_songs', partitions=2, value_format='avro');
```

You can derive a new stream with all of the song titles transformed to
uppercase:

```sql
CREATE STREAM title_cased_songs AS
    SELECT artist, UCASE(title) AS capitalized_song
    FROM rock_songs
    EMIT CHANGES;
```

Each time a new song is inserted into the `rock_songs` topic, the uppercase
version of the title is appended to the `title_cased_songs` stream.

TODO: Deriving a new stream from an existing table
--------------------------------------------------

TODO: Deriving a new table from an existing stream
--------------------------------------------------

TODO: Deriving a new table from an existing table
-------------------------------------------------

TODO: Deriving a new stream from multiple streams
-------------------------------------------------
