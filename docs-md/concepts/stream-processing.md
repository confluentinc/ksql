Creating a collection with an enforced schema over a new or existing Kafka topic is certainly helpful, but this alone has limited utility for creating an application. When a collection is declared, you can only work with the events in their current form. But a critical part of creating streaming applications is transforming, filtering, joining, and aggregating events.

In ksqlDB, this is accomplished by deriving new collections from existing ones and describing the changes between them. When a given collection is updated with a new event, the collections which are derived from it are updated in real-time. This rich form of computing is formally known as stream processing, because it creates programs that continually operate over unbounded streams of events ad infinitum. These processes are only terminated when you explicitly terminate them.

The general pattern for stream processing in ksqlDB is to create a new collection by `SELECT`ing from an existing one. The way to read this is that the result of the inner `SELECT` feeds into outer declared collection. A schema need not be declared when deriving a new collection as the column names and types can be inferred from the inner `SELECT`. Unless overridden in the `SELECT`, the `ROWKEY` and `ROWTIME` of each row remain. A few examples following of deriving between the different collection types:

Deriving a new stream from an existing stream
---------------------------------------------

Given the following stream:

````sql
CREATE STREAM rock_songs (artist VARCHAR, title VARCHAR)
    WITH (kafka_topic='rock_songs', partitions=2, value_format='avro');
```

A new stream can be derived with all of the song titles transformed into uppercase:

```sql
CREATE STREAM title_cased_songs AS
    SELECT artist, UCASE(title) AS capitalized_song
    FROM rock_songs
    EMIT CHANGES;
```

Each time a new song is inserted into `rock_songs`, the uppercase version of the title will be appended to the `title_cased_songs` stream.

Deriving a new stream from an existing table
--------------------------------------------

Deriving a new table from an existing stream
--------------------------------------------

Deriving a new table from an existing table
-------------------------------------------

Deriving a new stream from multiple streams
-------------------------------------------
