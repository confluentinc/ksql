---
layout: page
title: Events
tagline: Events in ksqlDB
description: Learn about events and stream processing in ksqlDB. 
keywords: ksqldb, event, stream
---

<script type="text/javascript">
        window.location = 'https://docs.confluent.io/platform/current/ksqldb/concepts/events.html';
</script>

What is an event?
-----------------

ksqlDB is a database that's purpose-built for stream
processing applications. The main focus of stream processing is modeling
computation over unbounded streams of events.

An event is anything that occurred and was recorded. It could be something
high-level that happened in a business, like the sale of an item or the
submission of an invoice. Or it could be something low-level, like a log line
emitted by a web server when a request is received. Anything that happens at
a point in time is an event.

Because events are so fundamental to stream processing, they are ksqlDB's core
unit of data. All of ksqlDB's features are oriented around making it easy to
solve problems using events. Although it's easy to think about individual
events, figuring out how to store related events together is a bit more
challenging. Fortunately, the idea of storing related events is well-explored
territory. {{ site.aktm }} leads the way, which is why ksqlDB is built directly
on top of it.

{{ site.ak }} is a distributed streaming platform for working with events. It’s
horizontally scalable, fault-tolerant, and extremely fast. Although working
with it directly can be low-level, it has a strong and opinionated approach for
modeling both individual events and stored events. For this reason, ksqlDB
borrows heavily from some of {{ site.ak }}'s abstractions. It doesn’t aim to
make you learn all of {{ site.ak }}, but it also doesn't reinvent the wheel
where there's already something really good to use.

ksqlDB represents events by using a simple key/value model, which is very
similar to {{ site.ak }}'s notion of a record. The key represents some form of
identity about the event. The value represents information about the event that
occurred. This combination of key and value makes it easy to model stored
events, since multiple events with the same key represent the same identity,
irrespective of their values.

But events in ksqlDB carry more information than just a key and value. Similar
to {{ site.ak }}, they also describe the time at which the event was true.

ksqlDB aims to raise the abstraction from working with a lower-level stream
processor. Usually, an event is called a "row", as if it were a row in a
relational database. Each row is composed of a series of columns. Columns
are either read from the event's key or value. ksqlDB also supports 3
pseudo columns, available on every row: `ROWTIME`, which represents the
time of the event, as well as `ROWPARTITION` and `ROWOFFSET`, which represent
the partition and offset of the source record, respectively.
In addition, windowed sources have `WINDOWSTART` and `WINDOWEND` system columns.

Starting in ksqlDB 0.24, you can create columns that are populated by a
record's header field. {{ site.ak }} headers are a list of zero or more key-value pairs,
where the keys are (not necessarily unique) strings and the values are byte arrays.
Headers typically contain metadata about records, which can then be used for routing
or processing For example, the header could store information about how to deserialize
strings in the value.
