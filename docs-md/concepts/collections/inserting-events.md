---
layout: page
title: Insert events into ksqlDB
tagline: Populate ksqlDB collections with event.
description: Learn how to use the INSERT INTO VALUES statement to add events to a stream or table. 
keywords: ksqldb, collection, event
---

One fundamental operation for working with collections is populating them
with events. There are a number of ways to do this:

- Use ksqlDB’s [INSERT INTO VALUES](../../developer-guide/ksqldb-reference/insert-values.md)
  syntax. This is the simplest approach.
- Use the {{ site.aktm }} clients to write data to the underlying topics. 
- Use connectors to source data from external systems.

The INSERT INTO VALUES statement inserts an event into an existing stream
or table. This statement is similar to what you would find in
[Postgres](https://www.postgresql.org/). You specify:

- the collection to insert values into;
- the sequence of columns that you have values for;
- the values.
 
The column names and values are zipped up to form a new event, which is
serialized in the same format as the collection. The following example
statement inserts an event that has two columns into a collection named
`all_publications`:

```sql
INSERT INTO all_publications (author, title) VALUES ('C.S. Lewis', 'The Silver Chair');
```

Any column that doesn't get a value explicitly is set to `null`. If no columns
are specified, a value for every column is expected in the same order as the
schema, with `ROWKEY` as the first column. If columns are specified, the order
doesn’t matter. You can specify `ROWTIME` as an explicit column, but it’s not
required when you omit the column specifications.

For more information, see
[INSERT INTO VALUES](../../developer-guide/ksqldb-reference/insert-values.md).

Page last revised on: {{ git_revision_date }}