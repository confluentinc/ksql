One fundamental operation for working with collections is populating them with events. There are several ways that you can do this: you can use the Kafka clients to write data to the underlying topics, or you can use connectors to source data from external systems. But the simplest option is to use ksqlDB’s insert into values syntax.

insert into values inserts an event into an existing stream or table. This works similarly to what you would find in Postgres. You specify the collection to insert into, the sequence of columns for which you have values, and finally the values. The column names and the values are zipped up to form a new event, which is serialized in the same format that the collection is. Here is a quick example that inserts an event with two columns:

```sql
INSERT INTO all_publications (author, title) VALUES ('C.S. Lewis', 'The Silver Chair');
```

Any column not explicitly given a value is set to null. If no columns are specified, a value for every column is expected in the same order as the schema with `ROWKEY` as the first column. If columns are specified, the order doesn’t matter. `ROWTIME` may be specified as an explicit column, but it’s not required when omitting the column specifications.