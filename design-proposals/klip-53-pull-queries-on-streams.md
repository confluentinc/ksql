# KLIP 53 - Pull Queries on Streams

**Author**: John Roesler (@vvcephei) | 
**Release Target**: 0.23.1; 7.1.0 | 
**Status**: _Merged_ | 
**Discussion**: _link to the design discussion PR_

**tl;dr:** People should be able to issue pull (ad-hoc, self-terminating) queries on streams,
           just as they can with tables.
           
## Motivation and background

ksqlDB allows the creation of both pull queries (which are issued ad-hoc and run to completion like
a typical database query) and push queries (which are long-running subscriptions that return
results continuously as new input data arrives).
ksqlDB also has a dual table/stream data model. You can issue push queries on tables and streams, 
and you can issue pull queries on tables, but you cannot currently issue pull queries on streams.

Previously, we believed it was unnecessary to implement pull queries on streams because for a
stream, the "current state" is equivalent to the entire history of records. In other words, a
pull query on a stream would return the same thing as a push query on a stream, if it is configured
to start at the beginning of the stream and if it's cancelled once it reaches the end.

In practice, though, it is not clear when you have reached the end of a stream, so you cannot
easily determine when to cancel the query. Additionally, it is cumbersome to have to configure
push queries to start from the beginning of the stream each time you want to issue one of these
pull-type queries.

## What is in scope

* Queries should be supported on STREAM objects without “EMIT CHANGES” at the end.
* Pull queries should start at the beginning of the stream.
* Pull queries should terminate. Streams do not have an "end" by definition, so we need to define
  when the query terminates. When you issue the query, ksqlDB will scan over all the
  data that is in the history of the stream at that moment and then terminate without
  waiting for more data.
* Pull queries on streams will support the same range of query operations as they
  do on tables. In other words, you will be able to filter the stream using a WHERE clause.

## What is not in scope

* Pull queries on streams will not currently support grouping/aggregations or joins.
  This would be useful but is left as future work.
  Note that neither aggregations nor joins are currently supported on table pull queries.
* If old records have already been dropped from the stream due to retention
  settings, they will not be included in the results.
* The scan over the stream may or may not pick up some new records that arrive during the query
  execution. The only guarantee is that the query will include all records that are already in
  the stream at the start of the query. If desired, we can make the end point stricter in the
  future.

## Value/Return

After this feature, you will be able to more easily debug data flows, for example
by inspecting whether a certain record is present in a stream. You will also be able
to interpret a table's backing topic as a stream and look for specific events in its
history. The ability to scan over a topic and apply complex filtering logic will also
enable new use cases, such as looking for certain kinds of events.

## Public APIS

No new syntax is added. The only change is that queries on stream objects will no
longer be required to end in `EMIT CHANGES`.

## Design

In order to support the full query expressiveness of ksqlDB, the current design
is to simply treat pull queries on streams as syntactic sugar. Internally, we will:
1. Create a typical push query
2. Configure the query to start from "earliest" (the beginning of the stream)
3. Find out the end offsets of the stream's partitions
4. Start the push query
5. Monitor the query's progress.
   When it reaches or passes all the offsets in step 3, terminate the query.

## Test plan

We will update the existing tests that validate an error if a pull query is attempted
on a stream. The logic will be updated to expect a valid response. We will also add new
unit and integration tests to ensure the query produces the desired output.

## Documentation Updates

* We will update the docs to remove references
  to the former restriction and document the new ability.
* We should add a new example or amend some current examples
  to show more complex versions than the quickstart does.

## Compatibility Implications

As this is only removing a restriction, no compatibility problems are anticipated.

## Security Implications

This is only adding a more convenient way to issue queries on data that is already
queriable, therefore the existing access control mechanisms are sufficient.
