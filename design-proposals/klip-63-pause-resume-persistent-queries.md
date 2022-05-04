# KLIP-63: PAUSE and RESUME for persistent queries 

**Author**: Jim Hughes (jnh5y) | 
**Release Target**: Unknown | 
**Status**: In Discussion | 
**Discussion**: 

**tl;dr:** Adds the ability to pause the processing of a persistent query.
           
## Motivation and background

Users may wish to pause a query for a number of reasons and then resume processing.  Presently, they would have to terminate a query and recreate it.  This approach means that any state would have to be restored; the goal of this work is to add the capability to pause processing temporarily while allowing Kafka consumers and state stores to be retained.

## What is in scope

Pausing and resuming persistent queries and the necessary language modifications to support this are in scope.

## What is not in scope

We are not considering the ability to pause and resume transient queries.  Additionally, modifying the consumers' state is out of scope.  (This would allow for processing to skip records or reprocess data.)

## Value/Return

End users will be able to manage query processing once this feature is implemented.  For example, this will allow for better control while designing data pipelines.  Pausing an upstream query will allow users to update a downstream query or other processing and iterate through design without processing all the data in a stream.

## Public APIS

SQL Grammar changes:
Add PAUSE <queryID>, PAUSE ALL, RESUME <queryID>, RESUME ALL.

## Design

For `PAUSE <queryID>` and `RESUME <queryID>`, appropriate methods on the `KafkaStreams` client will be called to pause or 
resume (respectively) the topology associated with the query.

For `PAUSE ALL` and `RESUME ALL`, all persistent queries will be paused or resumed.

## Test plan

Integration tests will show that 
- Pausing a query moves it from the RUNNING state to the PAUSED state.
- Queries in the PAUSED state do not process data.
- Paused queries can be resumed.  Once resumed, they will be in the running state and process data.
- Pausing a query does not impact other queries managed by the system.  (This will with and without shared runtimes enabled.)
- Queries can be paused when multiple ksqlDB servers are in use.  (This demonstrates that the PAUSE messages use the ksqlDB command topic.)
- After a server restart, paused queries will remain in the PAUSED state.  No processing should happen during the server restart.

## LOEs and Delivery Milestones

Delivery should be a small number of PRs.

## Documentation Updates

Pages should be made for the two new verbs (PAUSE and RESUME) like (TERMINATE)[https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/terminate/].

## Compatibility Implications

No compatibility considerations.

## Security Implications

No security considerations.
