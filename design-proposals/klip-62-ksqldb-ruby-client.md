# KLIP 62 - ksqlDB Ruby Client

**Author**: Lapo Elisacci (@LapoElisacci) | 
**Release Target**: TBD | 
**Status**: _In Discussion_ | 
**Discussion**: TBD

**tl;dr:** Develop a Ruby HTTP2 Client to request ksqlDB REST API

## Motivation and background

Allow Ruby / Ruby on Rails developers to get started with ksqlDB.

## What is in scope

* A lightweight but complete Ruby client, kslqDB host and port will be configurable.
* The Client will handle all the operations the Java one already does:

- Receive query results one row at a time
- Receive query results in a single batch
- Terminate a push query
- Insert a new row into a stream
- Insert new rows in a streaming fashion
- Create and manage new streams, tables, and persistent queries
- List streams, tables, topics, and queries
- Describe specific streams and tables
- Get metadata about the ksqlDB cluster
- Manage, list and describe connectors
- Define variables for substitution
- Execute Direct HTTP Requests

* Pull and push queries will be performed against the `/query-stream` endpoint, and inserts to the `/inserts-stream` endpoint. All other requests to the `/ksql` endpoint. 

* Documentation and examples

## What is not in scope

* Anything that concerns the ksqlDB development itself.
* Defining an ORM to dynamically build SQL Statements. (It will get handled by a future project)

## Value/Return

To be able to easily include ksqlDB in any Ruby / Ruby on Rails applications.

## Public APIS

* A class will expose a `config` method to configurate the connection between the client and ksqlDB, like so:

```Ruby
  KsqlDB::Client.configure do |config|
    config.host = 'http://localhost'
    config.port = 8088
  end
```

* A class method like `streamQuery()` will be available to query results one row at a time.
* A class method like `executeQuery()` will handle batched results queries.
* A class method like `terminatePushQuery()` will allow push queries termination.

More yet to come...


## Design

* A Ruby class will implement the logic to request all ksqlDB REST API available endpoints.
* Request responses will get wrapped inside objects to easily manipulate the returned data.

## Test plan

Both unit and integration tests.
All currenlty available ksqlDB statements will get properly tested.

## LOEs and Delivery Milestones

TBD

## Compatibility Implications

The client will be compatible with Ruby >= 2.6

## Security Implications

The client will support all protocols supported by the ksqlDB REST API.
