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
* The Client will handle most of the operations the Java one does:

- Receive query results one row at a time
- Receive query results in a single batch
- Terminate a push query
- Insert a new row into a stream
- Create and manage new streams, tables, and persistent queries
- List streams, tables, topics, and queries
- Describe specific streams and tables
- Get metadata about the ksqlDB cluster
- Manage, list and describe connectors

* Documentation and examples

## What is not in scope

* Anything that concerns the ksqlDB development itself.
* Defining an ORM to dynamically build SQL Statements. (It will get handled by a future project)

## Value/Return

To be able to easily include ksqlDB in any Ruby / Ruby on Rails applications.

## Public APIS

* A class will expose a `config` method to configurate the connection between the client and ksqlDB, like so:

```Ruby
  Ksql.configure do |config|
    config.host = 'http://localhost:8088'
  end
```

### Methods

- **ksql** - Allows statement to be requested against the /ksql endpoint.
- **close_query** - Allows closing persistent queries by requesting the /close-query endpoint.
- **stream** - Allows to perform push queries, processing messages one at a time in a streaming fashion.
- **query** - Allows to perform pull queries.
- **cluster_status** - Allows to introspect the ksqlDB cluster status.
- **health_check** - Allows to check the health of the ksqlDB server.
- **info** - Allows to get information about the status of a ksqlDB Server.
- **terminate** - Allows to terminate the cluster and clean up the resources, or to delete Kafka Topics.

## Design

* A Ruby class will implement the logic to request all ksqlDB REST API available endpoints.
* Request responses will get wrapped inside objects to easily manipulate the returned data.

## Test plan

Both unit and integration tests.
CREATE, DESCRIBE, DROP, INSERT, SELECT (both push and pull), SHOW and TERMINATE statements will get properly tested, to ensure the client always behaves as expected.

The standard Ruby RSpec test suite will do.
Testing requests will get performed against ksqlDB >= 0.22

## LOEs and Delivery Milestones

The Gem's under development and most of the endpoints are already covered.
A first release is expected to be live by the end of March 2022.

Any meaningfull future bug will get handled within a week.

## Compatibility Implications

The client will be compatible with Ruby >= 2.6

## Security Implications

The client will support all protocols supported by the ksqlDB REST API.
