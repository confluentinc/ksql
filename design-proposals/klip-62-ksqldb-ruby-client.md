# KLIP 62 - ksqlDB Ruby Client

**Author**: Lapo Elisacci (@LapoElisacci) | 
**Release Target**: TBD | 
**Status**: _In Discussion_ | 
**Discussion**: TBD

**tl;dr:** Develop a Ruby HTTP2 Client to request ksqlDB REST API

## Motivation and background

Allow Ruby / Ruby on Rails developers to get started with ksqlDB.

## What is in scope

* A lightweight but complete and configurable Ruby client.
* Support for both synchronous and asynchronous queries.
* Documentation and examples

## What is not in scope

* Anything that concerns the ksqlDB development itself.
* Defining an ORM to dynamically build SQL Statements. (It will get handled by a future project)

## Value/Return

To be able to easily include ksqlDB in any Ruby / Ruby on Rails applications.

## Public APIS

N/A

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
