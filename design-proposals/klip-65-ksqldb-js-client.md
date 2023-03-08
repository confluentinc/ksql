# KLIP 65 - ksqlDB JavaScript Client

**Author**: Javan Ang (@javanang) | Gerry Bong (@ggbong734) | Jonathan Luu @(jonathanluu17) | Michael Snyder (@MichaelCSnyder) | Matthew Xing (@aengil)
**Release Target**: August 2022|
**Status**: _In Development_ |
**Discussion**: TBD

**tl;dr:** Create a ksqlDB client for JavaScript using Node.js. The client should make it easier for JavaScript (Node.JS) developers to adopt ksqlDB and write stream processing applications on Kafka. The client will be published in npm. Please visit our [repo](https://github.com/oslabs-beta/ksqljs/tree/dev) for the latest progress.

## Motivation and background

The goal is simplify the use of ksqlDB in JavaScript by creating an easy-to-use library that would benefit developers looking to implement ksqlDB. Building a streaming architecture today requires putting together multiple subsystems: one to acquire events from existing data sources, another to store them, another to process them, etc. ksqlDB consolidates these subsystems into two components (storage and compute) and provides a simple SQL syntax to interact with the underlying Kafka clusters.

## What is in scope

The JavaScript client will support these basic operations:

- Push query
- Pull query
- Insert rows into an existing stream
- Terminate push query
- List streams/topics/queries
- Create & delete tables/streams

In addition the following methods will also be made available:

- Submit custom SQL statement
- Create table as (to perform aggregate functions and create materialized view )
- Create stream as (to create a stream from another stream )
- Pull from to (pull data from two time points)
- Inspect query/server/cluster status
- Query builder (to prevent SQL injection)

We will include documentation & examples.

At the time of writing, we have completed the core functionality and added documentation. Please visit our [repo](https://github.com/oslabs-beta/ksqljs/tree/dev) for the progress of the client.

We are referencing the [Go client](https://github.com/thmeitz/ksqldb-go) for some of the implementation.

## What is not in scope

We are not creating a new RESTful API. We will be leveraging ksqlDB API.

We can add support for any requested operation in the future. We are open to suggestions!!

At this point, we are not working on adding connector functionality (e.g., `CREATE SINK`, `CREATE SOURCE`).

## Value/Return

We hope that this could be transformative for the ksqlDB project in terms of increasing adoption among JavaScript developers. It would position ksqlDB as a great choice for writing stream processing applications, which are currently hard to write using ksqlDB alone.

## Public APIS

N/A. We are utilizing ksqlDB API.

## Design

The JavaScript client will provide an interface to make requests to ksqlDB APIs based on the axios module and the built-in http2 module in Node.js.

The JavaScript client will be packaged as a npm package (ksql-js) and will be available for download/installation at npm.

## Test plan

We will include integration tests using Jest. Users can spin up a Docker container with an included .yaml config file to run the tests.

## LOEs and Delivery Milestones

TBD

## Compatibility Implications

N/A

## Security Implications

To protect against blind/inferential SQL injections, we are exposing a query builder method in our client to help users parametrize their query. It is advisable for users to validate any input before using our query builder.
