---
layout: page
title: Contributing a new ksqlDB client
tagline: Contributing a new ksqlDB client
description: Contribute a new ksqlDB client for your favorite language.
keywords: ksqlDB, client
---

This page contains information and recommendations for how to contribute a new [ksqlDB client](index.md).

Functionality
-------------

Clients should support the following functionality:
- [Push and pull queries](#push-and-pull-queries)
- [Terminating push queries](#terminating-push-queries)
- [Inserting new rows of data into existing ksqlDB streams](#inserting-new-rows-of-data-into-existing-ksqldb-streams)
- [Creation of new streams, tables, and persistent queries](#creation-of-new-streams-tables-and-persistent-queries)
- [Terminating persistent queries](#terminating-persistent-queries)
- [Listing existing streams, tables, topics, and queries](#listing-existing-streams-tables-topics-and-queries)

### Push and pull queries ###

Push and pull queries may be issued by sending requests to the [`/query-stream` server endpoint](TODO).
If no errors are encountered while processing the query statement, the server will write a
metadata row to the response, followed by one or more result rows.

The metadata includes:
- result column names
- result column types, e.g., `STRING`, `INTEGER`, `DOUBLE`, `DECIMAL(4, 2)`, `ARRAY<STRING>`, ``STRUCT<`F1` STRING, `F2` INTEGER>``, etc.
- query ID, in the case of push queries

These metadata fields should be exposed from the client API. In particular, the query ID is used for
[terminating push queries](#terminating-push-queries).

The client representation of result rows should support all of the different data types supported by ksqlDB:

| Data type | Example column type string in metadata |
|-----------|----------------------------------------|
| STRING    | STRING                                 |
| INTEGER   | INTEGER                                |
| BIGINT    | BIGINT                                 |
| DOUBLE    | DOUBLE                                 |
| BOOLEAN   | BOOLEAN                                |
| DECIMAL   | DECIMAL(4, 2)                          |
| ARRAY     | ARRAY<INTEGER>                         |
| MAP       | MAP<STRING, DOUBLE>                    |
| STRUCT    | STRUCT<\`F1\` STRING, \`F2\` INTEGER>  |

Array, map, and struct types may be recursively nested within each other, and may contain any of the other types as well.

Users of the client may wish to receive result rows in different ways:
- Do not block. Rather, perform an action asynchronously on each new row as it arrives
- Block until the next row arrives
- Block until all rows have arrived. This only makes sense in the context of pull queries and push queries with `LIMIT` clauses. 

Details of client interfaces to support these methods will likely vary by language. For example, the [Java client](java-client.md)
uses [Reactive Streams](http://www.reactive-streams.org/) for its asynchronous streaming interface. 

### Terminating push queries ###

Requests to terminate push queries should be sent to the [`/close-query` server endpoint](TODO).
The client should expose an interface that allows users to pass in a push query ID to be passed to this endpoint.

### Inserting new rows of data into existing ksqlDB streams ###

ksqlDB supports inserting new rows of data into existing ksqlDB streams via the [`/inserts-stream` server endpoint](TODO).
If the server successfully validates the target stream name from a request made to the `/inserts-stream` endpoint,
the client may then write JSON-serialized rows of data to the connection, and the server will insert each
asynchronously and write an ack containing the relevant sequence number to the response.

Because the insertions happen asynchronously on the server side, it makes sense for the client to expose APIs
for inserting one row at a time, but inserting batches of rows is less useful due to the lack of transactional guarantees.

The client should provide a convenient way for users to create rows of data to insert.
The Java client's [KsqlObject](TODO) is one example, though details will likely vary by language.

### Creation of new streams, tables, and persistent queries ###

These DDL/DML requests should be sent to the [`/ksql` server endpoint](../ksqldb-rest-api/ksql-endpoint.md).

### Terminating persistent queries ###

These requests should be sent to the [`/ksql` server endpoint](../ksqldb-rest-api/ksql-endpoint.md).

### Listing existing streams, tables, topics, and queries ###

These admin operation requests should be sent to the [`/ksql` server endpoint](../ksqldb-rest-api/ksql-endpoint.md).

Configurations
--------------

The client of course needs to expose options for specifying the address of the ksqlDB server to connect to.
Additional options important to expose include: 
- Support for TLS-enabled ksqlDB servers
- Support for mutual-TLS-enabled ksqlDB servers
- Support for ksqlDB servers with HTTP basic authentication enabled

It may also be nice to support configuring multiple ksqlDB server addresses, so the client may route requests
to a different server if one is down.

Process
-------

Thanks for your interest in contributing a client!

To get started:
- Open [KLIP](../../../design-proposals/README.md) to propose the client you'd like to implement. The KLIP should include a high-level design and example interfaces.
- Contribute code to [ksql repo](https://github.com/confluentinc/ksql)
- Testing: Besides unit tests in the relevant language, there should also be integration tests to spin up a ksqlDB server and validate client behavior. TODO how to make this easier?
- Add a new docs page for the client with example usage to the [ksqlDB clients page](index.md).

