---
layout: page
title: Contributing a new ksqlDB client
tagline: Contributing a new ksqlDB client
description: Contribute a new ksqlDB client for your favorite language.
keywords: ksqlDB, client
---

If you're interested in using ksqlDB from a programming language we don't yet support, we've created
this guide to make it easy to add your own [ksqlDB client](index.md).
If you'd like to contribute your client to the main ksqlDB project, see [below](#contributing-your-client-to-the-main-ksqldb-repository). 

Overview
--------

Clients for ksqlDB communicate with ksqlDB servers via HTTP.
Different types of requests are sent to different server endpoints. Some server endpoints
follow the traditional request-response pattern whereas others use HTTP/2 streaming, for example,
to stream the results of push queries back to the client, or to accept a stream of rows
to insert into a ksqlDB stream.

The sections below cover the different types of requests clients should support.
Links to the relevant server endpoint documentation for each type of request include
details about the request and response formats. 

Functionality
-------------

Clients should support the following functionality:
- [Push and pull queries](#push-and-pull-queries)
- [Terminating push queries](#terminating-push-queries)
- [Inserting new rows of data into existing ksqlDB streams](#inserting-new-rows-of-data-into-existing-ksqldb-streams)
- [Listing existing streams, tables, topics, and queries](#listing-existing-streams-tables-topics-and-queries)
- [Creation and deletion of streams and tables](#creation-and-deletion-of-streams-and-tables)
- [Terminating persistent queries](#terminating-persistent-queries)

### Push and pull queries ###

Push and pull queries may be issued by sending HTTP requests to the [`/query-stream` server endpoint](../../developer-guide/ksqldb-rest-api/streaming-endpoint.md#executing-pull-or-push-queries).
As explained in the [server endpoint documentation](../../developer-guide/ksqldb-rest-api/streaming-endpoint.md#executing-pull-or-push-queries),
the body of the HTTP request includes the query statement as well as any relevant query properties.

If no errors are encountered while processing the query statement, the server will write a
metadata row to the response, followed by one or more result rows.

The metadata includes:
- result column names
- result column types, e.g., `STRING`, `INTEGER`, `DOUBLE`, `DECIMAL(4, 2)`, `ARRAY<STRING>`, ``STRUCT<`F1` STRING, `F2` INTEGER>``, etc.
- query ID, in the case of push queries

The client must expose these metadata fields as part of its API. In particular, the query ID is used for
[terminating push queries](#terminating-push-queries).

The client representation of result rows must support all of the different [data types](/reference/sql/data-types)
supported by ksqlDB. The following table contains examples of how different types are represented
in the metadata header returned from the `/query-stream` server endpoint:

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
Though ksqlDB supports [custom type definitions](/reference/sql/data-types#custom-types),
custom types are expanded into base types in the metadata header from the `/query-stream` endpoint,
so no special handling for custom types is required of the client. 

Users of the client may wish to receive result rows in different ways:
- Do not block. Rather, perform an action asynchronously on each new row as it arrives
- Block until the next row arrives
- Block until all rows have arrived. This only makes sense in the context of pull queries and push queries with `LIMIT` clauses. 

Details of client interfaces to support these methods vary by language. For example, the [Java client](java-client.md)
uses [Reactive Streams](http://www.reactive-streams.org/) for its asynchronous streaming interface. 

### Terminating push queries ###

Terminate push queries by sending HTTP requests to the [`/close-query` server endpoint](../../developer-guide/ksqldb-rest-api/streaming-endpoint.md#terminating-queries).
The client must expose an interface that enables users to pass in a push query ID to be passed to this endpoint.
The query ID associated with a push query is returned as part of the server response when a push
query is issued, and must be exposed by the [client method(s) for issuing push queries](#push-and-pull-queries)
in order for the user to terminate the push query. 

As explained in the [server endpoint documentation](../../developer-guide/ksqldb-rest-api/streaming-endpoint.md#terminating-queries),
the body of the HTTP request specifies the query ID. For successful requests, the response body will be empty.

### Inserting new rows of data into existing ksqlDB streams ###

ksqlDB supports inserting new rows of data into existing ksqlDB streams via the [`/inserts-stream` server endpoint](../../developer-guide/ksqldb-rest-api/streaming-endpoint.md#inserting-rows-into-an-existing-stream).

As explained in the [server endpoint documentation](../../developer-guide/ksqldb-rest-api/streaming-endpoint.md#inserting-rows-into-an-existing-stream),
the initial HTTP request body specifies the name of the target stream. If the server successfully
validates this target stream name, the client may then write JSON-serialized rows of data to the
connection. The server inserts each row asynchronously and writes an ack containing the relevant
sequence number to the response.

Because the insertions happen asynchronously on the server side, it makes sense for the client to expose APIs
for inserting one row at a time, but inserting batches of rows is less useful due to the lack of transactional guarantees.

The client should provide a convenient way for users to create rows of data to insert.
The Java client's [KsqlObject](../java-client/api/io/confluent/ksql/api/client/KsqlObject.html) is one example,
though details will vary by language.

### Listing existing streams, tables, topics, and queries ###

Send these admin operation requests to the [`/ksql` server endpoint](../ksqldb-rest-api/ksql-endpoint.md).

The following table contains the statement text that should be sent in the body of the HTTP request
for each type of client admin operation:

| Admin operation             | SQL statement text |
|-----------------------------|--------------------|
| List ksqlDB streams         | LIST STREAMS;      |
| List ksqlDB tables          | LIST TABLES;       |
| List Kafka topics           | LIST TOPICS;       |
| List running ksqlDB queries | LIST QUERIES;      |

The [server endpoint documentation](../ksqldb-rest-api/ksql-endpoint.md)
contains the format of the server response body for each type of request. For example, the response of
a `LIST STREAMS;` command is a JSON object with an array-type field with name `streams`. Each element
of the array is a JSON object with fields `name`, `topic` , `format`, and `type`, each of type string.

### Creation and deletion of streams and tables ###

These DDL/DML statements include:
- Creation of source streams and tables: `CREATE STREAM`, `CREATE TABLE`
- Creation of persistent queries: `CREATE STREAM AS SELECT`, `CREATE TABLE AS SELECT`
- Deletion of streams and tables: `DROP STREAM`, `DROP TABLE`

Send these DDL/DML requests to the [`/ksql` server endpoint](../ksqldb-rest-api/ksql-endpoint.md).

As explained in the [server endpoint documentation](../ksqldb-rest-api/ksql-endpoint.md),
the HTTP request body contains the text of the ksqlDB statement as well as any relevant
query properties. Because ksqlDB supports many different types of statements for creating streams,
tables, and persistent queries, the client interface should accept a SQL statement and pass the statement
to the server endpoint directly.

The server response will contain a command ID, command sequence number, and status information.
These pieces of information typically do not inform actions of ksqlDB users and do not need to be
exposed as part of the client interface as a result.

Note that the `/ksql` server endpoint accepts not only DDL/DML requests as described here, but also
other types of statements including admin operation requests described [above](#listing-existing-streams-tables-topics-and-queries).
If the server response for a SQL statement submitted to the `/ksql` endpoint has HTTP status code 200
but the response body does not fit the form documented above, then the submitted SQL statement did
not correspond to a DDL/DML statement. The client should handle this potential scenario by returning
an error to the user.

### Terminating persistent queries ###

Send these requests to the [`/ksql` server endpoint](../ksqldb-rest-api/ksql-endpoint.md).

The SQL statement that should be sent in the HTTP request body is `TERMINATE <persistent_query_id>;`.

As noted in the [server endpoint documentation](../ksqldb-rest-api/ksql-endpoint.md),
the format of the response body for terminate query requests is the same as for other DDL/DML requests
[above](#creation-and-deletion-of-streams-and-tables).

Configurations
--------------

The client must expose options for specifying the address of the ksqlDB server to connect to.
Additional options that are important to expose include:
- Support for TLS-enabled ksqlDB servers
- Support for mutual-TLS-enabled ksqlDB servers
- Support for ksqlDB servers with HTTP basic authentication enabled.

As an example, users specify options for the Java client via a [ClientOptions](../java-client/api/io/confluent/ksql/api/client/ClientOptions.html)
object that is passed when [creating a Client instance](../java-client/api/io/confluent/ksql/api/client/Client.html#create(io.confluent.ksql.api.client.ClientOptions)).

Additional configuration options that are nice to support include
- Custom HTTP request headers, to support connecting to ksqlDB servers configured with custom
  authentication mechanisms
- Support for configuring multiple ksqlDB server addresses, so the client may route requests
  to a different server if one is down.

Contributing your client to the main ksqlDB repository
------------------------------------------------------

Thanks for your interest in contributing a client!

To get started:
- Open a [KLIP](https://github.com/confluentinc/ksql/blob/master/design-proposals/README.md) to propose the client you'd like to implement. The KLIP should include a high-level design and example interfaces.
- Contribute code to [ksql repository](https://github.com/confluentinc/ksql)
- Testing: Besides unit tests in the relevant language, there should also be integration tests to spin up a ksqlDB server and validate client behavior.
- Add a new docs page for the client with example usage to the [ksqlDB clients page](index.md).

Don't hesitate to reach out in the #ksqldb channel of our community Slack
(found in the [Confluent Community Slack](https://launchpass.com/confluentcommunity))
for guidance at any point in the process. Thanks for your contribution!
