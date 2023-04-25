# KLIP 15 - ksqlDB Client and New Server API

**Author**: Tim Fox (purplefox) | 
**Release Target**: ? | 
**Status**: _In Discussion_ | 
**Discussion**:

Please read the initial discussion on the ksqlDB developer group first:

https://groups.google.com/forum/#!topic/ksql-dev/yxcRlsOsNmo

And there is a partially working prototype for this work described here:

https://groups.google.com/forum/#!topic/ksql-dev/5mLKvtZFs4Y

 For ksqlDB to be a successful project we need to provide an awesome out of the box developer experience
 and it should be super easy and fun to write powerful event streaming applications.
 Currently this is somewhat diffificult to do, and it requires the use of multiple clients and multiple moving parts in order to build
 a simple event sourcing / CQRS style application.
 We should provide a new ksqlDB client that provides that currently missing single interaction point with the event streaming
 platform so that we can improve the application development process.
 To support the client we should provide an updated HTTP2 based API that allows streaming use cases to be handled better.
           
## Motivation and background

In order to increase adoption of ksqlDB we need the developer experience to be as slick as possible. Currently, in order to write
a simple event streaming application with ksqlDB, up to 4 clients may be needed: 

1. An HTTP client to access the current HTTP/REST API
2. A WebSocket client to access the streaming query WebSockets endpoint
3. A JDBC client to access data in JDBC tables that are synced with KSQL via connect, as the current support for
pull queries does not make the data otherwise easily accessible.
4. A Kafka client for producing and consuming records from topics.

This is a lot for an application to handle and creates a steep learning curve for application developers.

Moreover, our current HTTP API is lacking, especially in the area of streaming query results which makes it difficult to write an application
that uses this API.

This KLIP proposes that we:

* Create a ksqlDB client (initially in Java and JavaScript) as the primary interaction point to the event streaming platform
for an application
* Create a new HTTP2 based server API using Vert.x that supports the client and enables the functionality of the client to be delivered in a straightforward
and efficient way
* Migrate the functionality of the current HTTP/REST API over to the new server implementation and retire the parts that are no longer needed.

## What is in scope

### The client

* We will create a new client, initially in Java (and potentially in JavaScript). Clients for other languages such as Python and Go will follow later.
* The client will initially support execution and streaming of queries (both push and pull), inserting of rows into streams, DDL operations and admin operations
such as list and describe.
* The client will support a reactive / async interaction style using Java CompletableFuture and Reactive Streams / JDK9 Flow API (or a copy thereof if we cannot upgrade from Java 8)
* The client will also support a direct / synchronous interaction style.
* The server API will be very simple, based on HTTP2 with a simple text/json based encoding therefore it will be very simple to use directly from vanilla
HTTP2 clients if a ksqldb client is not available

### The Server API

We will create a new, simple HTTP2 API for streaming query results from server to client and
for streaming inserts from client to server. We will use a simple text/JSON encoding for data.
Please note, this is not a REST API, it's a streaming HTTP API. The API does not follow REST principles. REST is inherently designed for request/response (RPC)
style interactions, not streaming.

The API will have the following characteristics:

* Multiplexed (because of HTTP2 multiplexing)
* Back-pressure (because of HTTP2 flow control)
* Text based so easy to use and view results using command line tools such as curl
* Can be used from any vanilla HTTP2 client for any language
* Simple newline based delimitation so easy to parse results on client side
* JSON encoding of data so very easy to parse as most languages have good JSON support
* CORS support
* HTTP basic auth support
* TLS
* Easy to create new clients using the protocol

Please note the parameters are not necessarily exhaustive. The description here is an outline not a detailed
low level design. The low level design will evolve during development, and the web-site API docs
will provide exact and detailed documentation on the endpoints and how to use them.

#### Query streaming

The request method will be a POST.

Requests will be made to a specific URL, e.g. "/query-stream" (this can be configurable)

The body of the request is a JSON object UTF-8 encoded as text, containing the arguments for the
operation (newlines have been added here for the sake of clarity but the real JSON must not contain
 unescaped newlines)

````
{
"sql": "select * from foo", <----- the SQL of the query to execute
"properties": {             <----- Optional properties for the query
    "prop1": "val1",
    "prop2": "val2"
   }
}

````

In the case of a successful query 

````
{
"queryId", "xyz123",                          <---- unique ID of the query, used when terminating the query
"columnNames":["col", "col2", "col3"],        <---- the names of the columns
"columnTypes":["BIGINT", "STRING", "BOOLEAN"] <---- The types of the columns
}
````

Followed by zero or more JSON arrays:

````
[123, "blah", true]
[432, "foo", true]
[765, "whatever", false]
````

Each JSON array or row will be delimited by a newline.

For a pull query the response will be ended by the server once all rows have been written, for
a push query the response will remain open until the connection is closed or the query is explicitly
terminated.

#### Terminating queries

Push queries can be explicitly terminated by the client by making a request to this endpoint

The request method will be a POST.

Requests will be made to a specific URL, e.g. "/close-query"

The body of the request is a JSON object UTF-8 encoded as text, containing the arguments for the
operation (newlines have been added here for the sake of clarity but the real JSON must not contain newlines)

````
{
"queryId": "xyz123", <----- the ID of the query to terminate
}

````
 

#### Inserts

The request method will be a POST.

Requests will be made to a specific URL, e.g. "/insert-stream" (this can be configurable)

The body of the request is a JSON object UTF-8 encoded as text, containing the arguments for the
operation (newlines have been added for clarity, the real JSON must not contain newlines):

````
{
"target": "my-stream" <----- The name of the KSQL stream to insert into
}

````

Followed by zero or more JSON objects representing the values to insert:

````
{
"col1" : "val1",
"col2": 2.3,
"col3", true
}
````
Each JSON object will be separated by a new line.

To terminate the insert stream the client should end the request.

Acks will be written to the response when each row has been
successfully committed to the underlying topic. Rows are committed in the order they are provided.
Each ack in the response is a JSON object, separated by newlines:

````
{"status":"ok","seq":0}
{"status":"ok","seq":2}
{"status":"ok","seq":1}
{"status":"ok","seq":3}
````

A successful ack will contain a field `status` with value `ok`.

All ack responses also contain a field `seq` with a 64 bit signed integer value. This number
corresponds to the sequence of the insert on the request. The first send has sequence `0`, the second
`1`, the third `2`, etc. It allows the client to correlate the ack to the corresponding send.

In case of error, an error response (see below) will be sent. For an error response for a send, the
`seq` field will also be included. 

Please note that acks can be returned in a different sequence to which the inserts were submitted. 

#### Errors

Apropriate status codes will be returned from failed requests. The response will also contain JSON
with further information on the error:

{
    "status": "error"
    "error_code": <Error code>
    "message": <Error Message>
}

#### Non streaming results

The API is designed for efficiently streaming rows from client to server or from server to client.

The amount of data that is streamed in any specific query or insert stream can be huge so we want to
avoid any solution that buffers it all in memory at any one time or requires specialised parsers to
parse.

For this reason we do not provide query results (or accept streams of inserts) by default as a single
JSON object. If we did so we would force users to find and use a streaming JSON parser in order to 
parse the results as they arrive. If no parser is available on their platform they would be forced
to parse the entire result set as a single JSON object in memory - this might not be feasible or
desirable due to memory and latency constraints. Newlines are very easy to parse by virtually every
target platform without having to rely on specialist libraries.

There are, however, some use cases where we can guarantee the results of the query are small, e.g.
when using a limit clause. In this case, the more general streaming use case degenerates into an RPC
use case. In this situation it can be convenient to accept the results as a single JSON object as we
may always want to parse them as a single object in memory. To support this use case we can allow the request
to contain an accept-header specifying the content-type of the response. To receive a response as 
a single JSON object content-type should be specified as 'text/json', for our delimited format we
will specify our own custom content type. The delimited format will be the default, as the API
is primarily a streaming API.

### Migration of existing "REST" API

We will migrate the existing Jetty based "REST" API to the new Vert.x based implementation as-is or with
minor modifications.

We will migrate the existing Jetty specific plug-ins to Vert.x

### Server implementation

The new server API will be implemented using Vert.x

The implementation will be designed in a reactive / non-blocking way in order to provide the best performance / scalability characteristics with
low resource usage. This will also influence the overall threading model of the server and position us with a better, more scalability internal
server architecture that will help future proof the ksqlDB server.

## What is not in scope

* We are not creating a new RESTful API.
* We are not currently looking to write clients for languages other than Java and JavaScript (although those may follow later)
* We will not initially allow for consuming directly from Kafka topics using the client (although tha may follow later)

## Value/Return

We hope that by providing a delightful, easy to use client and new HTTP2 based server API, it will enable application developers to easily write powerful
applications that take advantage of their data plane / event streaming platform more effectively.

We hope that this could be transformative for the project in terms of adoption as it would position ksqlDB as a great choice for
writing typical event sourcing / CQRS / DDD style applications, which are currently hard to write using ksqlDB alone. 

There are also further incidental advantages gained by this work. By using a modern server side implementation such as Vert.x 
there are benefits in relation to performance, scalability, simplicity of implementation, 
and threading model. Not to mention reduced dependencies and better resource usage. This will set us up better for the kinds of high
throughput operations that we will need to implement efficiently now that the project has pivoted to a more active application
facing server rather than a more passive DDL engine.

## Public APIS

The following changes / additions to APIs will occur:

* We will provide a new HTTP2 based streaming API. This will not be accessible using HTTP1.1
* The current chunked streaming and websockets streaming endpoints will be retired.
* The old API will be migrated to Vert.x possibly with some modifications and be accessible over HTTP1.1 and HTTP 2
* The Java client will provide a new public API

## Design

### The client

* The Java client will provide an API based on Reactive Streams and completable future. We can also consider providing a JDK 9 shim using the Flow API for those
users using Java 9+
* The networking will be handled by Vert.x (which uses Netty).
* The client will have minimal transitive jar dependencies - this is important as the client will be embedded in end user applications.
* Client connections are designed to be re-used.
* The client will be thread-safe.

### The server

* The toolkit used on the server side will be Vert.x
* The current Jetty / JAX-RS usage will be retired.
* The current non streaming HTTP/REST endpoints will be migrated to Vert.x - this should modernise and radically simplify and
clarify the server side implementation result in a cleaner implementation and reduced lines of code.
* The current query streaming endpoints (chunked response and Websockets) will be retired.
* Any current Jetty specific plugins (e.g. security plugins) will be migrated to Vert.x
* Vert.x has great support for working with various different network protocols and has has [unrivalled performance/scalability](https://www.techempower.com/benchmarks/)
characteristics for a JVM toolkit. It will also set us up well for a fully async / reactive internal threading model in the server that we
should aim towards for the future.

## Test plan

We will require unit/module level tests and integration tests for all the new or changed components as per standard best practice.

## Documentation Updates

* We will produce new guide(s) for the Java and JavaScript clients.
* We will provide a new guide for the new HTTP API and retire the existing "REST" API documentation.
* We will produce example applications showing how to use the client in a real app. E.g. using Spring Boot / Vert.x (Java) and Node.js (JavaScript)
* There may be some server side configuration changes due to the new server implementation.

# Compatibility Implications

The current chunked response query streaming endpoint will be removed so any users currently using that will have to upgrade.

The current websockets query streaming endpoint will be removed so any users currently using that will have to upgrade.
This endpoint is currently undocumented so it's not expected a lot of users are using it. It is used by C3 (?)
so that will need to be migrated to the new API.

There may be some minor incompatible changes on the migrated old server API.

## Performance Implications

* Streaming query results with the new client and server side implementation should provide significantly better performance than
the current websockets or HTTP streaming APIs
* Using Vert.x in general for hosting the API will provide excellent performance and scalability with very low resource usage.

## Security Implications

The new protocol should be available over TLS and we should support the same auth approach as we do with the current API
so there should be no extra security implications.
