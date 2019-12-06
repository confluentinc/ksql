# KLIP 15 - KQSLDB Client and new API

**Author**: Tim Fox (purplefox) | 
**Release Target**: ? | 
**Status**: _In Discussion_ | 
**Discussion**: https://groups.google.com/forum/#!topic/ksql-dev/yxcRlsOsNmo https://groups.google.com/forum/#!topic/ksql-dev/5mLKvtZFs4Y

 For ksqlDB to be a successful project we need to provide an awesome out of the box developer experience
 and it should be super easy and fun to write powerful event streaming applications.
 Currently this is somewhat diffificult to do, and it requires the use of multiple clients and multiple moving parts in order to build
 a simple event sourcing / CQRS style application.
 We should provide a new ksqlDB client that provides that currently missing single interaction point with the event streaming
 platform so that we can improve the application development process.
           
## Motivation and background

In order to increase adoption of ksqlDB we need the developer experience to be as slick as possible. Currently, in order to write
a simple event streaming application with ksqlDB, up to 4 clients may be needed: 

1. An HTTP client to access the current HTTP/REST API
2. A WebSocket client to access the streaming query WebSockets endpoint
3. A JDBC client to access data in JDBC tables that are synced with KSQL via connect, as the current support for
pull queries does not make the data otherwise easily accessible.
4. A Kafka client for producing and consuming messages from topics.

This is a lot for an application to handle and creates a steep learning curve for application developers.

Moreover, our current HTTP API is lacking, especially in the area of streaming query results which makes it difficult to write an application
that uses this API.

This KLIP proposes that we:

* Create a ksqlDB client (initially in Java and JavaScript) as the primary interaction point to the event streaming platform
for an application
* Create a new server side implementation that supports the client and enables the functionality of the client to be delivered in a straightforward
and efficient way
* This will also involve the creation of a new wire protocol for the client<->server communication.
* Migrate the functionality of the current HTTP/REST API over to the new server implementation and retire the parts that are no longer needed.

## What is in scope

### The client

* We will create a new client, initially in Java (and potentially in JavaScript). Clients for other languages such as Python and Go will follow later.
* The client will initially support execution and streaming of queries (both push and pull), inserting of rows into streams, DDL operations and admin operations
such as list and describe.
* The client will support a reactive / async interaction style using Java CompletableFuture and Reactive Streams / JDK9 Flow API (or a copy thereof if we cannot upgrade from Java 8)
* The client will also support a direct / synchronous interaction style.

### The protocol

We will create a new super simple streaming protocol designed to efficiently stream data from server to client or client to server.
HTTP/REST is a poor fit for streaming data as it is designed for a request/response interaction style. Attempting to implement "streaming"
over HTTP/REST usually results in a polling approach which is awkward and inefficient compared to a protocol which is designed with streaming in mind from
the start.

* Binary multiplexed protocol. The protocol will support multiple independent channels over a single connection. We want to discourage clients from opening new
connections for single operations (e.g. a single query) and closing them again. This leads to poor performance and an inefficient use of resources. Multiplexing allows
many queries to be running over a single connection.
* Each channel will implement flow control (back pressure) to prevent any one channel overwhelming the connection.
* The protocol will run over WebSockets - WebSockets clients are available and very easy to use in all languages of interest.
WebSockets are also usable from browsers - this will be very useful with a JavaScript client.
* The protocol will also run over TCP.
* The protocol will support both streaming and request/response semantics
* The protocol will follow tried and tested designs proved over many years by some of the most well known and best performing messaging
 systems in the industry.
* The protocol will be designed for generic flow controlled streaming of data, not limited to KSQL. Therefore it has potential for re-use in other
Confluent projects which might have a need for streamlined high performance streaming. 

### The server

* We will create a new server implementation to host the new protocol.
* The implementation will support equivalents for all the operations that are currently available on the HTTP/REST API
* The implementation will be designed in a reactive / non blocking way in order to provide the best performance / scalability characteristics with
low resource usage. This will also influence the overall threading model of the server and position us with a better, more scalability internal
server architecture that will help future proof the ksqlDB server.
* The problematic parts of the current HTTP API will be removed (query streaming using chunked response and websockets)
* The remainder of the current HTTP API will remain supported as-is using the new implementation.

## What is not in scope

* We are not creating a new RESTful API.
* We are not currently looking to write clients for languages other than Java and JavaScript (although those may follow later)
* We will not initially allow for consuming directly from Kafka topics using the client (although tha may follow later)

## Value/Return

We hope that by providing a delightful, easy to use client, it will enable application developers to easily write powerful
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

* The current chunked streaming and websockets streaming endpoints will be retired.
* The client will provide a new public API
* We will provide a new wire protocol for client<->server communication.

## Design

### The client

* The Java client will provide an API based on JDK 9 flow / reactive streams and completable future
* The networking will be handled by Vert.x (which uses Netty). Both Vert.x and Netty are very widely
used toolkits which are known for great performance, and have few dependencies.
* The client will have minimal transitive jar dependencies - this is important as the client will be embedded in end user applications.

### The server

* The toolkit used on the server side will be Vert.x
* The current Jetty / JAX-RS usage will be retired.
* The current non streaming HTTP/REST endpoints will be migrated to Vert.x - this should modernise and radically simplify and
clarify the server side implementation result in a cleaner implementation and reduced lines of code.
* The current query streaming endpoints (chunked response and Websockets) will be retired.
* Any current Jetty specific plugins (e.g. security plugins) will be migrated to Vert.x
* Vert.x has great support for working with various different network protocols and has has unrivalled performance/scalability
characteristics for a JVM toolkit. It will also set us up well for a fully async / reactive internal threading model in the server that we
should aim towards for the future.

### Implementation plan

There is a lot of work to do here and it should be split up into separate work packages rather than trying to complete it as a single
monolithic piece of work. Some obvious partitions of the work would be:

* Protocol implementation
* Java client implementation. This can be developed using a fake server so there is no dependency on completing the server work first.
* JavaScript client implementation. Again, this can be developed using a fake server
* Server streaming implementation
* Migration of existing endpoints to new server implementation
* Migration of existing plugins to new server implementation.
* Switchover from old API to new API

We should retain the current API implementation until it's ready to be switched over to the new implementation. The new implementation can exist
in the same codebase during development, and only after switchover will the old implementation be removed.
That way we can ensure there is a working API at all times.

## Test plan

We will require unit/module level tests and integration tests for all the new or changed components as per standard best practice.

## Documentation Updates

* We will produce new guide(s) for the Java and JavaScript clients.
* We will produce example applications showing how to use the client in a real app. E.g. using Spring Boot / Vert.x (Java) and Node.js for JavaScript
* The current documentation for the HTTP/REST API can remain more or less as-is with the removal of the problematic parts.
* We will write up a document on the new wire protocol to encourage 3rd parties to implement new clients.
* There may be some server side configuration changes due to the new server implementation.

# Compatibility Implications

The current chunked response query streaming endpoint will be removed so any users currently using that will have to upgrade.

The current websockets query streaming endpoint will be removed so any users currently using that will have to upgrade.
This endpoint is currently undocumented so it's not expected a lot of users are using it. It is used by C3 (?)
so that will need to be migrated to the new API.

## Performance Implications

* Streaming query results with the new client and protocol should provide significantly better performance than
the current websockets or HTTP streaming APIs
* Using Vert.x in general for hosting the API will provide excellent performance and scalability with very low resource usage.

## Security Implications

The new protocol should be available over TLS and we should support the same auth approach as we do with the current API
so there should be no extra security implications.
