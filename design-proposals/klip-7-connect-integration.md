# KLIP 7 - Kafka Connect Integration

**Author**: agavra | 
**Release Target**: 5.4 | 
**Status**: _In Discussion_ | 
**Discussion**: link

**tl;dr:** _provide first-class integration with Kafka connect for KSQL data ingress and egress_
           
## Motivation and background

Databases provide three functions:
- Data ingress (i.e. inserting data into the database)
- Data transform (i.e. deriving new data from old)
- Data egress (i.e. querying and/or ETL)

In order to develop KSQL as the streaming database, we must provide a complete story around each of
these functionality. Today, KSQL is a compelling tool to accomplish data transformations but relies
on external mechanisms to import and export data. This KLIP lays out foundations for an integration 
with Kafka Connect, a system that provides powerful primitives for data ingress and egress for 
Apache Kafka.

### Motivating Example

This document will extend the canonical KSQL example of joining pageviews with a user table, but in
this document we will assume that the user table is stored in a JDBC compatible database, as opposed
to a Kafka changelog topic. After implementing this KLIP, it should be possible for a KSQL user to
perform a stream-table join on these two without leaving KSQL's console.

## Scope

This KLIP will:
- define a SQL-like language for basic connector management
- provide a guiding principles and high level architecture for a connect-ksql integration

This KLIP will _not_:
- propose improvements to Kafka Connect such as easing the configuration burden, but will discuss
  how to leverage future Connect improvements

## Value/Return

This proposal targets _usability_ improvements for KSQL and simplifies bootstrapping KSQL for users
that may already have some, but not all, of their data in Kafka. As with the motivating example,
KSQL users will be able to easily model external systems within KSQL. The target user has already
played around with KSQL and is working on prototyping their first real application with existing
data.

## Design

### Architecture

There are three ways to integrate KSQL with Connect:
1. Run them separately and supply syntax to communicate between them
1. Embed Connect into KSQL, managing individual connect workers as part of the KSQL ecosystem
1. Package Connect and KSQL into a single JVM, but just spin them up at the top level

There is some consensus that at scale KSQL and Connect should run as separate components. This
means that the API that we design will not be able to leverage any "embedded-ness" of Connect,
nullifying most of the benefits of the second approach. 

There are still, however, benefits to packaging connect and KSQL in the same JVM for a seamless
experience for new users. Namely, new users will only need to download a single package, spin up
a single process and maintain only a single application. There are some (solvable) concerns with
this approach, but for the reason outlined above, the syntax will be developed with the assumption
that Connect is running as a separate component.

### Syntax

Analogous to DDL and DML, the Connect syntax will separate actions that have side effects with
actions that simply declare metadata on top of existing data. This solves two issues:

- A single connector can have many topics, which makes modeling them as a single statement difficult
- A connector can be modeled in different ways (e.g. stream/table) and connector lifecycle should
  be independent with how KSQL interprets it

In the future, there can exist shortcuts (similar to `C*AS`) to both create connectors and declare 
corresponding streams/tables.

There are a few more minor points of consideration:
- All "DDL" commands that cause changes in connect will not be submitted to the command topic;
  instead they will be issued on the REST server (this follows the pattern that all external-state
  modifications are not distributed)
- To make this work for all vanilla connectors, we will need Connect to implement an API that shows
  all topics created by a connector.

**Sample Ingress**

The first step is to create and start a Connector via the REST API (`POST /connectors`)
```sql
ksql> CREATE EXTERNAL SOURCE my-postgres-jdbc WITH ( \
      connector.class="io.confluent.connect.jdbc.jdbcSourceConnector", \ 
      tasks.max=1, \
      connection.url=jdbc:postgresql://localhost:5432/my-db, \
      ...);

--------------
SOURCE CREATED
--------------
```
*Note: as Connect manages to simplify the configuration, whether through templates or any other
means, we will immediately be able to leverage this simplification in KSQL's WITH clause.*

Next, we provide tools to understand the status of the connector and corresponding topics hitting
the `GET /connectors` and `GET /connectors/<name>` REST endpoints:
```sql
ksql> SHOW EXTERNAL SOURCES;

    name         |              connector                        |   topic prefix
-----------------|-----------------------------------------------|--------------------
my-postgres-jdbc | io.confluent.connect.jdbc.JdbcSourceConnector | test-postgres-jdbc

ksql> DESCRIBE my-postgres-jdbc;

name: my-postgres-jdbc
connector: io.confluent.connect.jdbc.SourceConnector
topic-prefix: test-postgres-jdbc

   worder id     | task | status
-----------------|------|---------
10.200.7.69:8083 | 0    | RUNNING

 name      |     topic                     |  schema
-----------|-------------------------------|--------------------------------------
user-table | test-postgres-jdbc-user-table | id VARCHAR, user VARCHAR, age INTEGER
page-table | test-postgres-jdbc-page-table | id VARCHAR, page VARCHAR, url VARCHAR
```

Then the user can create streams or tables from the underlying topics:
```sql
ksql> CREATE TABLE users_original WITH (kafka_topic = 'test-postgres-jdbc-user-table', ...);

--------------
STREAM CREATED
--------------
```

At this point, assuming the `pageviews_original` stream is already created, we can join the newly
created `users_original` table with the `pageviews_original` stream. Finally, the user can drop and 
terminate the connectors.
```sql
ksql> DROP EXTERNAL SOURCE my-postgres-jdbc TERMINATE;
```

**Sample Egress**

Data egress would work in a similar fashion:
```sql
ksql> CREATE EXTERNAL SINK my-elastic-sink FROM my-table1 WITH (...);

ksql> SHOW EXTERNAL SINKS;

    name         |              connector                        |   topic prefix
-----------------|-----------------------------------------------|--------------------
my-elastic-sink  | io.confluent.connect.elasticsearch.ElasticsearchSinkConnector | elastic-sink

ksql> DESCRIBE my-elastic-sink;

name: my-elastic-sink
connector: io.confluent.connect.elasticsearch.ElasticsearchSinkConnector
topic-prefix: elstic-sink

   worder id     | task | status
-----------------|------|---------
10.200.7.69:8083 | 1    | RUNNING

name           | topic
--------------------------------------------
elasticsearch1 | elastic-sink-elasticsearch1 

ksql> DROP EXTERNAL SINK my-elastic-sink TERMINATE;
```

### Advanced Topics

- **Error Handling**: it is difficult to understand whether a given connector is or is not working,
  and further difficult to debug why it is not working. This is out of scope for this KLIP and
  should be addressed in connect and then exposed externally. There is a class of synchronous errors 
  that can be easily identified (e.g. missing configurations) that can be bubbled up directly to the 
  user at the time when they submit the request.
- **Secret Management**: some connectors (e.g. JDBC connector) may require passing secrets to
  access the database. If this is the case, the MVP recommendation will be to lock down the command
  topic and pass it in-line over HTTPS. Later, we can solve this by allowing references to an
  external secret management service.

## Test plan

The test plan will involve writing a full demo application for the motivating use case, where the
user table is stored in Postgres. Beyond that, integration tests that spin up connect will be
added to ensure that compatibility does not degrade across releases.

## Documentation Updates

TODO

# Compatibility Implications

N/A

## Performance Implications

N/A

## Security Implications

See section on "Secret Management"
