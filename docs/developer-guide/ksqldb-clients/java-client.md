---
layout: page
title: Java Client for ksqlDB
tagline: Java client for ksqlDB
description: Send requests to ksqlDB from your Java app
keywords: ksqlDB, java, client
---

ksqlDB ships with a lightweight Java client that enables sending requests easily to a ksqlDB server
from within your Java application, as an alternative to using the [REST API](../api.md).
The client supports pull and push queries; inserting new rows of data into existing ksqlDB streams;
creation and management of new streams, tables, and persistent queries; and also admin operations
such as listing streams, tables, and topics.

!!! tip
    [View the Java client API documentation](api/index.html)

The client sends requests to the recently added HTTP2 server endpoints: pull and push queries are served by
the [`/query-stream` endpoint](../../developer-guide/ksqldb-rest-api/streaming-endpoint.md#executing-pull-or-push-queries),
and inserts are served by the [`/inserts-stream` endpoint](../../developer-guide/ksqldb-rest-api/streaming-endpoint.md#inserting-rows-into-an-existing-stream).
All other requests are served by the [`/ksql` endpoint](../../developer-guide/ksqldb-rest-api/ksql-endpoint.md).
The client is compatible only with ksqlDB deployments that are on version 0.10.0 or later.

Use the Java client to:

- [Receive query results one row at a time (streamQuery())](#stream-query)
- [Receive query results in a single batch (executeQuery())](#execute-query)
- [Terminate a push query (terminatePushQuery())](#terminate-push-query)
- [Insert a new row into a stream (insertInto())](#insert-into)
- [Insert new rows in a streaming fashion (streamInserts())](#stream-inserts)
- [Create and manage new streams, tables, and persistent queries (executeStatement())](#execute-statement)
- [List streams, tables, topics, and queries](#admin-operations)
- [Describe specific streams and tables](#describe-source)
- [Get metadata about the ksqlDB cluster](#server-info)
- [Manage, list and describe connectors](#connector-operations)
- [Define variables for substitution](#variable-substitution)
- [Execute Direct HTTP Requests](#direct-http-requests)
- [Assert the existence of a topic or schema](#assert-topics-schemas)

Get started below or skip to the end for full-fledged [examples](#tutorial-examples).

Getting Started
---------------

Start by creating a `pom.xml` for your Java application:

```xml
<?xml version="1.0" encoding="UTF-8"?>

<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>my.ksqldb.app</groupId>
    <artifactId>my-ksqldb-app</artifactId>
    <version>0.0.1</version>

    <properties>
        <!-- Keep versions as properties to allow easy modification -->
        <java.version>8</java.version>
        <ksqldb.version>{{ site.ksqldbversion }}</ksqldb.version>
        <!-- Maven properties for compilation -->
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <project.reporting.outputEncoding>UTF-8</project.reporting.outputEncoding>
    </properties>

    <repositories>
        <repository>
            <id>ksqlDB</id>
            <name>ksqlDB</name>
            <url>https://ksqldb-maven.s3.amazonaws.com/maven/</url>
        </repository>
    </repositories>

    <pluginRepositories>
        <pluginRepository>
            <id>ksqlDB</id>
            <url>https://ksqldb-maven.s3.amazonaws.com/maven/</url>
        </pluginRepository>
    </pluginRepositories>

    <dependencies>
        <dependency>
            <groupId>io.confluent.ksql</groupId>
            <artifactId>ksqldb-api-client</artifactId>
            <version>${ksqldb.version}</version>
            <classifier>with-dependencies</classifier>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
              <groupId>org.apache.maven.plugins</groupId>
              <artifactId>maven-compiler-plugin</artifactId>
              <version>3.8.1</version>
              <configuration>
                <source>${java.version}</source>
                <target>${java.version}</target>
                <compilerArgs>
                  <arg>-Xlint:all</arg>
                </compilerArgs>
              </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

!!! note
      If you’re using ksqlDB for Confluent Platform (CP), use the CP-specific modules
      from [https://packages.confluent.io/maven/](https://packages.confluent.io/maven/)
      by replacing the repositories in the example POM above with a repository with this
      URL instead. Also update `ksqldb.version` to be a CP version, such as `{{ site.cprelease }}`, instead.

!!! note
      The `with-dependencies` artifact was introduced in ksqlDB version 0.29 and CP version 7.4.0.  This jar includes  
      all the necessary dependencies and relocates most of them in an attempt to avoid classpath issues.  Using this jar
      provides the easiest way to get started.
      If you want more control over the dependencies on the classpath, you can depend directly on the client 
      using this dependency block instead:
      ```
          <dependency>
              <groupId>io.confluent.ksql</groupId>
              <artifactId>ksqldb-api-client</artifactId>
              <version>${ksqldb.version}</version>
          </dependency>
      ```
      If you do this, you will need to add all the transitive dependencies for `ksqldb-api-client`.

Create your example app at `src/main/java/my/ksqldb/app/ExampleApp.java`:

```java
package my.ksqldb.app;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;

public class ExampleApp {

  public static String KSQLDB_SERVER_HOST = "localhost";
  public static int KSQLDB_SERVER_HOST_PORT = 8088;
  
  public static void main(String[] args) {
    ClientOptions options = ClientOptions.create()
        .setHost(KSQLDB_SERVER_HOST)
        .setPort(KSQLDB_SERVER_HOST_PORT);
    Client client = Client.create(options);
    
    // Send requests with the client by following the other examples
    
    // Terminate any open connections and close the client
    client.close();
  }
}
```

For additional client options, see the [API reference](api/io/confluent/ksql/api/client/ClientOptions.html).

You can use the `ClientOptions` class to connect your Java client to
{{ site.ccloud }}. For more information, see
[Connect to a {{ site.ccloud }} ksqlDB cluster](#connect-to-cloud).

Receive query results one row at a time (streamQuery())<a name="stream-query"></a>
----------------------------------------------------------------------------------

The `streamQuery()` method enables client apps to receive query results one row at a time,
either asynchronously via a Reactive Streams subscriber or synchronously in a polling fashion.

You can use this method to issue both push and pull queries, but the usage pattern is better for push queries.
For pull queries, consider using the [`executeQuery()`](#execute-query)
method instead.

Query properties can be passed as an optional second argument. For more information, see the [client API reference](api/io/confluent/ksql/api/client/Client.html#streamQuery(java.lang.String,java.util.Map)).

By default, push queries return only newly arriving rows. To start from the beginning of the stream or table,
set the `auto.offset.reset` property to `earliest`.

```java
public interface Client {

  /**
   * Executes a query (push or pull) and returns the results one row at a time.
   * 
   * <p>This method may be used to issue both push and pull queries, but the usage 
   * pattern is better for push queries. For pull queries, consider using the 
   * {@link Client#executeQuery(String)} method instead.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * @param sql statement of query to execute
   * @return a future that completes once the server response is received, and contains the query
   *         result if successful
   */
  CompletableFuture<StreamedQueryResult> streamQuery(String sql);
  
  ...
  
}
```

### Asynchronous Usage ###

To consume records asynchronously, create a [Reactive Streams](http://www.reactive-streams.org/) subscriber to receive query result rows:

```java
import io.confluent.ksql.api.client.Row;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class RowSubscriber implements Subscriber<Row> {

  private Subscription subscription;

  public RowSubscriber() {
  }

  @Override
  public synchronized void onSubscribe(Subscription subscription) {
    System.out.println("Subscriber is subscribed.");
    this.subscription = subscription;

    // Request the first row
    subscription.request(1);
  }

  @Override
  public synchronized void onNext(Row row) {
    System.out.println("Received a row!");
    System.out.println("Row: " + row.values());

    // Request the next row
    subscription.request(1);
  }

  @Override
  public synchronized void onError(Throwable t) {
    System.out.println("Received an error: " + t);
  }

  @Override
  public synchronized void onComplete() {
    System.out.println("Query has ended.");
  }
}
```

Use the Java client to send the query result to the server and stream results to the subscriber:

```java
client.streamQuery("SELECT * FROM MY_STREAM EMIT CHANGES;")
    .thenAccept(streamedQueryResult -> {
      System.out.println("Query has started. Query ID: " + streamedQueryResult.queryID());
      
      RowSubscriber subscriber = new RowSubscriber();
      streamedQueryResult.subscribe(subscriber);
    }).exceptionally(e -> {
      System.out.println("Request failed: " + e);
      return null;
    });
```

### Synchronous Usage ###

To consume records one-at-a-time in a synchronous fashion, use the `poll()` method on the query result object.
If `poll()` is called with no arguments, it blocks until a new row becomes available or the query is terminated.
You can also pass a `Duration` argument to `poll()`, which causes `poll()` to return `null` if no new rows are received by the time the duration has elapsed.
For more information, see the [API reference](api/io/confluent/ksql/api/client/StreamedQueryResult.html#poll(java.time.Duration)).

```java
StreamedQueryResult streamedQueryResult = client.streamQuery("SELECT * FROM MY_STREAM EMIT CHANGES;").get();

for (int i = 0; i < 10; i++) {
  // Block until a new row is available
  Row row = streamedQueryResult.poll();
  if (row != null) {
    System.out.println("Received a row!");
    System.out.println("Row: " + row.values());
  } else {
    System.out.println("Query has ended.");
  }
}
```

Receive query results in a single batch (executeQuery())<a name="execute-query"></a>
------------------------------------------------------------------------------------

The `executeQuery()` method enables client apps to receive query results as a single batch
that's returned when the query completes.

This method is suitable for both pull queries and for terminating push queries,
for example, queries that have a `LIMIT` clause. For non-terminating push queries,
use the [`streamQuery()`](#stream-query) method instead.

Query properties can be passed as an optional second argument. For more
information, see the [client API reference](api/io/confluent/ksql/api/client/Client.html#executeQuery(java.lang.String,java.util.Map)).

By default, push queries return only newly arriving rows. To start from the beginning of the stream or table,
set the `auto.offset.reset` property to `earliest`.

```java
public interface Client {

  /**
   * Executes a query (push or pull) and returns all result rows in a single batch, once the query
   * has completed.
   * 
   * <p>This method is suitable for both pull queries and for terminating push queries,
   * for example, queries that have a {@code LIMIT} clause. For non-terminating push queries,
   * use the {@link Client#streamQuery(String)} method instead.
   *
   * @param sql statement of query to execute
   * @return query result
   */
  BatchedQueryResult executeQuery(String sql);
  
  ...
  
}
```

### Example Usage ###

```java
String pullQuery = "SELECT * FROM MY_MATERIALIZED_TABLE WHERE KEY_FIELD='some_key';";
BatchedQueryResult batchedQueryResult = client.executeQuery(pullQuery);

// Wait for query result
List<Row> resultRows = batchedQueryResult.get();

System.out.println("Received results. Num rows: " + resultRows.size());
for (Row row : resultRows) {
  System.out.println("Row: " + row.values());
}
```

Terminate a push query (terminatePushQuery())<a name="terminate-push-query"></a>
--------------------------------------------------------------------------------

The `terminatePushQuery()` method enables client apps to terminate push queries.

```java
public interface Client {

  /**
   * Terminates a push query with the specified query ID.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * @param queryId ID of the query to terminate
   * @return a future that completes once the server response is received
   */
  CompletableFuture<Void> terminatePushQuery(String queryId);
  
  ...
  
}
```

The query ID is obtained from the query result response object when the client issues push queries,
by using either the [`streamQuery()`](#stream-query) or [`executeQuery()`](#execute-query) methods.

### Example Usage ###

Here's an example of terminating a push query issued by using the `streamQuery()` method:

```java
String pushQuery = "SELECT * FROM MY_STREAM EMIT CHANGES;";
StreamedQueryResult streamedQueryResult = client.streamQuery(pushQuery).get();

String queryId = streamedQueryResult.queryID();
client.terminatePushQuery(queryId).get();
```

Here's an analogous example for terminating a push query issued by using the `executeQuery()` method:

```java
String pushQuery = "SELECT * FROM MY_STREAM EMIT CHANGES LIMIT 10;";
BatchedQueryResult batchedQueryResult = client.executeQuery(pushQuery);

String queryId = batchedQueryResult.queryID().get();
client.terminatePushQuery(queryId).get();
```

Insert a new row into a stream (insertInto())<a name="insert-into"></a>
-----------------------------------------------------------------------

Client apps can insert a new row of data into an existing ksqlDB stream by using the `insertInto()` method.
To insert multiple rows in a streaming fashion, see [`streamInserts()`](#stream-inserts) instead.

```java
public interface Client {

  /**
   * Inserts a row into a ksqlDB stream.
   *
   * <p>The {@code CompletableFuture} will be failed if a non-200 response is received from the
   * server, or if the server encounters an error while processing the insertion.
   *
   * @param streamName name of the target stream
   * @param row the row to insert. Keys are column names and values are column values.
   * @return a future that completes once the server response is received
   */
  CompletableFuture<Void> insertInto(String streamName, KsqlObject row);
  
  ...
  
}
```

Rows for insertion are represented as `KsqlObject` instances. A `KsqlObject` represents a map of strings
(in this case, column names) to values (column values).

### Example Usage ###

Here's an example of using the client to insert a new row into an existing stream `ORDERS`
with schema `(ORDER_ID BIGINT, PRODUCT_ID VARCHAR, USER_ID VARCHAR)`.

```java
KsqlObject row = new KsqlObject()
    .put("ORDER_ID", 12345678L)
    .put("PRODUCT_ID", "UAC-222-19234")
    .put("USER_ID", "User_321");

client.insertInto("ORDERS", row).get();
```

Insert new rows in a streaming fashion (streamInserts())<a name="stream-inserts"></a>
-------------------------------------------------------------------------------------

Starting with ksqlDB 0.11.0, the `streamInserts()` method enables client apps to insert new rows of
data into an existing ksqlDB stream in a streaming fashion. This is in contrast to the
[`insertInto()`](#insert-into) method which inserts a single row per request.

```java
public interface Client {
  
  /**
   * Inserts rows into a ksqlDB stream. Rows to insert are supplied by a
   * {@code org.reactivestreams.Publisher} and server acknowledgments are exposed similarly.
   *
   * <p>The {@code CompletableFuture} will be failed if a non-200 response is received from the
   * server.
   *
   * <p>See {@link InsertsPublisher} for an example publisher that may be passed an argument to
   * this method.
   *
   * @param streamName name of the target stream
   * @param insertsPublisher the publisher to provide rows to insert
   * @return a future that completes once the initial server response is received, and contains a
   *         publisher that publishes server acknowledgments for inserted rows.
   */
  CompletableFuture<AcksPublisher>
      streamInserts(String streamName, Publisher<KsqlObject> insertsPublisher);
  
  ...
  
}
```

Rows for insertion are represented as `KsqlObject` instances. A `KsqlObject` represents a map of strings
(in this case, column names) to values (column values).

The rows to be inserted are supplied via a [Reactive Streams](http://www.reactive-streams.org/) publisher.
For convenience, the Java client for ksqlDB ships with a simple publisher implementation suitable
for use with the `streamInserts()` method out of the box. This implementation is the [`InsertsPublisher`](api/io/confluent/ksql/api/client/InsertsPublisher.html)
in the example usage below.

As the specified rows are inserted by the ksqlDB server, the server responds with acknowledgments that
may be consumed from the [`AcksPublisher`](api/io/confluent/ksql/api/client/AcksPublisher.html) returned by the `streamInserts()` method.
The `AcksPublisher` is a Reactive Streams publisher.

### Example Usage ###

Here's an example of using the client to insert new rows into an existing stream `ORDERS`,
in a streaming fashion.
The `ORDERS` stream has schema `(ORDER_ID BIGINT, PRODUCT_ID VARCHAR, USER_ID VARCHAR)`.

```java
InsertsPublisher insertsPublisher = new InsertsPublisher();
AcksPublisher acksPublisher = client.streamInserts("ORDERS", insertsPublisher).get();

for (long i = 0; i < 10; i++) {
  KsqlObject row = new KsqlObject()
      .put("ORDER_ID", i)
      .put("PRODUCT_ID", "super_awesome_product")
      .put("USER_ID", "super_cool_user");
  insertsPublisher.accept(row);
}
insertsPublisher.complete();
```

To consume server acknowledgments for the stream of inserts, implement a Reactive Streams subscriber
to receive the acknowledgments:

```java
import io.confluent.ksql.api.client.InsertAck;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class AcksSubscriber implements Subscriber<InsertAck> {

  private Subscription subscription;

  public AcksSubscriber() {
  }

  @Override
  public synchronized void onSubscribe(Subscription subscription) {
    System.out.println("Subscriber is subscribed.");
    this.subscription = subscription;

    // Request the first ack
    subscription.request(1);
  }

  @Override
  public synchronized void onNext(InsertAck ack) {
    System.out.println("Received an ack for insert number: " + ack.seqNum());

    // Request the next ack
    subscription.request(1);
  }

  @Override
  public synchronized void onError(Throwable t) {
    System.out.println("Received an error: " + t);
  }

  @Override
  public synchronized void onComplete() {
    System.out.println("Inserts stream has been closed.");
  }
}
```
and subscribe to the AcksPublisher from above:
```java
acksPublisher.subscribe(new AcksSubscriber());
```

Create and manage new streams, tables, and persistent queries (executeStatement())<a name="execute-statement"></a>
------------------------------------------------------------------------------------------------------------------

Starting with ksqlDB 0.11.0, the `executeStatement()` method enables client apps to:

- Create new ksqlDB streams and tables
- Drop existing ksqlDB streams and tables
- Create new persistent queries, i.e., `CREATE ... AS SELECT` and `INSERT INTO ... AS SELECT` statements
- Pause, Resume, and Terminate persistent queries

```java
public interface Client {
  
  /**
   * Sends a SQL request to the ksqlDB server. This method supports 'CREATE', 'CREATE ... AS
   * SELECT', 'DROP', 'TERMINATE', and 'INSERT INTO ... AS SELECT' statements.
   *
   * <p>Each request should contain exactly one statement. Requests that contain multiple statements
   * will be rejected by the client, in the form of failing the {@code CompletableFuture}, and the
   * request will not be sent to the server.
   *
   * <p>The {@code CompletableFuture} is completed once a response is received from the server.
   * Note that the actual execution of the submitted statement is asynchronous, so the statement
   * may not have been executed by the time the {@code CompletableFuture} is completed.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * @param sql the request to be executed
   * @return a future that completes once the server response is received, and contains the query ID
   *         for statements that start new persistent queries
   */
  CompletableFuture<ExecuteStatementResult> executeStatement(String sql);
  
  ...
  
}
```

To use this method, pass in the SQL for the command to be executed.
Query properties can be passed as an optional second argument. For more information,
see the [client API reference](api/io/confluent/ksql/api/client/Client.html#executeStatement(java.lang.String,java.util.Map)).

As explained in the Javadocs for the method above, the `CompletableFuture` returned by the `executeStatement()`
method is completed as soon as the ksqlDB server has accepted the statement and a response is received
by the client. In most situations, the ksqlDB server will have already executed the statement by this time,
but this is not guaranteed.

For statements that create new persistent queries, the query ID may be retrieved from the returned
`ExecuteStatementResult`, as long as the ksqlDB server version is at least 0.11.0, and the statement
has executed by the time the server response was completed.

### Example Usage ###

Create a new ksqlDB stream, assuming the topic `orders` exists:
```java
String sql = "CREATE STREAM ORDERS (ORDER_ID BIGINT, PRODUCT_ID VARCHAR, USER_ID VARCHAR)"
               + "WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='json');";
client.executeStatement(sql).get();
```

Drop an existing ksqlDB table, assuming the table `USERS` exists:
```java
client.executeStatement("DROP TABLE USERS;").get();
``` 

Start a persistent query that reads from the earliest offset, assuming the stream `ORDERS` exists:
```java
String sql = "CREATE TABLE ORDERS_BY_USER AS "
               + "SELECT USER_ID, COUNT(*) as COUNT "
               + "FROM ORDERS GROUP BY USER_ID EMIT CHANGES;";
Map<String, Object> properties = Collections.singletonMap("auto.offset.reset", "earliest");
ExecuteStatementResult result = client.executeStatement(sql, properties).get();
System.out.println("Query ID: " + result.queryId().orElse("<null>"));
```

Terminate a persistent query, assuming a query with ID `CTAS_ORDERS_BY_USER_0` exists:
```java
client.executeStatement("TERMINATE CTAS_ORDERS_BY_USER_0;").get();
```

List streams, tables, topics, and queries<a name="admin-operations"></a>
------------------------------------------------------------------------

Starting with ksqlDB 0.11.0, the Java client for ksqlDB supports the following admin operations:

- Listing ksqlDB streams, by using the `listStreams()` method
- Listing ksqlDB tables, by using the `listTables()` method
- Listing Kafka topics available for use with ksqlDB, by using the `listTopics()` method
- Listing running ksqlDB queries, with the `listQueries()` method

### Example Usage ###

List ksqlDB streams:
```java
List<StreamInfo> streams = client.listStreams().get();
for (StreamInfo stream : streams) {
  System.out.println(
     stream.getName() 
     + " " + stream.getTopic() 
     + " " + stream.getKeyFormat()
     + " " + stream.getValueFormat()
     + " " + stream.isWindowed()
  );
}
``` 

List ksqlDB tables:
```java
List<TableInfo> tables = client.listTables().get();
for (TableInfo table : tables) {
  System.out.println(
       table.getName() 
       + " " + table.getTopic() 
       + " " + table.getKeyFormat()
       + " " + table.getValueFormat()
       + " " + table.isWindowed()
    );
}
```

List Kafka topics:
```java
List<TopicInfo> topics = client.listTopics().get();
for (TopicInfo topic : topics) {
  System.out.println(
       topic.getName() 
       + " " + topic.getPartitions() 
       + " " + topic.getReplicasPerPartition()
  );
}
```

List running ksqlDB queries:
```java
List<QueryInfo> queries = client.listQueries().get();
for (QueryInfo query : queries) {
  System.out.println(query.getQueryType() + " " + query.getId());
  if (query.getQueryType() == QueryType.PERSISTENT) {
    System.out.println(query.getSink().get() + " " + query.getSinkTopic().get());
  }
}
```

See the [API reference](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-clients/java-client/api/io/confluent/ksql/api/client/Client.html)
for more information.  

Describe specific streams and tables<a name="describe-source"></a>
------------------------------------------------------------------

Starting with ksqlDB 0.12.0, the `describeSource()` method enables client apps
to fetch metadata for existing ksqlDB streams and tables.
The metadata returned from this method includes the stream or table's underlying
topic name, column names and associated types, serialization formats, queries that
read and write from the stream or table, and more. For more details, see the
[API reference](api/io/confluent/ksql/api/client/Client.html#describeSource(java.lang.String)).

### Example Usage ###

Fetch metadata for the stream or table with name `my_source`:
```java
SourceDescription description = client.describeSource("my_source").get();
System.out.println("This source is a " + description.type());
System.out.println("This stream/table has " + description.fields().size() + " columns.");
System.out.println(description.writeQueries().size() + " queries write to this stream/table.");
System.out.println(description.readQueries().size() + " queries read from this stream/table.");
``` 

Get metadata about the ksqlDB cluster<a name="server-info"></a>
---------------------------------------------------------------

Starting with ksqlDB 0.16.0, the `serverInfo()` method enables client apps to fetch metadata about
the ksqlDB cluster. The metadata returned from this method includes the version of ksqlDB the server
is running, the Kafka cluster id and the ksqlDB service id. For more details, see the 
[API reference](api/io/confluent/ksql/api/client/Client.html#serverInfo()).

### Example Usage ###

Fetch server metadata:
```java
ServerInfo serverInfo = client.serverInfo().get();
System.out.println("The ksqlDB version running on this server is " + serverInfo.getServerVersion());
System.out.println("The Kafka cluster this server is using is " + serverInfo.getKafkaClusterId());
System.out.println("The id of this ksqlDB service is " + serverInfo.getKsqlServiceId());
``` 

Manage, list and describe connectors<a name="connector-operations"></a>
-----------------------------------------------------------------------

Starting with ksqlDB 0.18.0, the Java client for ksqlDB supports the following connector operations:
- Creating new connectors by using the [`createConnector()`](/api/io/confluent/ksql/api/client/Client.html#createConnector(java.lang.String,boolean,java.util.Map)) method
- Dropping existing connectors by using the [`dropConnector()`](/api/io/confluent/ksql/api/client/Client.html#dropConnector(java.lang.String)) method
- Listing connectors by using the [`listConnectors()`](/api/io/confluent/ksql/api/client/Client.html#listConnectors()) method
- Describing a specific connector by using the [`describeConnector()`](/api/io/confluent/ksql/api/client/Client.html#describeConnector(java.lang.String)) method

### Example Usage ###

Create a new connector:
```java
Map<String, String> connectorProperties = ImmutableMap.of(
  "connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector",
  "connection.url", "jdbc:postgresql://localhost:5432/my.db",
  "mode", "bulk",
  "topic.prefix", "jdbc-",
  "table.whitelist", "users",
  "key", "username"
);
client.createConnector("jdbc-connector", true, connectorProperties, false).get();
```

Drop a connector:
```java
client.dropConnector("jdbc-connector", true).get();
```

List connectors:
```java
List<ConnectorInfo> connectors = client.listConnectors().get();
for (ConnectorInfo connector : connectors) {
  System.out.println(connector.name()
    + " " + connector.type()
    + " " + connector.className()
    + " " + connector.state()
    + "\n"
  );
}
```

Describe a connector:
```java
ConnectorDescription description = client.describeConnector("jdbc-connector").get();
System.out.println(description.name()
  + " is a " + description.type() + " connector.\n"
  + " The connector's class is " + description.className() + ".\n"
  + " The connector is currently " + description.state() + ".\n"
  + " It reads/writes to " + description.sources().size() + " ksqlDB sources"
  + " and uses " + description.topics().size() + " topics."
);
```

Define variables for substitution<a name="variable-substitution"></a>
---------------------------------------------------------------

Starting with ksqlDB 0.18.0, users can define session variables by calling the [`define()`](/api/io/confluent/ksql/api/client/Client.html#define(java.lang.String,boolean,java.lang.Object)) method and
reference them in other functions by wrapping the variable name in `${}`. The [`undefine()`](/api/io/confluent/ksql/api/client/Client.html#undefine(java.lang.String)) method
undefines a session variable, and [`getVariables()`](/api/io/confluent/ksql/api/client/Client.html#getVariables()) returns a map of the currently defined variables
and their values. Substitution is supported for the following functions:
* `streamQuery`
* `executeQuery`
* `executeStatement`
* `describeSource`
* `createConnector`
* `dropConnector`
* `describeConnector`

### Example Usage ###
Define a new variable:
```java
client.define("topic", "stream-topic");
```

Use a variable in `executeStatement`:
```java
client.executeStatement("CREATE STREAM S (NAME STRING, AGE INTEGER) WITH (kafka_topic='${topic}', value_format='json');");
```

Undefine a variable:
```java
client.undefine("topic");
```

Get all variables:
```java
Map<String, Object> variables = client.getVariables();
```

Execute Direct HTTP Requests<a name="direct-http-requests"></a>
---------------------------------------------------------------

Sometimes, you need to execute requests directly against the ksqlDB server for reasons including, 
but not limited to, accessing features in ksqlDB REST API that are not available in the API client,
or deserializing responses into different classes that are more native to your application.

For this purpose, the Client now adds an `HttpRequest` and `HttpResponse` interface that you can 
use for sending direct requests. 

### Example Usage ###
Call the `/info` endpoint in ksqlDB with: 
```java
HttpResponse response = client.buildRequest("GET", "/info")
    .send()
    .get();

// check status with
assert response.status() == 200;

// parse body (a byte[]) with:
parseIntoJson(response.body())

// or use the helper method to read body into a map:
Map<String, Map<String, Object>> info = response.bodyAsMap();
```

Add query properties variables: 
```java
HttpResponse response = client.buildRequest("POST", "/ksql")
    .payload("ksql", "CREATE STREAM FOO AS CONCAT(A, `wow;`) FROM `BAR`;")
    .propertiesKey("streamsProperties")
    .property("auto.offset.reset", "earliest")
    .send()
    .get();
assert response.status() == 200;
```

Or build the entire payload manually: 
```java
HttpResponse response = client.buildRequest("POST", "/ksql")
    .payload("ksql", "CREATE STREAM FOO AS CONCAT(A, `wow;`) FROM `BAR`;")
    .payload("streamsProperties", Collections.singletonMap("auto.offset.reset", "earliest"))
    .send()
    .get();
assert response.status() == 200;
```

The `send()` method adds authentication headers as specified in 
[ClientOptions](api/io/confluent/ksql/api/client/ClientOptions.html).

Assert the existence of a topic or schema<a name="assert-topics-schemas"></a>
----------------------------------------------------------------------------

Starting with ksqlDB 0.27, users can use the `assertSchema` and `assertTopic` methods to assert the
existence of resources. If the assertion fails, then the method will return a failed `CompletableFuture`.

### Example Usage ###
Assert that a topic and schema exist before creating a stream:

```java
client.assertTopic("foo", ImmutableMap.of("partitions", 1, "replicas", 3), true)
        .thenCompose((a) -> client.assertSchema("foo-key", 3, true))
        .thenAccept((a) -> client.executeStatement("CREATE STREAM..."))
        .exceptionally(...);
```

Assert that a topic doesn't exist:

```java
client.assertTopic("foo", false)
        .thenAccept(() -> client.executeStatement("CREATE STREAM..."))
        .exceptionally(...);
```

Tutorial Examples<a name="tutorial-examples"></a>
-------------------------------------------------

### Event-driven microservice ###

In the [ksqlDB tutorial on creating an event-driven microservice](../../tutorials/event-driven-microservice.md),
the ksqlDB CLI is used to [create a stream for transactions](../../tutorials/event-driven-microservice.md#create-the-transactions-stream),
[seed some transaction events](../../tutorials/event-driven-microservice.md#seed-some-transaction-events),
and [process transaction events into a table and verify output](../../tutorials/event-driven-microservice.md#create-the-anomalies-table).
Here's the equivalent functionality using the Java client for ksqlDB.

Create the transactions stream:

```java
String sql = "CREATE STREAM transactions ("
             + "     tx_id VARCHAR KEY,"
             + "    email_address VARCHAR,"
             + "     card_number VARCHAR,"
             + "     timestamp VARCHAR,"
             + "     amount DECIMAL(12, 2)"
             + ") WITH ("
             + "     kafka_topic = 'transactions',"
             + "     partitions = 8,"
             + "     value_format = 'avro',"
             + "     timestamp = 'timestamp',"
             + "     timestamp_format = 'yyyy-MM-dd''T''HH:mm:ss'"
             + ");";
Map<String, Object> properties = Collections.singletonMap("auto.offset.reset", "earliest");
client.executeStatement(sql, properties).get();
```

Seed some transaction events:

```java
// Create the rows to insert
List<KsqlObject> insertRows = new ArrayList<>();
insertRows.add(new KsqlObject()
   .put("EMAIL_ADDRESS", "michael@example.com")
   .put("CARD_NUMBER", "358579699410099")
   .put("TX_ID", "f88c5ebb-699c-4a7b-b544-45b30681cc39")
   .put("TIMESTAMP", "2020-04-22T03:19:58")
   .put("AMOUNT", new BigDecimal("50.25")));
insertRows.add(new KsqlObject()
   .put("EMAIL_ADDRESS", "derek@example.com")
   .put("CARD_NUMBER", "352642227248344")
   .put("TX_ID", "0cf100ca-993c-427f-9ea5-e892ef350363")
   .put("TIMESTAMP", "2020-04-25T12:50:30")
   .put("AMOUNT", new BigDecimal("18.97")));
insertRows.add(new KsqlObject()
   .put("EMAIL_ADDRESS", "colin@example.com")
   .put("CARD_NUMBER", "373913272311617")
   .put("TX_ID", "de9831c0-7cf1-4ebf-881d-0415edec0d6b")
   .put("TIMESTAMP", "2020-04-19T09:45:15")
   .put("AMOUNT", new BigDecimal("12.50")));
insertRows.add(new KsqlObject()
   .put("EMAIL_ADDRESS", "michael@example.com")
   .put("CARD_NUMBER", "358579699410099")
   .put("TX_ID", "044530c0-b15d-4648-8f05-940acc321eb7")
   .put("TIMESTAMP", "2020-04-22T03:19:54")
   .put("AMOUNT", new BigDecimal("103.43")));
insertRows.add(new KsqlObject()
   .put("EMAIL_ADDRESS", "derek@example.com")
   .put("CARD_NUMBER", "352642227248344")
   .put("TX_ID", "5d916e65-1af3-4142-9fd3-302dd55c512f")
   .put("TIMESTAMP", "2020-04-25T12:50:25")
   .put("AMOUNT", new BigDecimal("3200.80")));
insertRows.add(new KsqlObject()
   .put("EMAIL_ADDRESS", "derek@example.com")
   .put("CARD_NUMBER", "352642227248344")
   .put("TX_ID", "d7d47fdb-75e9-46c0-93f6-d42ff1432eea")
   .put("TIMESTAMP", "2020-04-25T12:51:55")
   .put("AMOUNT", new BigDecimal("154.32")));
insertRows.add(new KsqlObject()
   .put("EMAIL_ADDRESS", "michael@example.com")
   .put("CARD_NUMBER", "358579699410099")
   .put("TX_ID", "c5719d20-8d4a-47d4-8cd3-52ed784c89dc")
   .put("TIMESTAMP", "2020-04-22T03:19:32")
   .put("AMOUNT", new BigDecimal("78.73")));
insertRows.add(new KsqlObject()
   .put("EMAIL_ADDRESS", "colin@example.com")
   .put("CARD_NUMBER", "373913272311617")
   .put("TX_ID", "2360d53e-3fad-4e9a-b306-b166b7ca4f64")
   .put("TIMESTAMP", "2020-04-19T09:45:35")
   .put("AMOUNT", new BigDecimal("234.65")));
insertRows.add(new KsqlObject()
   .put("EMAIL_ADDRESS", "colin@example.com")
   .put("CARD_NUMBER", "373913272311617")
   .put("TX_ID", "de9831c0-7cf1-4ebf-881d-0415edec0d6b")
   .put("TIMESTAMP", "2020-04-19T09:44:03")
   .put("AMOUNT", new BigDecimal("150.00")));

// Insert the rows
List<CompletableFuture<Void>> insertFutures = new ArrayList<>();
for (KsqlObject row : insertRows) {
  insertFutures.add(client.insertInto("TRANSACTIONS", row));
}

// Wait for the inserts to complete
CompletableFuture<Void> allInsertsFuture =
    CompletableFuture.allOf(insertFutures.toArray(new CompletableFuture<?>[0]));
allInsertsFuture.thenRun(() -> System.out.println("Seeded transaction events."));
```

Create the anomalies tables:

```java
String sql = "CREATE TABLE possible_anomalies WITH ("
             + "    kafka_topic = 'possible_anomalies',"
             + "    VALUE_AVRO_SCHEMA_FULL_NAME = 'io.ksqldb.tutorial.PossibleAnomaly'"
             + ")   AS"
             + "    SELECT card_number AS `card_number_key`,"
             + "           as_value(card_number) AS `card_number`,"
             + "           latest_by_offset(email_address) AS `email_address`,"
             + "           count(*) AS `n_attempts`,"
             + "           sum(amount) AS `total_amount`,"
             + "           collect_list(tx_id) AS `tx_ids`,"
             + "           WINDOWSTART as `start_boundary`,"
             + "           WINDOWEND as `end_boundary`"
             + "    FROM transactions"
             + "    WINDOW TUMBLING (SIZE 30 SECONDS, RETENTION 1000 DAYS)"
             + "    GROUP BY card_number"
             + "    HAVING count(*) >= 3"
             + "    EMIT CHANGES;";
Map<String, Object> properties = Collections.singletonMap("auto.offset.reset", "earliest");
client.executeStatement(sql, properties).get();
```

Check contents of the anomalies table with a push query:

```java
String query = "SELECT * FROM possible_anomalies EMIT CHANGES;";
Map<String, Object> properties = Collections.singletonMap("auto.offset.reset", "earliest");
client.streamQuery(query, properties)
    .thenAccept(streamedQueryResult -> {
      System.out.println("Result column names: " + streamedQueryResult.columnNames());
      
      RowSubscriber subscriber = new RowSubscriber();
      streamedQueryResult.subscribe(subscriber);
    }).exceptionally(e -> {
      System.out.println("Push query request failed: " + e);
      return null;
    });
```

In the example above, `RowSubscriber` is the example subscriber implementation introduced in the
[section on the `streamQuery()` method](#stream-query) above. The `RowSubscriber` implementation
can be adapted to adjust how the received rows are printed, or to pass them to a downstream application. 

### Pull queries against a materialized view ###

As a second example, in the ksqlDB tutorial on [building a materialized view/cache](../../tutorials/materialized.md),
the ksqlDB CLI is used to issue [pull queries against materialized views](../../tutorials/materialized.md#query-the-materialized-views)
containing information about customer calls to a call center. Here's a similar set of queries using the Java client for ksqlDB:

```java
String sql1 = "SELECT name, total_calls, minutes_engaged FROM lifetime_view WHERE name = 'derek';";
String sql2 = "SELECT name, total_calls, minutes_engaged FROM lifetime_view WHERE name = 'michael';";

// Execute two pull queries and compare the results
client.executeQuery(sql1).thenCombine(
    client.executeQuery(sql2),
    (queryResult1, queryResult2) -> {
      // One row is returned from each query, as long as the queried keys exist
      Row result1 = queryResult1.get(0);
      Row result2 = queryResult2.get(0);
      if (result1.getLong("TOTAL_CALLS") > result2.getLong("TOTAL_CALLS")) {
        System.out.println(result1.getString("NAME") + " made more calls.");
      } else {
        System.out.println(result2.getString("NAME") + " made more calls.");
      }
      return null;
    });
```

Connect to a {{ site.ccloud }} ksqlDB cluster <a name="connect-to-cloud"></a>
-----------------------------------------------------------------------------

Use the following code snippet to connect your Java client to a hosted ksqlDB
cluster in {{ site.ccloud }}.

```java
ClientOptions options = ClientOptions.create()
 .setBasicAuthCredentials("<ksqlDB-API-key>", "<ksqlDB-API-secret>")
 .setHost("<ksqlDB-endpoint>")
 .setPort(443)
 .setUseTls(true)
 .setUseAlpn(true);
```

Get the API key and endpoint URL from your {{ site.ccloud }} cluster.

- For the API key, see 
  [Create an API key for Confluent Cloud ksqlDB](https://docs.confluent.io/cloud/current/cp-component/ksqldb-ccloud-cli.html#create-an-api-key-for-ccloud-ksql-cloud-through-the-ccloud-cli).
- For the endpoint, run the `ccloud ksql app list` command. For more information,
  see [Access a ksqlDB application in Confluent Cloud with an API key](https://docs.confluent.io/cloud/current/cp-component/ksqldb-ccloud-cli.html#access-a-ksql-cloud-application-in-ccloud-with-an-api-key).

## Suggested Reading

- [ksqlDB Meets Java: An IoT-Inspired Demo of the Java Client for ksqlDB](https://www.confluent.io/blog/ksqldb-java-client-iot-inspired-demo/)
