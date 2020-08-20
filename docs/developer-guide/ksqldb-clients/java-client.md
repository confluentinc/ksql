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
        <ksqldb.version>{{ site.release }}</ksqldb.version>
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
        <repository>
            <id>confluent</id>
            <name>Confluent</name>
            <url>https://jenkins-confluent-packages-beta-maven.s3.amazonaws.com/{{ site.ksqldbcpversion }}/1/maven/</url>
        </repository>
    </repositories>

    <pluginRepositories>
        <pluginRepository>
            <id>ksqlDB</id>
            <url>https://ksqldb-maven.s3.amazonaws.com/maven/</url>
        </pluginRepository>
        <pluginRepository>
            <id>confluent</id>
            <url>https://jenkins-confluent-packages-beta-maven.s3.amazonaws.com/{{ site.ksqldbcpversion }}/1/maven/</url>
        </pluginRepository>
    </pluginRepositories>

    <dependencies>
        <dependency>
            <groupId>io.confluent.ksql</groupId>
            <artifactId>ksqldb-api-client</artifactId>
            <version>${ksqldb.version}</version>
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
      from [http://packages.confluent.io/maven/](http://packages.confluent.io/maven/)
      by replacing the repositories in the example POM above with a repository with this
      URL instead. Also update `ksqldb.version` to be a CP version, such as `6.0.0`, instead.

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

Receive query results one row at a time (streamQuery())<a name="stream-query"></a>
----------------------------------------------------------------------------------

The `streamQuery()` method enables client apps to receive query results one row at a time,
either asynchronously via a Reactive Streams subscriber or synchronously in a polling fashion.

```java
public interface Client {

  /**
   * Executes a query (push or pull) and returns the results one row at a time.
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

You can use this method to issue both push and pull queries, but the usage pattern is better for push queries.
For pull queries, consider using the [`executeQuery()`](#execute-query)
method instead.

Query properties can be passed as an optional second argument. For more information, see the [client API reference](api/io/confluent/ksql/api/client/Client.html#streamQuery(java.lang.String,java.util.Map)).

By default, push queries return only newly arriving rows. To start from the beginning of the stream or table,
set the `auto.offset.reset` property to `earliest`.

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

```java
public interface Client {

  /**
   * Executes a query (push or pull) and returns all result rows in a single batch, once the query
   * has completed.
   *
   * @param sql statement of query to execute
   * @return query result
   */
  BatchedQueryResult executeQuery(String sql);
  
  ...
  
}
```

This method is suitable for both pull queries and for terminating push queries,
for example, queries that have a `LIMIT` clause). For non-terminating push queries,
use the [`streamQuery()`](#stream-query)
method instead.

Query properties can be passed as an optional second argument. For more
information, see the [client API reference](api/io/confluent/ksql/api/client/Client.html#executeQuery(java.lang.String,java.util.Map)).

By default, push queries return only newly arriving rows. To start from the beginning of the stream or table,
set the `auto.offset.reset` property to `earliest`.

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
- Terminate persistent queries

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
  System.out.println(stream.getName() + " " + stream.getTopic() + " " + stream.getFormat());
}
``` 

List ksqlDB tables:
```java
List<TableInfo> tables = client.listTables().get();
for (TableInfo table : tables) {
  System.out.println(table.getName() + " " + table.getTopic() + " " + table.getFormat() + " " + table.isWindowed());
}
```

List Kafka topics:
```java
List<TopicInfo> topics = client.listTopics().get();
for (TopicInfo topic : topics) {
  System.out.println(topic.getName() + " " + topic.getPartitions() + " " + topic.getReplicasPerPartition());
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
