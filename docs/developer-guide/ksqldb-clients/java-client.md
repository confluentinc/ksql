---
layout: page
title: Java Client for ksqlDB
tagline: Java client for ksqlDB
description: Send requests to ksqlDB from your Java app
keywords: ksqlDB, java, client
---

ksqlDB ships with a lightweight Java client that enables sending requests easily to a ksqlDB server
from within your Java application, as an alternative to using the [REST API](../api.md).
The client currently supports pull and push queries as well as inserting new rows of data into existing ksqlDB streams.
Soon the client will also support persistent queries and admin operations such as listing streams, tables, and topics. 

The client sends requests to the recently added HTTP2 server endpoints: pull and push queries are served by
the [`/query-stream` endpoint](TODO), and inserts are served by the [`/inserts-stream` endpoint](TODO).
The client is compatible only with ksqlDB deployments that are on version 0.10.0 or later.

Use the Java client to:

- [Receive query results one row at a time (streamQuery())](#stream-query)
- [Receive query results in a single batch (executeQuery())](#execute-query)
- [Terminate a push query (terminatePushQuery())](#terminate-push-query)
- [Insert a new row into a stream (insertInto())](#insert-into)

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
    
    client.close();
  }
}
```

For additional client options, see [the API reference](TODO).

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
   * <p>By default, push queries issued via this method return results starting from the beginning
   * of the stream or table. To override this behavior, use the method
   * {@link #streamQuery(String, Map)} to pass in the query property {@code auto.offset.reset}
   * with value set to {@code latest}.
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
For pull queries, consider [the `executeQuery()` method](./execute-query.md) instead. 

Query properties can be passed as an optional second argument. For more information, see the [client API reference](TODO).

By default, push queries return results starting from the beginning of the stream or table.
To start from the end and receive only newly arriving rows, set the `auto.offset.reset` property to `latest`.

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
For more information, see the [API reference](TODO).

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
   * <p>By default, push queries issued via this method return results starting from the beginning
   * of the stream or table. To override this behavior, use the method
   * {@link #executeQuery(String, Map)} to pass in the query property {@code auto.offset.reset}
   * with value set to {@code latest}.
   *
   * @param sql statement of query to execute
   * @return query result
   */
  BatchedQueryResult executeQuery(String sql);
  
  ...
  
}
```

This method is suitable for both pull queries and for terminating push queries, for example, queries that have a `LIMIT` clause).
For non-terminating push queries, use [the `streamQuery()` method](./stream-query.md) instead.

Query properties can be passed as an optional second argument. For more information, see the [client API reference](TODO).

By default, push queries return results starting from the beginning of the stream or table.
To start from the end and receive only newly arriving rows, set the `auto.offset.reset` property to `latest`.

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
by using either the [`streamQuery()`](./stream-query.md) or [`executeQuery()`](./execute-query.md) methods.

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

Client apps can insert new rows of data into existing ksqlDB streams by using the `insertInto()` method.

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
with schema (ORDER_ID BIGINT, PRODUCT_ID VARCHAR, USER_ID VARCHAR).

```java
KsqlObject row = new KsqlObject()
    .put("ORDER_ID", 12345678L)
    .put("PRODUCT_ID", "UAC-222-19234")
    .put("USER_ID", "User_321");

client.insertInto("ORDERS", row).get();
```

Tutorial Examples
-----------------

In the [ksqlDB tutorial on creating an event-driven microservice](../../tutorials/event-driven-microservice.md),
the ksqlDB CLI is used to [seed some transaction events](../../tutorials/event-driven-microservice.md#seed-some-transaction-events)
into a stream of transactions. Here's the equivalent functionality using the Java client for ksqlDB:

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
