---
layout: page
title: Java Client: Receive Query Results One Row at a Time
tagline: Java client: streamQuery
description: The `streamQuery()` method returns query results one row at a time
---

The `streamQuery()` method allows client apps to receive query results one row at a time,
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

This method may be used to issue both push and pull queries, though the usage pattern is most suited for push queries.
For pull queries, consider [the `executeQuery()` method](./execute-query.md) instead. 

Query properties can be passed as an optional second argument. See the [client API reference](TODO) for more.

Asynchronous Usage
------------------

To consume records in an asynchronous fashion, first create a Reactive Streams subscriber to receive query result rows:

```java
import io.confluent.ksql.api.client.Row;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

private static class RowSubscriber implements Subscriber<Row> {

  private Subscription subscription;

  public RowSubscriber() {
  }

  @Override
  public synchronized void onSubscribe(final Subscription subscription) {
    System.out.println("Subscriber is subscribed.");
    this.subscription = subscription;
  }

  @Override
  public synchronized void onNext(final Row row) {
    System.out.println("Received a row!");
    System.out.println("Row: " + row.values());
  }

  @Override
  public synchronized void onError(final Throwable t) {
    System.out.println("Received an error: " + t);
  }

  @Override
  public synchronized void onComplete() {
    System.out.println("Query has ended.");
  }

  public Subscription getSubscription() {
    return subscription;
  }
}
```

Then, use the client to send the query result to the server and stream results to the subscriber:

```java
client.streamQuery("SELECT * FROM MY_STREAM EMIT CHANGES;")
    .thenAccept(streamedQueryResult -> {
      System.out.println("Query has started. Query ID: " + streamedQueryResult.queryID());
      
      final RowSubscriber subscriber = new RowSubscriber();
      streamedQueryResult.subscribe(subscriber);
      subscriber.getSubscription().request(10);
    }).exceptionally(e -> {
      System.out.println("Request failed: " + e);
      return null;
    });
```

Synchronous Usage
-----------------

To consume records one-at-a-time in a synchronous fashion, use the `poll()` method on the query result object.

```java
final StreamedQueryResult streamedQueryResult;
try {
  streamedQueryResult = client.streamQuery("SELECT * FROM MY_STREAM EMIT CHANGES;").get();
} catch (ExecutionException e) {
  System.out.println("Request failed: " + e);
  return;
}

Row row;
for (int i = 0; i < 10; i++) {
  row = streamedQueryResult.poll();
  if (row != null) {
    System.out.println("Received a row!");
    System.out.println("Row: " + row.values());
  } else {
    System.out.println("Query has ended.");
  }
}
```
