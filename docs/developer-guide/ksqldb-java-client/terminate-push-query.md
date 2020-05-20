---
layout: page
title: Java Client: Terminate a Push Query
tagline: Java client: terminatePushQuery
description: The `terminatePushQuery()` method terminates a push query
---

The `terminatePushQuery()` method allows client apps to terminate push queries.

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

The query ID is obtained from the query result response object when push queries are issued via the client,
via either [`streamQuery()`](./stream-query.md) or [`executeQuery()`](./execute-query.md).

Example Usage
-------------

Here's an example of terminating a push query issued via `streamQuery()`:

```java
final StreamedQueryResult streamedQueryResult;
try {
  streamedQueryResult = client.streamQuery("SELECT * FROM MY_STREAM EMIT CHANGES;").get();
} catch (ExecutionException e) {
  System.out.println("Query request failed: " + e);
  return;
}

final String queryId = streamedQueryResult.queryID();
try {
  client.terminatePushQuery(queryId).get();
} catch (ExecutionException e) {
  System.out.println("Terminate request failed: " + e);
}
```

And here's an analogous example for terminating a push query issued via `executeQuery()`:

```java
final String pullQuery = "SELECT * FROM MY_STREAM EMIT CHANGES LIMIT 10;";
final BatchedQueryResult batchedQueryResult = client.executeQuery(pullQuery);

final String queryId;
try {
  queryId = batchedQueryResult.queryID().get();
} catch (ExecutionException e) {
  System.out.println("Query request failed: " + e);
  return;
}

try {
  client.terminatePushQuery(queryId).get();
} catch (ExecutionException e) {
  System.out.println("Terminate request failed: " + e);
}
```