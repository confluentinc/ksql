---
layout: page
title: Java Client: Receive Query Results as a Single Batch
tagline: Java client: executeQuery
description: The `executeQuery()` method returns query results as a single batch
---

The `executeQuery()` method allows client apps to receive query results as a single batch,
returned once the query has completed.

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

This method is suitable for both pull queries as well as terminating push queries (e.g., those with a `LIMIT` clause).
For non-temrinating push queries, use [the `streamQuery()` method](./stream-query.md) instead. 

Query properties can be passed as an optional second argument. See the [client API reference](TODO) for more.

Example Usage
-------------

```java
final String pullQuery = "SELECT * FROM MY_MATERIALIZED_TABLE WHERE ROWKEY='some_key';";
final BatchedQueryResult batchedQueryResult = client.executeQuery(pullQuery);

final List<Row> resultRows;
try {
  resultRows = batchedQueryResult.get();
} catch (ExecutionException e) {
  System.out.println("Request failed: " + e);
  return;
}

System.out.println("Received results. Num rows: " + resultRows.size());
for (final Row row : resultRows) {
  System.out.println("Row: " + row.values());
}
```
