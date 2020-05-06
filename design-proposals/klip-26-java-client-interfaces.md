# KLIP 26 - Java client interfaces

**Author**: Victoria Xia (@vcrfxia) | 
**Release Target**: ksqlDB 0.10.0 | 
**Status**: _In development_ | 
**Discussion**: TBD

**tl;dr:** _[KLIP 15](./klip-15-new-api-and-client.md) already made the case for why it makes sense
           to introduce a Java client for ksqlDB. This KLIP proposes interfaces for the client._

## Motivation and background

See [KLIP 15](./klip-15-new-api-and-client.md).

## What is in scope

The Java client will support the following operations:
* Push query
* Pull query
* Select DDL operations:
    * `CREATE STREAM`
    * `CREATE TABLE`
    * `CREATE STREAM ... AS SELECT`
    * `CREATE TABLE ... AS SELECT`
    * `DROP STREAM`
    * `DROP TABLE`
    * `INSERT INTO`
    * `TERMINATE <queryId>`
    * `CREATE CONNECTOR`
    * `DROP CONNECTOR`
* Select admin operations:
    * `SHOW TOPICS` (non-extended)
    * `SHOW STREAMS` (non-extended)
    * `SHOW TABLES` (non-extended)
    * `SHOW QUERIES` (non-extended)
    * `SHOW CONNECTORS`
* Insert values, i.e., insert rows into an existing stream/table
* Terminate push query (via the `/close-query` endpoint)

The purpose of this KLIP is to reach agreement on the interfaces / public APIs.
Implementation details will not be covered.

## What is not in scope

This KLIP does not cover Java client support for the following:
* `DESCRIBE <stream/table>`, `DESCRIBE CONNECTOR`, `DESCRIBE FUNCTION`
* `EXPLAIN <queryId>`
* `PRINT <topic>`
* `SHOW TOPICS EXTENDED`, `SHOW <STREAMS/TABLES> EXTENDED`, `SHOW QUERIES EXTENDED`
* `CREATE TYPE`, `DROP TYPE`, `SHOW TYPES`
* `SHOW FUNCTIONS`, `SHOW PROPERTIES`
* `RUN SCRIPT`
* Use of other endpoints (info, healthcheck, terminate cluster, status, etc.)

We can always add support for these operations in the future if desired.

As above, implementation details are out of scope as the purpose of this KLIP is to reach agreement
on the interfaces / public APIs.

## Value/Return

See [KLIP 15](./klip-15-new-api-and-client.md).

## Public APIS

The following subsections describe the methods of the `Client` interface:
```
public interface Client {
    ...
}
```

### Constructors
```
  static Client create(ClientOptions clientOptions) {
    return new ClientImpl(clientOptions);
  }

  static Client create(ClientOptions clientOptions, Vertx vertx) {
    return new ClientImpl(clientOptions, vertx);
  }
```

The Java client will be implemented as a Vert.x HttpClient. We expose a constructor that allows users to provide their own `Vertx` instance
in order to take advantage of a shared connection pool and other properties if desired.

`ClientOptions` will initially be as follows:
```
public interface ClientOptions {

  ClientOptions setHost(String host);

  ClientOptions setPort(int port);

  ClientOptions setUseTls(boolean useTls);

  ClientOptions setUseClientAuth(boolean useClientAuth);

  ClientOptions setTrustStore(String trustStorePath);

  ClientOptions setTrustStorePassword(String trustStorePassword);

  ClientOptions setKeyStore(String keyStorePath);

  ClientOptions setKeyStorePassword(String keyStorePassword);

  ClientOptions setBasicAuthCredentials(String username, String password);

  String getHost();

  int getPort();

  boolean isUseTls();

  boolean isUseClientAuth();

  boolean isUseBasicAuth();

  String getTrustStore();

  String getTrustStorePassword();

  String getKeyStore();

  String getKeyStorePassword();

  String getBasicAuthUsername();

  String getBasicAuthPassword();

  ClientOptions copy();

  static ClientOptions create() {
    return new ClientOptionsImpl();
  }
}
```

We can always add additional configuration options later. We may also wish to expose the Vert.x `HttpClientOptions` for
advanced users that wish to provide custom configs.

### Transient queries -- Streaming

The `Client` interface will provide the following methods for streaming the results of a transient (push or pull) query:
```
  /**
   * Execute a query (push or pull) and receive the results one row at a time.
   *
   * @param sql statement of query to execute.
   * @return query result.
   */
  CompletableFuture<StreamedQueryResult> streamQuery(String sql);

  /**
   * Execute a query (push or pull) and receive the results one row at a time.
   *
   * @param sql statement of query to execute.
   * @param properties query properties.
   * @return query result.
   */
  CompletableFuture<StreamedQueryResult> streamQuery(String sql, Map<String, Object> properties);
```
where `StreamedQueryResult` is as follows:
```
import org.reactivestreams.Publisher;

/**
 * The result of a query (push or pull), streamed one row at time. Records may be consumed by either
 * subscribing to the publisher or polling (blocking) for one record at a time. These two methods of
 * consumption are mutually exclusive; only one method may be used (per StreamedQueryResult).
 */
public interface StreamedQueryResult extends Publisher<Row> {

  List<String> columnNames();

  List<ColumnType> columnTypes();

  String queryID();

  /**
   * Block until a row becomes available.
   *
   * @return the row.
   */
  Row poll();

  /**
   * Block until a row becomes available or the timeout has elapsed.
   *
   * @param timeout amount of to wait for a row. Non-positive values are interpreted as no timeout.
   * @param timeUnit unit for timeout param.
   * @return the row, if available; else, null.
   */
  Row poll(long timeout, TimeUnit timeUnit);

  boolean isComplete();

  void close();
}
```
Note that `StreamedQueryResult` is a Reactive Streams `Publisher` so users can stream results. Users can also call `poll()` to receive
results in a synchronous fashion instead. Only one of the two methods will be allowed per `StreamedQueryResult` instance.

The `Row` interface is as follows:
```
/**
 * A single record, returned as part of a query result.
 */
public interface Row {

  List<String> columnNames();

  List<ColumnType> columnTypes();

  KsqlArray values();

  /**
   * Get the value for a particular column of the Row as an Object.
   *
   * @param columnIndex index of column (1-indexed).
   * @return column value.
   */
  Object getObject(int columnIndex);

  /**
   * Get the value for a particular column of the Row as an Object.
   *
   * @param columnName name of column.
   * @return column value.
   */
  Object getObject(String columnName);

  /**
   * Get the value for a particular column of the Row as a string.
   *
   * @param columnIndex index of column (1-indexed).
   * @return column value.
   */
  String getString(int columnIndex);

  /**
   * Get the value for a particular column of the Row as a string.
   *
   * @param columnName name of column.
   * @return column value.
   */
  String getString(String columnName);

  /**
   * Get the value for a particular column of the Row as an integer.
   *
   * @param columnIndex index of column (1-indexed).
   * @return column value.
   */
  Integer getInt(int columnIndex);

  /**
   * Get the value for a particular column of the Row as an integer.
   *
   * @param columnName name of column.
   * @return column value.
   */
  Integer getInt(String columnName);

  /**
   * Get the value for a particular column of the Row as a long.
   *
   * @param columnIndex index of column (1-indexed).
   * @return column value.
   */
  Long getLong(int columnIndex);

  /**
   * Get the value for a particular column of the Row as a long.
   *
   * @param columnName name of column.
   * @return column value.
   */
  Long getLong(String columnName);

  /**
   * Get the value for a particular column of the Row as a double.
   *
   * @param columnIndex index of column (1-indexed).
   * @return column value.
   */
  Double getDouble(int columnIndex);

  /**
   * Get the value for a particular column of the Row as a double.
   *
   * @param columnName name of column.
   * @return column value.
   */
  Double getDouble(String columnName);

  /**
   * Get the value for a particular column of the Row as a boolean.
   *
   * @param columnIndex index of column (1-indexed).
   * @return column value.
   */
  Boolean getBoolean(int columnIndex);

  /**
   * Get the value for a particular column of the Row as a boolean.
   *
   * @param columnName name of column.
   * @return column value.
   */
  Boolean getBoolean(String columnName);

  /**
   * Get the value for a particular column of the Row as a decimal.
   *
   * @param columnIndex index of column (1-indexed).
   * @return column value.
   */
  BigDecimal getDecimal(int columnIndex);

  /**
   * Get the value for a particular column of the Row as a decimal.
   *
   * @param columnName name of column.
   * @return column value.
   */
  BigDecimal getDecimal(String columnName);
  
  /**
   * Get the value for a particular column of the Row as a KsqlObject.
   * Useful for MAP and STRUCT column types.
   *
   * @param columnIndex index of column (1-indexed).
   * @return column value.
   */
  KsqlObject getKsqlObject(int columnIndex);

  /**
   * Get the value for a particular column of the Row as a KsqlObject.
   * Useful for MAP and STRUCT column types.
   *
   * @param columnName name of column.
   * @return column value.
   */
  KsqlObject getKsqlObject(String columnName);

  /**
   * Get the value for a particular column of the Row as a KsqlArray. Useful for ARRAY column types.
   *
   * @param columnIndex index of column (1-indexed).
   * @return column value.
   */
  KsqlArray getKsqlArray(int columnIndex);

  /**
   * Get the value for a particular column of the Row as a KsqlArray. Useful for ARRAY column types.
   *
   * @param columnName name of column.
   * @return column value.
   */
  KsqlArray getKsqlArray(String columnName);
}
```

`KsqlArray` and `KsqlObject` are wrappers around the Vert.x types `JsonArray` and `JsonObject`, which can be thought of as `List<Object>` and `Map<String, Object>` with enhanced type safety and other features (e.g., serialization to/from JSON) we may choose to leverage in the future.
We chose to introduce new types rather than using   `List<Object>` and `Map<String, Object>` directly in order to allow more flexibility for these APIs to evolve in the future.
(I'm not a fan of the names `KsqlArray` and `KsqlObject` but couldn't think of anything better. Suggestions appreciated!)

The methods exposed on each type are as follows:
```
public KsqlArray {

  KsqlArray();

  KsqlArray(List list);

  boolean contains(Object value);

  int size();

  boolean isEmpty();

  List getList();

  Iterator<Object> iterator();

  java.util.stream.Stream<Object> stream();

  Object getValue(int pos);

  String getString(int pos);

  Integer getInteger(int pos);

  Long getLong(int pos);

  Double getDouble(int pos);

  Boolean getBoolean(int pos);

  KsqlArray getKsqlArray(int pos);

  KsqlObject getKsqlObject(int pos);

  Object remove(int pos);

  Object remove(Object value);

  KsqlArray add(String value);

  KsqlArray add(Integer value);

  KsqlArray add(Long value);

  KsqlArray add(Double value);

  KsqlArray add(Boolean value);

  KsqlArray add(KsqlArray value);

  KsqlArray add(KsqlObject value);

  KsqlArray add(Object value);

  KsqlArray addNull();

  KsqlArray addAll(KsqlArray array);

  KsqlArray copy();
}
```
and
```
public KsqlObject {

  KsqlObject();

  KsqlObject(Map<String, Object> map);

  boolean containsKey(String key);

  Set<String> fieldNames();

  int size();

  boolean isEmpty();

  Map<String, Object> getMap();

  Iterator<Map.Entry<String,Object>> iterator();

  java.util.stream.Stream<Map.Entry<String,Object>> stream();

  Object getValue(String key);

  String getString(String key);

  Integer getInteger(String key);

  Long getLong(String key);

  Double getDouble(String key);

  Boolean getBoolean(String key);

  KsqlArray getKsqlArray(String key);

  KsqlObject getKsqlObject(String key);

  Object remove(String key);

  KsqlObject put(String key, Integer value);

  KsqlObject put(String key, Long value);

  KsqlObject put(String key, String value);

  KsqlObject put(String key, Double value);

  KsqlObject put(String key, Boolean value);

  KsqlObject put(String key, KsqlArray value);

  KsqlObject put(String key, KsqlObject value);

  KsqlObject put(String key, Object value);

  KsqlObject putNull(String key);

  KsqlObject mergeIn(KsqlObject other);

  KsqlObject copy();
}
``` 
It may be nice to expose specific methods for handling `BigDecimal` on these types in order to have the decimal ksqlDB type feel like a first-class citizen, though this needs more investigation to flesh out since such methods would not be able to be delegated to Vert.x's `JsonArray` and `JsonObject` directly.

Finally, `ColumnType` is:
```
public interface ColumnType {

  enum Type { STRING, INTEGER, BIGINT, DOUBLE, BOOLEAN, DECIMAL, ARRAY, MAP, STRUCT }

  Type getType();

}
```

For now, `ColumnType` simply wraps a type represented as an enum. We can add additional methods in the future if we wish to expose more detailed type information (e.g., decimal scale and precision, or inner types for nested/complex types).
(The server side APIs currently return type information only as strings, which means providing a fully-specified type requires either client-side parsing or a server side change. The latter is preferred but is a fair bit of additional work.)

For `getDecimal(...)` in the `Row` interface, rather than trying to parse the column type to extract the precision and scale, we will simply convert the value to a BigDecimal without explicitly specifying the precision and scale. We could also add an option for users to specify the scale and precision in the getter, if we think that would be useful.

It's interesting that the method `getKsqlObject()` in the `Row` interface is used to represent both the `MAP` and `STRUCT` ksqlDB types. I'm not sure what a better alternative to avoid this confusion might be.

### Transient queries -- Non-streaming

The `Client` interface will also provide the following methods for receiving the results of a transient query (push or pull) in a single batch (non-streaming),
once the query has completed:
```
  /**
   * Execute a query (push or pull) and receive all result rows together, once the query has
   * completed.
   *
   * @param sql statement of query to execute.
   * @return query result.
   */
  CompletableFuture<BatchQueryResult> executeQuery(String sql);

  /**
   * Execute a query (push or pull) and receive all result rows together, once the query has
   * completed.
   *
   * @param sql statement of query to execute.
   * @param properties query properties.
   * @return query result.
   */
  CompletableFuture<BatchQueryResult> executeQuery(String sql, Map<String, Object> properties);
```
where
```
public interface BatchQueryResult {

  List<String> columnNames();

  List<ColumnType> columnTypes();

  String queryID();
  
  List<Rows> rows();
```

For a query to "complete" could mean:
* The query is a pull query
* The query is a push query with a limit clause, and the limit has been reached
* The query is a push query that has been terminated

We may want to introduce a limit to the number of rows that may be returned from these `executeQuery()` methods,
in order to decrease the likelihood of running out of memory.

For `BatchQueryResult`, the `columnNames()` and `columnTypes()` methods are not strictly necessary as this same information is contained in `Row`
and (unlike the `StreamedQueryResult`) the rows are returned at the same time as the query result object itself.
I think it's nice to have them for purposes of consistency but we remove them if others think the additional methods are redundant.

### Insert values

A method to insert one row at a time:
```
  /**
   * Insert a single row into the relevant stream/table.
   *
   * @param streamName name of stream/table.
   * @param row the row to insert.
   * @return a future that completes once the request has been processed.
   */
  CompletableFuture<Void> insertInto(String streamName, KsqlObject row);
```

A method to stream inserts (via the `/inserts-stream` endpoint):
```
  CompletableFuture<Publisher<InsertResponse>> streamInserts(String streamName, Publisher<KsqlObject> insertsPublisher);
}
```
where `InsertResponse` is as follows:
```
public interface InsertResponse {

  /**
   * Whether the row was successfully inserted or not.
   */
  boolean isSuccessful();

  /**
   * Unique sequence number for the row in the stream of inserts.
   */
  int getSequenceNum();

  /**
   * If unsuccessful, the error message.
   */
  String getErrorMessage();

  /**
   * If unsuccessful, the error code.
   */
  int getErrorCode();

}
```

### DDL operations

#### `CREATE <STREAM/TABLE>`, `CREATE <STREAM/TABLE> ... AS SELECT`, `INSERT INTO`

`Client` methods:
```
  /**
   * Execute DDL statement: `CREATE STREAM`, `CREATE TABLE`, `CREATE STREAM ... AS SELECT`, `CREATE TABLE ... AS SELECT`, `INSERT INTO`
   */
  CompletableFuture<ExecuteStatementResponse> executeStatement(String sql);

  /**
   * Execute DDL statement: `CREATE STREAM`, `CREATE TABLE`, `CREATE STREAM ... AS SELECT`, `CREATE TABLE ... AS SELECT`, `INSERT INTO`
   */
  CompletableFuture<ExecuteStatementResponse> executeStatement(String sql, Map<String, Object> properties);

  /**
   * Execute DDL statement: `CREATE STREAM`, `CREATE TABLE`, `CREATE STREAM ... AS SELECT`, `CREATE TABLE ... AS SELECT`, `INSERT INTO`
   */
  CompletableFuture<ExecuteStatementResponse> executeStatement(String sql, Map<String, Object> properties, long commandSequenceNumber);

```
with
```
public interface ExecuteStatementResponse {
    
  long getCommandSequenceNumber();

}
```
Command sequence number is exposed since that's the mechanism ksqlDB uses to ensure a server receiving a new request has
executed earlier statements that the new request depends on. In the future we could consider introducing a "request pipelining"
setting on the client, similar to the one used by the ksqlDB CLI, which automatically tracks and feeds the latest command
sequence number into subsequent commands (rather than having the user do it themselves). This would be slightly more complicated
than for the ksqlDB CLI, however, since the client could be used by multiple threads.

Interestingly, the new server APIs (used by `streamQuery()` and `executeQuery()` above) don't support the command sequence number option,
so we should either add that for consistency (and then add it to the client as well) or phase out support for command sequence number
on the old server APIs (including the `/ksql` endpoint used by the DDL and admin commands).
Do we think it makes sense to avoid exposing command sequence number in the client at all, for consistency?

An earlier version of this KLIP proposed having separate methods for `executeDdl()` and `executeDml()`
rather than having a single `executeStatement()` method as the name `executeStatement()` is potentially
misleadingly broad, but `DDL` and `DML` aren't used throughout our docs and there was some confusion
around which statements fell into which categories so having a single `executeStatement()` seems simplest.

Other alternatives considered include
```
  CompletableFuture<CreateSourceResponse> createStream(String sql);

  CompletableFuture<CreateSourceResponse> createStream(String sql, Map<String, Object> properties);
  
  CompletableFuture<CreateSourceResponse> createStream(String sql, Map<String, Object> properties, long commandSequenceNumber);

  CompletableFuture<CreateSourceResponse> createTable(String sql);

  CompletableFuture<CreateSourceResponse> createTable(String sql, Map<String, Object> properties);
  
  CompletableFuture<CreateSourceResponse> createTable(String sql, Map<String, Object> properties, long commandSequenceNumber);

  CompletableFuture<InsertIntoResponse> insertIntoSource(String sql);

  CompletableFuture<InsertIntoResponse> insertIntoSource(String sql, Map<String, Object> properties);
  
  CompletableFuture<InsertIntoResponse> insertIntoSource(String sql, Map<String, Object> properties, long commandSequenceNumber);
```

In this version, the implementations of `createStream(...)` and `createTable(...)` would be identical and could be replaced with a single `createSource(...)`
but the name of this method feels confusing. `CreateSourceResponse` and `InsertIntoResponse` are identical to `ExecuteStatementResponse` above.

Other alternatives include separating `CREATE <STREAM/TABLE>` and `CREATE <STREAM/TABLE> ... AS SELECT` into separate methods, but that feels unnecessarily complex.

#### `DROP <STREAM/TABLE>`

`Client` methods:
```
  /**
   * Drop stream. The underlying Kafka topic will not be deleted.
   */
  CompletableFuture<DropSourceResponse> dropStream(String streamName);

  /**
   * Drop stream. The underlying Kafka topic may optionally be deleted.
   */
  CompletableFuture<DropSourceResponse> dropStream(String streamName, boolean deleteTopic);

  /**
   * Drop table. The underlying Kafka topic will not be deleted.
   */
  CompletableFuture<DropSourceResponse> dropTable(String tableName);

  /**
   * Drop table. The underlying Kafka topic may optionally be deleted.
   */
  CompletableFuture<DropSourceResponse> dropTable(String tableName, boolean deleteTopic);
```
where `DropSourceResponse` is actually the same as `ExecuteStatementResponse`/`CreateSourceResponse`/`InsertIntoResponse` above.

Again, the implementations of `dropStream(...)` and `dropTable(...)` would be the same so we could instead have a single `dropSource(...)`, but the naming might be confusing.

If we choose to keep them separate, there's an open question of whether the client should validate that `dropStream(...)` is not used to drop a table, and vice versa.
IMO such validation would be introducing complexity without much benefit, though this point of ambiguity makes me prefer a single `dropSource(...)` if we can agree on a method name that's not confusing.

Note that users can also execute `DROP <STREAM/TABLE>` requests via `executeStatement()` above, so we could also get rid of these additional methods altogether.
Whether we choose to do so or not comes down to whether we see value in providing a method a user can call with a stream/table name directly, rather than passing in the full sql string for the command.

#### `TERMINATE <queryId>`

```
  CompletableFuture<TerminateQueryResponse> terminatePersistentQuery(String queryId);
```
where `TerminateQueryResponse` is again the same as `ExecuteStatementResponse`/`CreateSourceResponse`/`InsertIntoResponse`/`DropSourceResponse` above.

The method name `terminatePersistenQuery(...)` is to distinguish from `terminatePushQuery(...)` below.

As above, users could also terminate persistent queries via `executeStatement()`, so we could remove `terminatePersistentQuery()` if we don't see value in providing a convenience method
that only requires the query ID, rather than the full sql string for the command.

#### Connectors

```
  CompletableFuture<ConnectorInfo> createSourceConnector(String name, Map<String, String> properties);

  CompletableFuture<ConnectorInfo> createSinkConnector(String name, Map<String, String> properties);

  CompletableFuture<Void> dropConnector(String name);
```
where `ConnectorInfo` is from an Apache Kafka module ([link](https://github.com/apache/kafka/blob/trunk/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/ConnectorInfo.java)).

Or would we rather not have a dependency on Apache Kafka in the ksqlDB client interfaces?

### Admin operations

#### `SHOW TOPICS`

```
  CompletableFuture<List<TopicInfo>> listTopics();
```
with
```
public interface TopicInfo {

  String getName();

  int getPartitions();
  
  List<Integer> getReplicasPerPartition();

}
```

#### `SHOW <STREAMS/TABLES>`

```
  CompletableFuture<List<StreamInfo>> listStreams();

  CompletableFuture<List<TableInfo>> listTables();
```
with
```
public interface StreamInfo {

  String getName();

  String getTopic();

  String getFormat();
  
}
```
and
```
public interface TableInfo {

  String getName();

  String getTopic();

  String getFormat();

  boolean isWindowed();
  
}
```

I'm not sure whether it makes more sense for `StreamInfo#getFormat()` and `TableInfo#getFormat()` to return a string or an enum value.
The latter would make it easier for the user to know the possible values, but we'd have to keep the list up to date and would also sacrifice forward compatibility.

#### `SHOW QUERIES`

```
  CompletableFuture<List<QueryInfo>> listQueries();
```
with
```
public interface QueryInfo {

  boolean isPersistentQuery();

  boolean isPushQuery();

  /**
   * Query ID, used for control operations such as terminating the query
   */
  String getId();

  String getSql();

  /**
   * Name of sink, for a persistent query. Else, empty.
   */
  Optional<String> getSink();

  /**
   * Name of sink topic, for a persistent query. Else, empty.
   */
  Optional<String> getSinkTopic();

}
```

#### `SHOW CONNECTORS`

```
  CompletableFuture<ConnectorList> listConnectors();
```
with
```
public interface ConnectorList {
  
  List<ConnectorInfo> getConnectors();
  
  /**
   * Any warnings returned by the server as a result of listing connectors.
   */
  List<String> getWarnings();

}
```
and
```
public interface ConnectorInfo {

  enum ConnectorType {
    SOURCE,
    SINK,
    UNKNOWN;
  }

  String getName();

  ConnectorType getType();

  String getClassName();

  String getState();

}
```

I don't love that the introduction of `ConnectorList` wrapped around `List<ConnectorInfo>` breaks the pattern established by `SHOW TOPICS`/`SHOW <STREAMS/TABLES>`/`SHOW QUERIES` but it seems important to propagate any server warnings to the user so the trade-off is worth it IMO.

### Terminate push query

```
  CompletableFuture<Void> terminatePushQuery(String queryId);
```

### Miscellaneous

The `Client` interface will also have the following:
```
  void close();
```

## Design

Implementation of the client is out of scope. This KLIP is only about the interfaces.

## Test plan

N/A. This KLIP is only about the interfaces.

## LOEs and Delivery Milestones

The order in which the client methods will be implemented is as follows:
* Push and pull queries
* Insert values
* DDL statements
* Terminate push queries
* Admin operations

Push and pull queries are targeted for ksqlDB 0.10.0 for sure. Insert values and some DDL statements may make the cut as well, but it's uncertain at the moment. Everything that slips through will be in ksqlDB 0.11.0 instead. 

A more detailed breakdown of implementation phases is out of scope for this KLIP.

## Documentation Updates

We'll add Java docs for the Client interfaces for sure. I think it'd also be good to add a new docs page with example usage of the client, though I'm not sure what sort of example makes the most sense.

## Compatibility Implications

N/A

## Security Implications

N/A
