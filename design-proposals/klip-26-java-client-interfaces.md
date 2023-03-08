# KLIP 26 - Java client interfaces

**Author**: Victoria Xia (@vcrfxia) | 
**Release Target**: 0.10.0; 6.0.0 | 
**Status**: _Merged_ | 
**Discussion**: [GitHub PR](https://github.com/confluentinc/ksql/pull/5236)

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
* Select admin operations:
    * `SHOW TOPICS` (non-extended)
    * `SHOW STREAMS` (non-extended)
    * `SHOW TABLES` (non-extended)
    * `SHOW QUERIES` (non-extended)
* Insert values, i.e., insert rows into an existing stream
* Terminate push query (via the `/close-query` endpoint)

The purpose of this KLIP is to reach agreement on the interfaces / public APIs.
Implementation details will not be covered.

## What is not in scope

This KLIP does not cover Java client support for the following:
* `DESCRIBE <stream/table>`, `DESCRIBE FUNCTION`
* `EXPLAIN <queryId>`
* `PRINT <topic>`
* `SHOW TOPICS EXTENDED`, `SHOW <STREAMS/TABLES> EXTENDED`, `SHOW QUERIES EXTENDED`
* `CREATE CONNECTOR`, `DROP CONNECTOR`, `SHOW CONNECTORS`, `DESCRIBE CONNECTOR`
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
  
  ClientOptions setVerifyHost(boolean verifyHost);
  
  ClientOptions setUseAlpn(boolean useAlpn);

  ClientOptions setTrustStore(String trustStorePath);

  ClientOptions setTrustStorePassword(String trustStorePassword);

  ClientOptions setKeyStore(String keyStorePath);

  ClientOptions setKeyStorePassword(String keyStorePassword);

  ClientOptions setBasicAuthCredentials(String username, String password);
  
  ClientOptions setExecuteQueryMaxResultRows(int maxRows);

  String getHost();

  int getPort();

  boolean isUseTls();
  
  boolean isVerifyHost();
  
  boolean isUseAlpn();

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

  /**
   * Executes a query (push or pull) and returns the results one row at a time.
   *
   * <p>If a non-200 response is received from the server, the {@code CompletableFuture} will be
   * failed.
   *
   * <p>By default, push queries issued via this method return results starting from the beginning
   * of the stream or table. To override this behavior, pass in the query property
   * {@code auto.offset.reset} with value set to {@code latest}.
   *
   * @param sql statement of query to execute
   * @param properties query properties
   * @return a future that completes once the server response is received, and contains the query
   *         result if successful
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
 *
 * <p>The {@code subscribe()} method cannot be called if {@link #isFailed} is true.
 */
public interface StreamedQueryResult extends Publisher<Row> {

  List<String> columnNames();

  List<ColumnType> columnTypes();

  /**
   * Returns the ID of the underlying query if the query is a push query. Else, returns null.
   *
   * @return the query ID
   */
  String queryID();

  /**
   * Returns the next row. Blocks until one is available or the underlying query is terminated
   * (either gracefully or because of an error).
   *
   * @return the row, or null if the query was terminated.
   */
  Row poll();

  /**
   * Returns the next row. Blocks until one is available, the specified timeout has elapsed, or the
   * underlying query is terminated (either gracefully or because of an error).
   *
   * @param timeout amount of time to wait for a row. A non-positive value will cause this method to
   *        block until a row is received or the query is terminated.
   * @return the row, or null if the timeout elapsed or the query was terminated.
   */
  Row poll(Duration timeout);

  /**
   * Returns whether the {@code StreamedQueryResult} is complete.
   *
   * <p>A {@code StreamedQueryResult} is complete if the HTTP connection associated with this query
   * has been ended gracefully. Once complete, the @{code StreamedQueryResult} will continue to
   * deliver any remaining rows, then call {@code onComplete()} on the subscriber, if present.
   *
   * @return whether the {@code StreamedQueryResult} is complete.
   */
  boolean isComplete();

  /**
   * Returns whether the {@code StreamedQueryResult} is failed.
   *
   * <p>A {@code StreamedQueryResult} is failed if an error is received from the server. Once
   * failed, {@code onError()} is called on the subscriber, if present, any existing {@code poll()}
   * calls will return null, and new calls to {@link #poll} and {@code subscribe()} will be
   * rejected.
   *
   * @return whether the {@code StreamedQueryResult} is failed.
   */
  boolean isFailed();
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

  /**
   * Returns the values (data) in this row, represented as a {@link KsqlArray}.
   *
   * <p>Returned values are JSON types which means numeric columns are not necessarily typed in
   * accordance with {@link #columnTypes}. For example, a {@code BIGINT} field will be typed as an
   * integer rather than a long, if the numeric value fits into an integer.
   *
   * @return the values
   */
  KsqlArray values();

  /**
   * Returns the data in this row represented as a {@link KsqlObject} where keys are column names
   * and values are column values.
   *
   * @return the data
   */
  KsqlObject asObject();
  
  /**
   * Returns whether the value for a particular column of the {@code Row} is null.
   *
   * @param columnIndex index of column (1-indexed)
   * @return whether the column value is null
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  boolean isNull(int columnIndex);

  /**
   * Returns whether the value for a particular column of the {@code Row} is null.
   *
   * @param columnName name of column
   * @return whether the column value is null
   * @throws IllegalArgumentException if the column name is invalid
   */
  boolean isNull(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as an {@code Object}.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  Object getValue(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as an {@code Object}.
   *
   * @param columnName name of column
   * @return column value
   * @throws IllegalArgumentException if the column name is invalid
   */
  Object getValue(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as a string.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws ClassCastException if the column value is not a string
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  String getString(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as a string.
   *
   * @param columnName name of column
   * @return column value
   * @throws ClassCastException if the column value is not a string
   * @throws IllegalArgumentException if the column name is invalid
   */
  String getString(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as an integer.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws ClassCastException if the column value is not a {@code Number}
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  Integer getInteger(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as an integer.
   *
   * @param columnName name of column
   * @return column value
   * @throws ClassCastException if the column value is not a {@code Number}
   * @throws IllegalArgumentException if the column name is invalid
   */
  Integer getInteger(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as a long.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws ClassCastException if the column value is not a {@code Number}
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  Long getLong(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as a long.
   *
   * @param columnName name of column
   * @return column value
   * @throws ClassCastException if the column value is not a {@code Number}
   * @throws IllegalArgumentException if the column name is invalid
   */
  Long getLong(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as a double.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws ClassCastException if the column value is not a {@code Number}
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  Double getDouble(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as a double.
   *
   * @param columnName name of column
   * @return column value
   * @throws ClassCastException if the column value is not a {@code Number}
   * @throws IllegalArgumentException if the column name is invalid
   */
  Double getDouble(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as a boolean.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws ClassCastException if the column value is not a boolean
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  Boolean getBoolean(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as a boolean.
   *
   * @param columnName name of column
   * @return column value
   * @throws ClassCastException if the column value is not a boolean
   * @throws IllegalArgumentException if the column name is invalid
   */
  Boolean getBoolean(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as a {@code BigDecimal}.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws ClassCastException if the column value is not a {@code Number}
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  BigDecimal getDecimal(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as a {@code BigDecimal}.
   *
   * @param columnName name of column
   * @return column value
   * @throws ClassCastException if the column value is not a {@code Number}
   * @throws IllegalArgumentException if the column name is invalid
   */
  BigDecimal getDecimal(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as a {@link KsqlObject}.
   * Useful for {@code MAP} and {@code STRUCT} column types.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws ClassCastException if the column value cannot be converted to a map
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  KsqlObject getKsqlObject(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as a {@link KsqlObject}.
   * Useful for {@code MAP} and {@code STRUCT} column types.
   *
   * @param columnName name of column
   * @return column value
   * @throws ClassCastException if the column value cannot be converted to a map
   * @throws IllegalArgumentException if the column name is invalid
   */
  KsqlObject getKsqlObject(String columnName);

  /**
   * Returns the value for a particular column of the {@code Row} as a {@link KsqlArray}.
   * Useful for {@code ARRAY} column types.
   *
   * @param columnIndex index of column (1-indexed)
   * @return column value
   * @throws ClassCastException if the column value cannot be converted to a list
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  KsqlArray getKsqlArray(int columnIndex);

  /**
   * Returns the value for a particular column of the {@code Row} as a {@link KsqlArray}.
   * Useful for {@code ARRAY} column types.
   *
   * @param columnName name of column
   * @return column value
   * @throws ClassCastException if the column value cannot be converted to a list
   * @throws IllegalArgumentException if the column name is invalid
   */
  KsqlArray getKsqlArray(String columnName);
}
```

`KsqlArray` and `KsqlObject` are wrappers around the Vert.x types `JsonArray` and `JsonObject`, which can be thought of as `List<Object>` and `Map<String, Object>` with enhanced type safety and other features (e.g., serialization to/from JSON) we may choose to leverage in the future.
We chose to introduce new types rather than using   `List<Object>` and `Map<String, Object>` directly in order to allow more flexibility for these APIs to evolve in the future.
(I'm not a fan of the names `KsqlArray` and `KsqlObject` but couldn't think of anything better. Suggestions appreciated!)

The methods exposed on each type are as follows:
```
/**
 * A representation of an array of values.
 */
public KsqlArray {

  /**
   * Creates an empty instance.
   */
  public KsqlArray() {
    delegate = new JsonArray();
  }

  /**
   * Creates an instance with the specified values.
   *
   * @param list the values
   */
  public KsqlArray(final List<?> list) {
    delegate = new JsonArray(list);
  }

  /**
   * Returns the size (number of values) of the array.
   *
   * @return the size
   */
  public int size() {
    return delegate.size();
  }

  /**
   * Returns whether the array is empty.
   *
   * @return whether the array is empty
   */
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  /**
   * Returns values of the array as a list.
   *
   * @return list of values
   */
  public List<?> getList() {
    return delegate.getList();
  }

  /**
   * Returns an iterator over values of the array.
   *
   * @return the iterator
   */
  public Iterator<Object> iterator() {
    return delegate.iterator();
  }

  /**
   * Returns values of the array as a stream.
   *
   * @return the stream
   */
  public java.util.stream.Stream<Object> stream() {
    return delegate.stream();
  }

  /**
   * Returns the value at a specified index as an {@code Object}.
   *
   * @param pos the index
   * @return the value
   */
  public Object getValue(final int pos) {
    return delegate.getValue(pos);
  }

  /**
   * Returns the value at a specified index as a string.
   *
   * @param pos the index
   * @return the value
   * @throws ClassCastException if the value is not a string
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public String getString(final int pos) {
    return delegate.getString(pos);
  }

  /**
   * Returns the value at a specified index as an integer.
   *
   * @param pos the index
   * @return the value
   * @throws ClassCastException if the value is not a {@code Number}
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public Integer getInteger(final int pos) {
    return delegate.getInteger(pos);
  }

  /**
   * Returns the value at a specified index as a long.
   *
   * @param pos the index
   * @return the value
   * @throws ClassCastException if the value is not a {@code Number}
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public Long getLong(final int pos) {
    return delegate.getLong(pos);
  }

  /**
   * Returns the value at a specified index as a double.
   *
   * @param pos the index
   * @return the value
   * @throws ClassCastException if the value is not a {@code Number}
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public Double getDouble(final int pos) {
    return delegate.getDouble(pos);
  }

  /**
   * Returns the value at a specified index as a boolean.
   *
   * @param pos the index
   * @return the value
   * @throws ClassCastException if the value is not a boolean
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public Boolean getBoolean(final int pos) {
    return delegate.getBoolean(pos);
  }

  /**
   * Returns the value at a specified index as a {@code BigDecimal}.
   *
   * @param pos the index
   * @return the value
   * @throws ClassCastException if the value is not a {@code Number}
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public BigDecimal getDecimal(final int pos) {
    return new BigDecimal(getValue(pos).toString());
  }

  /**
   * Returns the value at a specified index as a {@code KsqlArray}.
   *
   * @param pos the index
   * @return the value
   * @throws ClassCastException if the value cannot be converted to a list
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public KsqlArray getKsqlArray(final int pos) {
    return new KsqlArray(delegate.getJsonArray(pos));
  }

  /**
   * Returns the value at a specified index as a {@link KsqlObject}.
   *
   * @param pos the index
   * @return the value
   * @throws ClassCastException if the value cannot be converted to a map
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public KsqlObject getKsqlObject(final int pos) {
    return new KsqlObject(delegate.getJsonObject(pos));
  }

  /**
   * Removes the value at a specified index from the array.
   *
   * @param pos the index
   * @return the removed value
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public Object remove(final int pos) {
    return delegate.remove(pos);
  }

  /**
   * Removes the first occurrence of the specified value from the array, if present.
   *
   * @param value the value to remove
   * @return whether the value was removed
   */
  public boolean remove(final Object value) {
    return delegate.remove(value);
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final String value) {
    delegate.add(value);
    return this;
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final Integer value) {
    delegate.add(value);
    return this;
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final Long value) {
    delegate.add(value);
    return this;
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final Double value) {
    delegate.add(value);
    return this;
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final Boolean value) {
    delegate.add(value);
    return this;
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final BigDecimal value) {
    // Vert.x JsonArray does not accept BigDecimal values. Instead we store the value as a string
    // so as to not lose precision.
    delegate.add(value.toString());
    return this;
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final KsqlArray value) {
    delegate.add(KsqlArray.toJsonArray(value));
    return this;
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final KsqlObject value) {
    delegate.add(KsqlObject.toJsonObject(value));
    return this;
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final Object value) {
    delegate.add(value);
    return this;
  }

  /**
   * Appends a null value to the end of the array.
   *
   * @return a reference to this
   */
  public KsqlArray addNull() {
    delegate.addNull();
    return this;
  }

  /**
   * Appends the values in the specified {@code KsqlArray} to the end of this instance.
   *
   * @param array the values to append
   * @return a reference to this
   */
  public KsqlArray addAll(final KsqlArray array) {
    delegate.addAll(toJsonArray(array));
    return this;
  }

  /**
   * Returns a copy of this.
   *
   * @return the copy
   */
  public KsqlArray copy() {
    return new KsqlArray(delegate.copy());
  }

  /**
   * Returns a JSON string representing the values in the array.
   *
   * @return the JSON string
   */
  public String toJsonString() {
    return delegate.toString();
  }

  /**
   * Returns a JSON string representing the values in the array. Same as {@link #toJsonString()}.
   *
   * @return the JSON string
   */
  @Override
  public String toString() {
    return toJsonString();
  }
}
```
and
```
/**
 * A representation of a map of string keys to values. Useful for representing a JSON object.
 */
public KsqlObject {

  /**
   * Creates an empty instance.
   */
  public KsqlObject() {
    delegate = new JsonObject();
  }

  /**
   * Creates an instance with the specified entries.
   *
   * @param map the entries
   */
  public KsqlObject(final Map<String, Object> map) {
    delegate = new JsonObject(map);
  }

  KsqlObject(final JsonObject jsonObject) {
    delegate = Objects.requireNonNull(jsonObject);
  }

  /**
   * Returns whether the map contains the specified key.
   *
   * @param key the key
   * @return whether the map contains the key
   */
  public boolean containsKey(final String key) {
    return delegate.containsKey(key);
  }

  /**
   * Returns the keys of the map.
   *
   * @return the keys
   */
  public Set<String> fieldNames() {
    return delegate.fieldNames();
  }

  /**
   * Returns the size (number of entries) of the map.
   *
   * @return the size
   */
  public int size() {
    return delegate.size();
  }

  /**
   * Returns whether the map is empty.
   *
   * @return whether the map is empty
   */
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  /**
   * Returns the entries of the map as a {@code Map}.
   *
   * @return the entries
   */
  public Map<String, Object> getMap() {
    return delegate.getMap();
  }

  /**
   * Returns an iterator over the entries of the map.
   *
   * @return the iterator
   */
  public Iterator<Entry<String,Object>> iterator() {
    return delegate.iterator();
  }

  /**
   * Returns entries of the map as a stream.
   *
   * @return the stream
   */
  public java.util.stream.Stream<Map.Entry<String,Object>> stream() {
    return delegate.stream();
  }

  /**
   * Returns the value associated with the specified key as an {@code Object}. Returns null if the
   * key is not present.
   *
   * @param key the key
   * @return the value
   */
  public Object getValue(final String key) {
    return delegate.getValue(key);
  }

  /**
   * Returns the value associated with the specified key as a string. Returns null if the key is not
   * present.
   *
   * @param key the key
   * @return the value
   * @throws ClassCastException if the value is not a string
   */
  public String getString(final String key) {
    return delegate.getString(key);
  }

  /**
   * Returns the value associated with the specified key as an integer. Returns null if the key is
   * not present.
   *
   * @param key the key
   * @return the value
   * @throws ClassCastException if the value is not a {@code Number}
   */
  public Integer getInteger(final String key) {
    return delegate.getInteger(key);
  }

  /**
   * Returns the value associated with the specified key as a long. Returns null if the key is not
   * present.
   *
   * @param key the key
   * @return the value
   * @throws ClassCastException if the value is not a {@code Number}
   */
  public Long getLong(final String key) {
    return delegate.getLong(key);
  }

  /**
   * Returns the value associated with the specified key as a double. Returns null if the key is
   * not present.
   *
   * @param key the key
   * @return the value
   * @throws ClassCastException if the value is not a {@code Number}
   */
  public Double getDouble(final String key) {
    return delegate.getDouble(key);
  }

  /**
   * Returns the value associated with the specified key as a boolean. Returns null if the key is
   * not present.
   *
   * @param key the key
   * @return the value
   * @throws ClassCastException if the value is not a boolean
   */
  public Boolean getBoolean(final String key) {
    return delegate.getBoolean(key);
  }

  /**
   * Returns the value associated with the specified key as a {@code BigDecimal}. Returns null if
   * the key is not present.
   *
   * @param key the key
   * @return the value
   * @throws ClassCastException if the value is not a {@code Number}
   */
  public BigDecimal getDecimal(final String key) {
    return new BigDecimal(getValue(key).toString());
  }

  /**
   * Returns the value associated with the specified key as a {@link KsqlArray}. Returns null if the
   * key is not present.
   *
   * @param key the key
   * @return the value
   * @throws ClassCastException if the value cannot be converted to a list
   */
  public KsqlArray getKsqlArray(final String key) {
    return new KsqlArray(delegate.getJsonArray(key));
  }

  /**
   * Returns the value associated with the specified key as a {@code KsqlObject}. Returns null if
   * the key is not present.
   *
   * @param key the key
   * @return the value
   * @throws ClassCastException if the value cannot be converted to a map
   */
  public KsqlObject getKsqlObject(final String key) {
    return new KsqlObject(delegate.getJsonObject(key));
  }

  /**
   * Removes the value associated with a specified key.
   *
   * @param key the key
   * @return the removed value, or null if the key was not present
   */
  public Object remove(final String key) {
    return delegate.remove(key);
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final Integer value) {
    delegate.put(key, value);
    return this;
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final Long value) {
    delegate.put(key, value);
    return this;
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final String value) {
    delegate.put(key, value);
    return this;
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final Double value) {
    delegate.put(key, value);
    return this;
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final Boolean value) {
    delegate.put(key, value);
    return this;
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final BigDecimal value) {
    // Vert.x JsonObject does not accept BigDecimal values. Instead we store the value as a string
    // so as to not lose precision.
    delegate.put(key, value.toString());
    return this;
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final KsqlArray value) {
    delegate.put(key, KsqlArray.toJsonArray(value));
    return this;
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final KsqlObject value) {
    delegate.put(key, KsqlObject.toJsonObject(value));
    return this;
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final Object value) {
    delegate.put(key, value);
    return this;
  }

  /**
   * Adds an entry for the specified key with null value to the map.
   *
   * @param key the key
   * @return a reference to this
   */
  public KsqlObject putNull(final String key) {
    delegate.putNull(key);
    return this;
  }

  /**
   * Adds entries from the specified {@code KsqlObject} into this instance.
   *
   * @param other the entries to add
   * @return a reference to this
   */
  public KsqlObject mergeIn(final KsqlObject other) {
    delegate.mergeIn(toJsonObject(other));
    return this;
  }

  /**
   * Returns a copy of this.
   *
   * @return the copy
   */
  public KsqlObject copy() {
    return new KsqlObject(delegate.copy());
  }

  /**
   * Returns a JSON string representing the entries in the map.
   *
   * @return the JSON string
   */
  public String toJsonString() {
    return delegate.toString();
  }

  /**
   * Returns a JSON string representing the entries in the map. Same as {@link #toJsonString()}.
   *
   * @return the JSON string
   */
  @Override
  public String toString() {
    return toJsonString();
  }
}
``` 

Finally, `ColumnType` is:
```
/**
 * The type of a column returned as part of a query result.
 */
public interface ColumnType {

  enum Type { STRING, INTEGER, BIGINT, DOUBLE, BOOLEAN, DECIMAL, ARRAY, MAP, STRUCT }

  Type getType();

}
```

For now, `ColumnType` simply wraps a type represented as an enum. We can add additional methods in the future if we wish to expose more detailed type information (e.g., decimal scale and precision, or inner types for nested/complex types).
(The server side APIs currently return type information only as strings, which means providing a fully-specified type requires either client-side parsing or a server side change. The latter is preferred but is a fair bit of additional work.)

As pointed out in the javadocs, push queries issued via this method default to using `auto.offset.reset=earliestt`
(as a consequence of using the new `/query-stream` endpoint), which is a departure from the old REST API and the ksqlDB CLI.

For `getDecimal(...)` in the `Row` interface, rather than trying to parse the column type to extract the precision and scale, we will simply convert the value to a BigDecimal without explicitly specifying the precision and scale. We could also add an option for users to specify the scale and precision in the getter, if we think that would be useful.

It's interesting that the method `getKsqlObject()` in the `Row` interface is used to represent both the `MAP` and `STRUCT` ksqlDB types. I'm not sure what a better alternative to avoid this confusion might be.

### Transient queries -- Non-streaming

The `Client` interface will also provide the following methods for receiving the results of a transient query (push or pull) in a single batch (non-streaming),
once the query has completed:
```
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

  /**
   * Executes a query (push or pull) and returns all result rows in a single batch, once the query
   * has completed.
   *
   * <p>By default, push queries issued via this method return results starting from the beginning
   * of the stream or table. To override this behavior, pass in the query property
   * {@code auto.offset.reset} with value set to {@code latest}.
   *
   * @param sql statement of query to execute
   * @param properties query properties
   * @return query result
   */
  BatchedQueryResult executeQuery(String sql, Map<String, Object> properties);
```
where
```
/**
 * The result of a query (push or pull), returned as a single batch once the query has finished
 * executing, or the query has been terminated. For non-terminating push queries,
 * {@link StreamedQueryResult} should be used instead.
 *
 * <p>If a non-200 response is received from the server, this future will complete exceptionally.
 *
 * <p>The maximum number of {@code Row}s that may be returned from a {@code BatchedQueryResult}
 * defaults to {@link ClientOptions#DEFAULT_EXECUTE_QUERY_MAX_RESULT_ROWS} and can be configured
 * via {@link ClientOptions#setExecuteQueryMaxResultRows(int)}.
 */
public abstract class BatchedQueryResult extends CompletableFuture<List<Row>> {

  /**
   * Returns a {@code CompletableFuture} containing the ID of the underlying query if the query is
   * a push query, else null. The future is completed once a response is received from the server.
   *
   * @return a future containing the query ID (or null in the case of pull queries)
   */
  public abstract CompletableFuture<String> queryID();

}
```

For a query to "complete" could mean:
* The query is a pull query
* The query is a push query with a limit clause, and the limit has been reached
* The query is a push query that has been terminated

Similar to the `streamQuery()` methods above, push queries issued via this method default to using `auto.offset.reset=earliestt`
(as a consequence of using the new `/query-stream` endpoint), which is a departure from the old REST API and the ksqlDB CLI.

### Insert values

A method to insert one row at a time:
```
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

Note that these methods only allow the insertion of rows into ksqlDB streams, not tables.

We consciously choose not to expose a method to insert a batch of rows in a non-streaming fashion
as the lack of transactional guarantees means some rows may be inserted whereas others are not (e.g., if an error is encountered partway). 

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

In considering whether it makes more sense for `StreamInfo#getFormat()` and `TableInfo#getFormat()` to return a string or an enum value,
the latter would make it easier for the user to know the possible values, but we'd have to keep the list up to date and would also sacrifice forward compatibility.
Almog also points out that returning the format as a string allows support of custom formats in the future, which is a huge plus, so we'll go with strings for now.

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

### Terminate push query

```
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
```

### Miscellaneous

The `Client` interface will also have the following:
```
  /**
   * Closes the underlying HTTP client.
   */
  void close();
```

## Design

Implementation of the client is out of scope. This KLIP is only about the interfaces.

## Test plan

N/A. This KLIP is only about the interfaces.

## LOEs and Delivery Milestones

The following client methods will be available in the 0.10.0 release:
* Push and pull queries
* Terminate push queries
* Insert values -- non-streaming (i.e., `insertInto()` only)

The following methods are targeted for 0.11.0:
* Insert values -- streaming (i.e., `streamInserts()`)
* DDL statements
* Admin operations

## Documentation Updates

HTML API docs generated from the javadocs on the client interfaces will be hosted on the ksqlDB microsite.
We'll also have a separate docs page with example usage for each of the methods. (See https://github.com/confluentinc/ksql/pull/5434 for a work-in-progress draft.)

## Compatibility Implications

N/A

## Security Implications

N/A

## Rejected Alternatives

### Connectors (out of scope)

An earlier version of this KLIP proposed supporting `CREATE CONNECTOR`, `DROP CONNECTOR`, and `SHOW CONNECTORS` as part of the first version of the Java client covered in this KLIP,
but we decided to hold off on connector support in this first version of the client since a more preferable long-term solution would be for Connect to introduce a Java client,
rather than having users send commands via the ksqlDB client to send to ksqlDB to forward to Connect. In the meantime, users can use the REST API to manage connectors through ksqlDB if desired.

Below I've recorded the proposed interfaces for managing Connectors, for reference if we'd like to revisit in the future.

#### `CREATE CONNECTOR`, `DROP CONNECTOR`

```
  CompletableFuture<ConnectorInfo> createSourceConnector(String name, Map<String, String> properties);

  CompletableFuture<ConnectorInfo> createSinkConnector(String name, Map<String, String> properties);

  CompletableFuture<Void> dropConnector(String name);
```
where `ConnectorInfo` is from an Apache Kafka module ([link](https://github.com/apache/kafka/blob/trunk/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/entities/ConnectorInfo.java)).
We might like to wrap this type to avoid introducing a dependency on Apache Kafka in the ksqlDB client.

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

It's not great that the introduction of `ConnectorList` wrapped around `List<ConnectorInfo>` breaks the pattern established by `SHOW TOPICS`/`SHOW <STREAMS/TABLES>`/`SHOW QUERIES` but it seems important to propagate any server warnings to the user so the trade-off is worth it IMO.
