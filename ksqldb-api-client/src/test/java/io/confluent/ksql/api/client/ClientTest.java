/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.api.client;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import io.confluent.ksql.api.BaseApiTest;
import io.confluent.ksql.api.client.impl.ClientImpl;
import io.confluent.ksql.api.client.impl.ClientOptionsImpl;
import io.confluent.ksql.api.server.PushQueryId;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.vertx.core.Context;
import io.vertx.ext.web.client.WebClient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class ClientTest extends BaseApiTest {

  @SuppressWarnings("unchecked")
  protected static final List<String> DEFAULT_COLUMN_NAMES = BaseApiTest.DEFAULT_COLUMN_NAMES.getList();
  @SuppressWarnings("unchecked")
  protected static final List<String> DEFAULT_COLUMN_TYPES = BaseApiTest.DEFAULT_COLUMN_TYPES.getList();
  protected static final Map<String, Object> DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES =
      BaseApiTest.DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES.getMap();
  protected static final String DEFAULT_PUSH_QUERY_WITH_LIMIT = "select * from foo emit changes limit 10;";

  protected Context context;
  protected Client client;

  @Override
  public void setUp() {
    super.setUp();

    context = vertx.getOrCreateContext();
  }

  @Override
  protected WebClient createClient() {
    // Use Java client for these tests, rather than a vanilla WebClient
    this.client = createJavaClient();

    return null;
  }

  @Override
  protected void stopClient() {
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        log.error("Failed to close client", e);
      }
    }
  }

  @Test
  public void shouldStreamPushQueryAsync() throws Exception {
    // When
    final QueryResult queryResult =
        client.streamQuery(DEFAULT_PUSH_QUERY, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get();

    // Then
    assertThat(queryResult.columnNames(), is(DEFAULT_COLUMN_NAMES));
    assertThat(queryResult.columnTypes(), is(DEFAULT_COLUMN_TYPES));

    shouldDeliver(queryResult, DEFAULT_ROWS.size());

    String queryId = queryResult.queryID();
    assertThat(queryId, is(notNullValue()));
    verifyPushQueryServerState(DEFAULT_PUSH_QUERY, queryId);
  }

  @Test
  public void shouldStreamPushQuerySync() throws Exception {
    // When
    final QueryResult queryResult =
        client.streamQuery(DEFAULT_PUSH_QUERY, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get();

    // Then
    assertThat(queryResult.columnNames(), is(DEFAULT_COLUMN_NAMES));
    assertThat(queryResult.columnTypes(), is(DEFAULT_COLUMN_TYPES));

    for (int i = 0; i < DEFAULT_ROWS.size(); i++) {
      final Row row = queryResult.poll();
      assertThat(row.values(), equalTo(rowWithIndex(i).getList()));
      assertThat(row.columnNames(), equalTo(DEFAULT_COLUMN_NAMES));
      assertThat(row.columnTypes(), equalTo(DEFAULT_COLUMN_TYPES));
    }

    String queryId = queryResult.queryID();
    assertThat(queryId, is(notNullValue()));
    verifyPushQueryServerState(DEFAULT_PUSH_QUERY, queryId);
  }

  @Test
  public void shouldStreamPullQueryAsync() throws Exception {
    // When
    final QueryResult queryResult =
        client.streamQuery(DEFAULT_PULL_QUERY).get();

    // Then
    assertThat(queryResult.columnNames(), is(DEFAULT_COLUMN_NAMES));
    assertThat(queryResult.columnTypes(), is(DEFAULT_COLUMN_TYPES));

    shouldDeliver(queryResult, DEFAULT_ROWS.size());

    verifyPullQueryServerState();
  }

  @Test
  public void shouldStreamPullQuerySync() throws Exception {
    // When
    final QueryResult queryResult =
        client.streamQuery(DEFAULT_PULL_QUERY).get();

    // Then
    assertThat(queryResult.columnNames(), is(DEFAULT_COLUMN_NAMES));
    assertThat(queryResult.columnTypes(), is(DEFAULT_COLUMN_TYPES));

    for (int i = 0; i < DEFAULT_ROWS.size(); i++) {
      final Row row = queryResult.poll();
      assertThat(row.values(), equalTo(rowWithIndex(i).getList()));
      assertThat(row.columnNames(), equalTo(DEFAULT_COLUMN_NAMES));
      assertThat(row.columnTypes(), equalTo(DEFAULT_COLUMN_TYPES));
    }

    verifyPullQueryServerState();
  }

  @Test
  public void shouldHandleErrorResponseFromStreamQuery() throws Exception {
    // Given
    ParseFailedException pfe = new ParseFailedException("invalid query blah");
    testEndpoints.setCreateQueryPublisherException(pfe);

    // When/Then
    assertErrorWhen(
        () -> client.streamQuery("bad query", DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES),
        "Received 400 response from server",
        "invalid query blah"
    );
  }

  @Test
  public void shouldExecutePullQuery() throws Exception {
    // When
    final List<Row> rows = client.executeQuery(DEFAULT_PULL_QUERY).get();

    // Then
    assertThat(rows, hasSize(DEFAULT_ROWS.size()));
    for (int i = 0; i < DEFAULT_ROWS.size(); i++) {
      assertThat(rows.get(i).values(), equalTo(rowWithIndex(i).getList()));
      assertThat(rows.get(i).columnNames(), equalTo(DEFAULT_COLUMN_NAMES));
      assertThat(rows.get(i).columnTypes(), equalTo(DEFAULT_COLUMN_TYPES));
    }

    verifyPullQueryServerState();
  }

  @Test
  public void shouldExecutePushQuery() throws Exception {
    // When
    final List<Row> rows =
        client.executeQuery(DEFAULT_PUSH_QUERY_WITH_LIMIT, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get();

    // Then
    assertThat(rows, hasSize(DEFAULT_ROWS.size()));
    for (int i = 0; i < DEFAULT_ROWS.size(); i++) {
      assertThat(rows.get(i).values(), equalTo(rowWithIndex(i).getList()));
      assertThat(rows.get(i).columnNames(), equalTo(DEFAULT_COLUMN_NAMES));
      assertThat(rows.get(i).columnTypes(), equalTo(DEFAULT_COLUMN_TYPES));
    }

    verifyPushQueryServerState(DEFAULT_PUSH_QUERY_WITH_LIMIT);
  }

  @Test
  public void shouldHandleErrorResponseFromExecuteQuery() throws Exception {
    // Given
    ParseFailedException pfe = new ParseFailedException("invalid query blah");
    testEndpoints.setCreateQueryPublisherException(pfe);

    // When/Then
    assertErrorWhen(
        () -> client.executeQuery("bad query"),
        "Received 400 response from server",
        "invalid query blah"
    );
  }

  protected Client createJavaClient() {
    return new ClientImpl(createJavaClientOptions(), vertx);
  }

  protected ClientOptions createJavaClientOptions() {
    return new ClientOptionsImpl()
        .setHost("localhost")
        .setPort(server.getListeners().get(0).getPort())
        .setUseTls(false);
  }

  private void verifyPushQueryServerState(final String sql) {
    verifyPushQueryServerState(sql, null);
  }

  private void verifyPushQueryServerState(final String sql, final String queryId) {
    assertThat(testEndpoints.getLastSql(), is(sql));
    assertThat(testEndpoints.getLastProperties(), is(BaseApiTest.DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES));

    if (queryId != null) {
      assertThat(server.getQueryIDs(), hasSize(1));
      assertThat(server.getQueryIDs().contains(new PushQueryId(queryId)), is(true));
    }
  }

  private void verifyPullQueryServerState() {
    assertThat(testEndpoints.getLastSql(), is(DEFAULT_PULL_QUERY));
    assertThat(testEndpoints.getLastProperties().getMap(), is(Collections.emptyMap()));

    assertThat(server.getQueryIDs(), hasSize(0));
  }

  private void shouldDeliver(final Publisher<Row> publisher, final int numRows) {
    TestSubscriber<Row> subscriber = new TestSubscriber<Row>() {
      @Override
      public synchronized void onSubscribe(final Subscription sub) {
        super.onSubscribe(sub);
        sub.request(numRows);
      }
    };
    publisher.subscribe(subscriber);
    assertThatEventually(subscriber::getValues, hasSize(numRows));
    for (int i = 0; i < numRows; i++) {
      assertThat(subscriber.getValues().get(i).values(), equalTo(rowWithIndex(i).getList()));
    }
    assertThat(subscriber.isCompleted(), equalTo(false));
    assertThat(subscriber.getError(), is(nullValue()));
  }

  private void assertErrorWhen(
      final Supplier<CompletableFuture<?>> queryRequest,
      final String... errorMessages
  ) throws Exception {
    // Given
    CountDownLatch latch = new CountDownLatch(1);

    // When
    final CompletableFuture<?> cf = queryRequest.get()
        .exceptionally(error -> {

          // Then
          assertThat(error, notNullValue());
          for (final String msg : errorMessages) {
            assertThat(error.getMessage(), containsString(msg));
          }

          latch.countDown();
          return null;
        });
    awaitLatch(latch, cf);
  }

  private static void awaitLatch(CountDownLatch latch, CompletableFuture<?> cf) throws Exception {
    // Log reason for any failures, else output of failed tests is uninformative
    cf.exceptionally(failure -> {
      System.out.println("Failure reason: " + failure.getMessage());
      return null;
    });

    awaitLatch(latch);
  }

  private static void awaitLatch(CountDownLatch latch) throws Exception {
    assertThat(latch.await(2000, TimeUnit.MILLISECONDS), is(true));
  }

  private static class TestSubscriber<T> implements Subscriber<T> {

    private Subscription sub;
    private boolean completed;
    private Throwable error;
    private final List<T> values = new ArrayList<>();

    public TestSubscriber() {
    }

    @Override
    public synchronized void onSubscribe(final Subscription sub) {
      this.sub = sub;
    }

    @Override
    public synchronized void onNext(final T value) {
      values.add(value);
    }

    @Override
    public synchronized void onError(final Throwable t) {
      this.error = t;
    }

    @Override
    public synchronized void onComplete() {
      this.completed = true;
    }

    public boolean isCompleted() {
      return completed;
    }

    public Throwable getError() {
      return error;
    }

    public List<T> getValues() {
      return values;
    }

    public Subscription getSub() {
      return sub;
    }
  }
}