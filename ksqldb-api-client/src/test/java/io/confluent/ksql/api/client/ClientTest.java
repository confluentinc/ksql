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

import static io.confluent.ksql.api.client.util.ClientTestUtil.awaitLatch;
import static io.confluent.ksql.api.client.util.ClientTestUtil.subscribeAndWait;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_BAD_REQUEST;
import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.api.BaseApiTest;
import io.confluent.ksql.api.TestQueryPublisher;
import io.confluent.ksql.api.client.QueryInfo.QueryType;
import io.confluent.ksql.api.client.exception.KsqlClientException;
import io.confluent.ksql.api.client.exception.KsqlException;
import io.confluent.ksql.api.client.impl.StreamedQueryResultImpl;
import io.confluent.ksql.api.client.util.ClientTestUtil;
import io.confluent.ksql.api.client.util.ClientTestUtil.TestSubscriber;
import io.confluent.ksql.api.client.util.RowUtil;
import io.confluent.ksql.api.server.KsqlApiException;
import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.ConnectorDescription;
import io.confluent.ksql.rest.entity.ConnectorList;
import io.confluent.ksql.rest.entity.CreateConnectorEntity;
import io.confluent.ksql.rest.entity.DropConnectorEntity;
import io.confluent.ksql.rest.entity.ErrorEntity;
import io.confluent.ksql.rest.entity.FieldInfo;
import io.confluent.ksql.rest.entity.FieldInfo.FieldType;
import io.confluent.ksql.rest.entity.FunctionDescriptionList;
import io.confluent.ksql.rest.entity.FunctionNameList;
import io.confluent.ksql.rest.entity.FunctionType;
import io.confluent.ksql.rest.entity.KafkaTopicInfo;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.PushQueryId;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionEntity;
import io.confluent.ksql.rest.entity.QueryStatusCount;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SchemaInfo;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TypeList;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryStatus;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo.ConnectorState;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientTest extends BaseApiTest {

  protected static final Logger log = LoggerFactory.getLogger(ClientTest.class);

  @SuppressWarnings("unchecked")
  protected static final List<String> DEFAULT_COLUMN_NAMES = BaseApiTest.DEFAULT_COLUMN_NAMES.getList();
  @SuppressWarnings("unchecked")
  protected static final List<ColumnType> DEFAULT_COLUMN_TYPES =
      RowUtil.columnTypesFromStrings(BaseApiTest.DEFAULT_COLUMN_TYPES.getList());
  protected static final Map<String, Object> DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES =
      BaseApiTest.DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES.getMap();
  protected static final String DEFAULT_PUSH_QUERY_WITH_LIMIT = "select * from foo emit changes limit 10;";
  protected static final List<KsqlArray> EXPECTED_ROWS = convertToClientRows(DEFAULT_JSON_ROWS);

  protected static final List<KsqlObject> INSERT_ROWS = generateInsertRows();
  protected static final List<JsonObject> EXPECTED_INSERT_ROWS = convertToJsonRows(INSERT_ROWS);

  protected static final String EXECUTE_STATEMENT_REQUEST_ACCEPTED_DOC =
      "The ksqlDB server accepted the statement issued via executeStatement(), but the response "
          + "received is of an unexpected format. ";
  protected static final String EXECUTE_STATEMENT_USAGE_DOC = "The executeStatement() method is only "
      + "for 'CREATE', 'CREATE ... AS SELECT', 'DROP', 'TERMINATE', and 'INSERT INTO ... AS "
      + "SELECT' statements. ";

  protected Client javaClient;

  @Override
  public void setUp() {
    super.setUp();

    this.javaClient = createJavaClient();
  }

  @Override
  protected WebClient createClient() {
    // Ensure these tests use Java client rather than WebClient (as in BaseApiTest)
    return null;
  }

  @Override
  protected void stopClient() {
    if (javaClient != null) {
      try {
        javaClient.close();
      } catch (Exception e) {
        log.error("Failed to close client", e);
      }
    }
  }

  @Test
  public void shouldStreamPushQueryAsync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult =
        javaClient.streamQuery(DEFAULT_PUSH_QUERY, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(DEFAULT_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(DEFAULT_COLUMN_TYPES));

    shouldReceiveRows(streamedQueryResult, false);

    String queryId = streamedQueryResult.queryID();
    assertThat(queryId, is(notNullValue()));
    verifyPushQueryServerState(DEFAULT_PUSH_QUERY, queryId);

    assertThat(streamedQueryResult.isComplete(), is(false));
  }

  @Test
  public void shouldStreamPushQuerySync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult =
        javaClient.streamQuery(DEFAULT_PUSH_QUERY, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(DEFAULT_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(DEFAULT_COLUMN_TYPES));

    for (int i = 0; i < DEFAULT_JSON_ROWS.size(); i++) {
      final Row row = streamedQueryResult.poll();
      verifyRowWithIndex(row, i);
    }

    String queryId = streamedQueryResult.queryID();
    assertThat(queryId, is(notNullValue()));
    verifyPushQueryServerState(DEFAULT_PUSH_QUERY, queryId);

    assertThat(streamedQueryResult.isComplete(), is(false));
  }

  @Test
  public void shouldStreamPullQueryAsync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult =
        javaClient.streamQuery(DEFAULT_PULL_QUERY).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(DEFAULT_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(DEFAULT_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(nullValue()));

    shouldReceiveRows(streamedQueryResult, true);

    verifyPullQueryServerState();

    assertThatEventually(streamedQueryResult::isComplete, is(true));
  }

  @Test
  public void shouldStreamPullQuerySync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult =
        javaClient.streamQuery(DEFAULT_PULL_QUERY).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(DEFAULT_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(DEFAULT_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(nullValue()));

    for (int i = 0; i < DEFAULT_JSON_ROWS.size(); i++) {
      final Row row = streamedQueryResult.poll();
      verifyRowWithIndex(row, i);
    }
    assertThat(streamedQueryResult.poll(), is(nullValue()));

    verifyPullQueryServerState();

    assertThatEventually(streamedQueryResult::isComplete, is(true));
  }

  @Test
  public void shouldStreamPushQueryWithLimitAsync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult =
        javaClient.streamQuery(DEFAULT_PUSH_QUERY_WITH_LIMIT, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(DEFAULT_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(DEFAULT_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(notNullValue()));

    shouldReceiveRows(streamedQueryResult, true);

    verifyPushQueryServerState(DEFAULT_PUSH_QUERY_WITH_LIMIT);

    assertThatEventually(streamedQueryResult::isComplete, is(true));
  }

  @Test
  public void shouldStreamPushQueryWithLimitSync() throws Exception {
    // When
    final StreamedQueryResult streamedQueryResult =
        javaClient.streamQuery(DEFAULT_PUSH_QUERY_WITH_LIMIT, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get();

    // Then
    assertThat(streamedQueryResult.columnNames(), is(DEFAULT_COLUMN_NAMES));
    assertThat(streamedQueryResult.columnTypes(), is(DEFAULT_COLUMN_TYPES));
    assertThat(streamedQueryResult.queryID(), is(notNullValue()));

    for (int i = 0; i < DEFAULT_JSON_ROWS.size(); i++) {
      final Row row = streamedQueryResult.poll();
      verifyRowWithIndex(row, i);
    }
    assertThat(streamedQueryResult.poll(), is(nullValue()));

    verifyPushQueryServerState(DEFAULT_PUSH_QUERY_WITH_LIMIT);

    assertThatEventually(streamedQueryResult::isComplete, is(true));
  }

  @Test
  public void shouldHandleErrorResponseFromStreamQuery() {
    // Given
    ParseFailedException pfe = new ParseFailedException("invalid query blah");
    testEndpoints.setCreateQueryPublisherException(pfe);

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.streamQuery("bad query", DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("invalid query blah"));
  }

  @Test
  public void shouldFailPollStreamedQueryResultIfSubscribed() throws Exception {
    // Given
    final StreamedQueryResult streamedQueryResult =
        javaClient.streamQuery(DEFAULT_PUSH_QUERY, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get();
    subscribeAndWait(streamedQueryResult);

    // When
    final Exception e = assertThrows(IllegalStateException.class, streamedQueryResult::poll);

    // Then
    assertThat(e.getMessage(), containsString("Cannot poll if subscriber has been set"));
  }

  @Test
  public void shouldFailSubscribeStreamedQueryResultIfPolling() throws Exception {
    // Given
    final StreamedQueryResult streamedQueryResult =
        javaClient.streamQuery(DEFAULT_PUSH_QUERY, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get();
    streamedQueryResult.poll(Duration.ofNanos(1));

    // When
    final Exception e = assertThrows(
        IllegalStateException.class,
        () -> streamedQueryResult.subscribe(new TestSubscriber<>())
    );

    // Then
    assertThat(e.getMessage(), containsString("Cannot set subscriber if polling"));
  }

  @Test
  public void shouldFailPollStreamedQueryResultIfFailed() throws Exception {
    // Given
    final StreamedQueryResult streamedQueryResult =
        javaClient.streamQuery(DEFAULT_PUSH_QUERY, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get();
    sendQueryPublisherError();
    assertThatEventually(streamedQueryResult::isFailed, is(true));

    // When
    final Exception e = assertThrows(
        IllegalStateException.class,
        () -> streamedQueryResult.poll()
    );

    // Then
    assertThat(e.getMessage(), containsString("Cannot poll on StreamedQueryResult that has failed"));
  }

  @Test
  public void shouldReturnFromPollStreamedQueryResultOnError() throws Exception {
    // Given
    final StreamedQueryResult streamedQueryResult =
        javaClient.streamQuery(DEFAULT_PUSH_QUERY, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get();
    for (int i = 0; i < DEFAULT_JSON_ROWS.size(); i++) {
      streamedQueryResult.poll();
    }

    CountDownLatch pollStarted = new CountDownLatch(1);
    CountDownLatch pollReturned = new CountDownLatch(1);
    new Thread(() -> {
      // This poll() call blocks as there are no more rows to be returned
      final Row row = StreamedQueryResultImpl.pollWithCallback(streamedQueryResult, () -> pollStarted.countDown());
      assertThat(row, is(nullValue()));
      pollReturned.countDown();
    }).start();
    awaitLatch(pollStarted);

    // When
    sendQueryPublisherError();

    // Then: poll() call terminates because of the error
    awaitLatch(pollReturned);
  }

  @Test
  public void shouldPropagateErrorWhenStreamingFromStreamQuery() throws Exception {
    // Given
    final StreamedQueryResult streamedQueryResult =
        javaClient.streamQuery(DEFAULT_PUSH_QUERY, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get();
    final TestSubscriber<Row> subscriber = subscribeAndWait(streamedQueryResult);

    // When
    sendQueryPublisherError();

    // Then
    assertThatEventually(subscriber::getError, is(notNullValue()));
    assertThat(subscriber.getError(), instanceOf(KsqlException.class));
    assertThat(subscriber.getError().getMessage(), containsString("Error in processing query. Check server logs for details."));

    assertThatEventually(streamedQueryResult::isFailed, is(true));
    assertThat(streamedQueryResult.isComplete(), is(false));
    assertThat(subscriber.isCompleted(), equalTo(false));
  }

  @Test
  public void shouldDeliverBufferedRowsViaPollIfComplete() throws Exception {
    // Given
    final StreamedQueryResult streamedQueryResult =
        javaClient.streamQuery(DEFAULT_PUSH_QUERY_WITH_LIMIT, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get();
    assertThatEventually(streamedQueryResult::isComplete, is(true));

    // When / Then
    for (int i = 0; i < DEFAULT_JSON_ROWS.size(); i++) {
      final Row row = streamedQueryResult.poll();
      verifyRowWithIndex(row, i);
    }
    assertThat(streamedQueryResult.poll(), is(nullValue()));
  }

  @Test
  public void shouldDeliverBufferedRowsOnErrorIfStreaming() throws Exception {
    // Given
    final StreamedQueryResult streamedQueryResult =
        javaClient.streamQuery(DEFAULT_PUSH_QUERY, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get();
    TestSubscriber<Row> subscriber = subscribeAndWait(streamedQueryResult);
    sendQueryPublisherError();
    assertThatEventually(streamedQueryResult::isFailed, is(true));
    assertThat(subscriber.getValues(), hasSize(0));

    // When
    subscriber.getSub().request(DEFAULT_JSON_ROWS.size());

    // Then
    assertThatEventually(subscriber::getError, is(notNullValue()));
    assertThatEventually(subscriber::getValues, hasSize(DEFAULT_JSON_ROWS.size()));
    verifyRows(subscriber.getValues());
  }

  @Test
  public void shouldFailSubscribeStreamedQueryResultOnError() throws Exception {
    // Given
    final StreamedQueryResult streamedQueryResult =
        javaClient.streamQuery(DEFAULT_PUSH_QUERY, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get();
    sendQueryPublisherError();
    assertThatEventually(streamedQueryResult::isFailed, is(true));

    // When
    final Exception e = assertThrows(
        IllegalStateException.class,
        () -> streamedQueryResult.subscribe(new TestSubscriber<>())
    );

    // Then
    assertThat(e.getMessage(), containsString("Cannot subscribe to failed publisher"));
  }

  @Test
  public void shouldAllowSubscribeStreamedQueryResultIfComplete() throws Exception {
    // Given
    final StreamedQueryResult streamedQueryResult =
        javaClient.streamQuery(DEFAULT_PUSH_QUERY_WITH_LIMIT, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get();
    assertThatEventually(streamedQueryResult::isComplete, is(true));

    // When
    TestSubscriber<Row> subscriber = subscribeAndWait(streamedQueryResult);
    assertThat(subscriber.getValues(), hasSize(0));
    subscriber.getSub().request(DEFAULT_JSON_ROWS.size());

    // Then
    assertThatEventually(subscriber::getValues, hasSize(DEFAULT_JSON_ROWS.size()));
    verifyRows(subscriber.getValues());
    assertThat(subscriber.getError(), is(nullValue()));
  }

  @Test
  public void shouldExecutePullQuery() throws Exception {
    // When
    final BatchedQueryResult batchedQueryResult = javaClient.executeQuery(DEFAULT_PULL_QUERY);

    // Then
    assertThat(batchedQueryResult.queryID().get(), is(nullValue()));

    verifyRows(batchedQueryResult.get());

    verifyPullQueryServerState();
  }

  @Test
  public void shouldExecutePushWithLimitQuery() throws Exception {
    // When
    final BatchedQueryResult batchedQueryResult =
        javaClient.executeQuery(DEFAULT_PUSH_QUERY_WITH_LIMIT, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES);

    // Then
    assertThat(batchedQueryResult.queryID().get(), is(notNullValue()));

    verifyRows(batchedQueryResult.get());

    verifyPushQueryServerState(DEFAULT_PUSH_QUERY_WITH_LIMIT);
  }

  @Test
  public void shouldHandleErrorResponseFromExecuteQuery() {
    // Given
    ParseFailedException pfe = new ParseFailedException("invalid query blah");
    testEndpoints.setCreateQueryPublisherException(pfe);

    // When
    final BatchedQueryResult batchedQueryResult = javaClient.executeQuery("bad query");
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        batchedQueryResult::get
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("invalid query blah"));

    // queryID future should also be completed exceptionally
    final Exception queryIdException = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> batchedQueryResult.queryID().get()
    );
    assertThat(queryIdException.getCause(), instanceOf(KsqlClientException.class));
    assertThat(queryIdException.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(queryIdException.getCause().getMessage(), containsString("invalid query blah"));
  }

  @Test
  public void shouldTerminatePushQueryIssuedViaStreamQuery() throws Exception {
    // Given
    final StreamedQueryResult streamedQueryResult =
        javaClient.streamQuery(DEFAULT_PUSH_QUERY, DEFAULT_PUSH_QUERY_REQUEST_PROPERTIES).get();
    final String queryId = streamedQueryResult.queryID();
    assertThat(queryId, is(notNullValue()));

    // Query is running on server, and StreamedQueryResult is not complete
    assertThat(server.getQueryIDs(), hasSize(1));
    assertThat(server.getQueryIDs().contains(new PushQueryId(queryId)), is(true));
    assertThat(streamedQueryResult.isComplete(), is(false));

    // When
    javaClient.terminatePushQuery(queryId).get();

    // Then: query is no longer running on server, and StreamedQueryResult is complete
    assertThat(server.getQueryIDs(), hasSize(0));
    assertThatEventually(streamedQueryResult::isComplete, is(true));
  }

  @Test
  public void shouldTerminatePushQueryIssuedViaExecuteQuery() throws Exception {
    // Given
    // Issue non-terminating push query via executeQuery(). This is NOT an expected use case
    final BatchedQueryResult batchedQueryResult = javaClient.executeQuery(DEFAULT_PUSH_QUERY);
    final String queryId = batchedQueryResult.queryID().get();
    assertThat(queryId, is(notNullValue()));

    // Query is running on server, and BatchedQueryResult is not complete
    assertThat(server.getQueryIDs(), hasSize(1));
    assertThat(server.getQueryIDs().contains(new PushQueryId(queryId)), is(true));
    assertThat(batchedQueryResult.isDone(), is(false));

    // When
    javaClient.terminatePushQuery(queryId).get();

    // Then: query is no longer running on server, and BatchedQueryResult is complete
    assertThat(server.getQueryIDs(), hasSize(0));
    assertThatEventually(batchedQueryResult::isDone, is(true));
    assertThat(batchedQueryResult.isCompletedExceptionally(), is(false));
  }

  @Test
  public void shouldHandleErrorResponseFromTerminatePushQuery() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.terminatePushQuery("nonexistent query ID").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("No query with id"));
    assertThat(e.getCause().getMessage(), containsString("Error code: " + ERROR_CODE_BAD_REQUEST));
  }

  @Test
  public void shouldInsertInto() throws Exception {
    // When
    javaClient.insertInto("test-stream", INSERT_ROWS.get(0)).get();

    // Then
    assertThatEventually(() -> testEndpoints.getInsertsSubscriber().getRowsInserted(), hasSize(1));
    assertThat(testEndpoints.getInsertsSubscriber().getRowsInserted().get(0), is(EXPECTED_INSERT_ROWS.get(0)));
    assertThatEventually(() -> testEndpoints.getInsertsSubscriber().isCompleted(), is(true));
    assertThatEventually(() -> testEndpoints.getInsertsSubscriber().isClosed(), is(true));
    assertThat(testEndpoints.getLastTarget(), is("test-stream"));
  }

  @Test
  public void shouldHandleErrorResponseFromInsertInto() {
    // Given
    KsqlApiException exception = new KsqlApiException("Cannot insert into a table", ERROR_CODE_BAD_REQUEST);
    testEndpoints.setCreateInsertsSubscriberException(exception);

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.insertInto("a-table", INSERT_ROWS.get(0)).get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("Cannot insert into a table"));
  }

  @Test
  public void shouldHandleErrorFromInsertInto() {
    // Given
    testEndpoints.setAcksBeforePublisherError(0);

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.insertInto("test-stream", INSERT_ROWS.get(0)).get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received error from /inserts-stream"));
    assertThat(e.getCause().getMessage(), containsString("Error code: 50000"));
    assertThat(e.getCause().getMessage(), containsString("Message: Error in processing inserts. Check server logs for details."));
  }

  @Test
  public void shouldStreamInserts() throws Exception {
    // Given:
    final InsertsPublisher insertsPublisher = new InsertsPublisher();

    // When:
    final AcksPublisher acksPublisher = javaClient.streamInserts("test-stream", insertsPublisher).get();
    for (final KsqlObject row : INSERT_ROWS) {
      insertsPublisher.accept(row);
    }

    TestSubscriber<InsertAck> acksSubscriber = subscribeAndWait(acksPublisher);
    acksSubscriber.getSub().request(INSERT_ROWS.size());

    // Then:
    assertThatEventually(() -> testEndpoints.getInsertsSubscriber().getRowsInserted(), hasSize(INSERT_ROWS.size()));
    for (int i = 0; i < INSERT_ROWS.size(); i++) {
      assertThat(testEndpoints.getInsertsSubscriber().getRowsInserted().get(i), is(EXPECTED_INSERT_ROWS.get(i)));
    }
    assertThat(testEndpoints.getLastTarget(), is("test-stream"));

    assertThatEventually(acksSubscriber::getValues, hasSize(INSERT_ROWS.size()));
    assertThat(acksSubscriber.getError(), is(nullValue()));
    for (int i = 0; i < INSERT_ROWS.size(); i++) {
      assertThat(acksSubscriber.getValues().get(i).seqNum(), is(Long.valueOf(i)));
    }
    assertThat(acksSubscriber.isCompleted(), is(false));

    assertThat(acksPublisher.isComplete(), is(false));
    assertThat(acksPublisher.isFailed(), is(false));

    // When:
    insertsPublisher.complete();

    // Then:
    assertThatEventually(acksPublisher::isComplete, is(true));
    assertThat(acksPublisher.isFailed(), is(false));
    assertThatEventually(acksSubscriber::isCompleted, is(true));
  }

  @Test
  public void shouldHandleErrorResponseFromStreamInserts() {
    // Given
    KsqlApiException exception = new KsqlApiException("Cannot insert into a table", ERROR_CODE_BAD_REQUEST);
    testEndpoints.setCreateInsertsSubscriberException(exception);

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.streamInserts("a-table", new InsertsPublisher()).get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 400 response from server"));
    assertThat(e.getCause().getMessage(), containsString("Cannot insert into a table"));
  }

  @Test
  public void shouldHandleErrorFromStreamInserts() throws Exception {
    // Given:
    testEndpoints.setAcksBeforePublisherError(INSERT_ROWS.size() - 1);
    final InsertsPublisher insertsPublisher = new InsertsPublisher();

    // When:
    final AcksPublisher acksPublisher = javaClient.streamInserts("test-stream", insertsPublisher).get();
    for (int i = 0; i < INSERT_ROWS.size(); i++) {
      insertsPublisher.accept(INSERT_ROWS.get(i));
    }

    TestSubscriber<InsertAck> acksSubscriber = subscribeAndWait(acksPublisher);
    acksSubscriber.getSub().request(INSERT_ROWS.size() - 1); // Error is sent even if not requested

    // Then:
    // No ack is emitted for the row that generates the error, but the row still counts as having been inserted
    assertThatEventually(() -> testEndpoints.getInsertsSubscriber().getRowsInserted(), hasSize(INSERT_ROWS.size()));
    for (int i = 0; i < INSERT_ROWS.size(); i++) {
      assertThat(testEndpoints.getInsertsSubscriber().getRowsInserted().get(i), is(EXPECTED_INSERT_ROWS.get(i)));
    }
    assertThat(testEndpoints.getLastTarget(), is("test-stream"));

    assertThatEventually(acksSubscriber::getValues, hasSize(INSERT_ROWS.size() - 1));
    for (int i = 0; i < INSERT_ROWS.size() - 1; i++) {
      assertThat(acksSubscriber.getValues().get(i).seqNum(), is(Long.valueOf(i)));
    }
    assertThatEventually(acksSubscriber::getError, is(notNullValue()));

    assertThat(acksPublisher.isFailed(), is(true));
    assertThat(acksPublisher.isComplete(), is(false));
  }

  @Test
  public void shouldExecuteStatementWithQueryId() throws Exception {
    // Given
    final CommandStatusEntity entity = new CommandStatusEntity(
        "CSAS;",
        new CommandId("STREAM", "FOO", "CREATE"),
        new CommandStatus(
            CommandStatus.Status.SUCCESS,
            "Success",
            Optional.of(new QueryId("CSAS_0"))
        ),
        0L
    );
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    final Map<String, Object> properties = ImmutableMap.of("auto.offset.reset", "earliest");

    // When
    final ExecuteStatementResult result = javaClient.executeStatement("CSAS;", properties).get();

    // Then
    assertThat(testEndpoints.getLastSql(), is("CSAS;"));
    assertThat(testEndpoints.getLastProperties(), is(new JsonObject().put("auto.offset.reset", "earliest")));
    assertThat(result.queryId(), is(Optional.of("CSAS_0")));
  }

  @Test
  public void shouldExecuteStatementWithoutQueryId() throws Exception {
    // Given
    final CommandStatusEntity entity = new CommandStatusEntity(
        "CSAS;",
        new CommandId("STREAM", "FOO", "CREATE"),
        new CommandStatus(
            CommandStatus.Status.SUCCESS,
            "Success"
        ),
        0L
    );
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    final Map<String, Object> properties = ImmutableMap.of("auto.offset.reset", "earliest");

    // When
    final ExecuteStatementResult result = javaClient.executeStatement("CSAS;", properties).get();

    // Then
    assertThat(testEndpoints.getLastSql(), is("CSAS;"));
    assertThat(testEndpoints.getLastProperties(), is(new JsonObject().put("auto.offset.reset", "earliest")));
    assertThat(result.queryId(), is(Optional.empty()));
  }

  @Test
  public void shouldHandleErrorResponseFromExecuteStatement() {
    // Given
    io.confluent.ksql.util.KsqlException exception = new io.confluent.ksql.util.KsqlException("something bad");
    testEndpoints.setExecuteKsqlRequestException(exception);

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("CSAS;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 500 response from server"));
    assertThat(e.getCause().getMessage(), containsString("something bad"));
  }

  @Test
  public void shouldRejectMultipleRequestsFromExecuteStatement() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("CSAS; CTAS;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(),
        containsString("executeStatement() may only be used to execute one statement at a time"));
  }

  @Test
  public void shouldRejectRequestWithMissingSemicolonFromExecuteStatement() {
    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("missing semicolon").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(),
        containsString("Missing semicolon in SQL for executeStatement() request"));
  }

  @Test
  public void shouldFailOnNoEntitiesFromExecuteStatement() {
    // Given
    testEndpoints.setKsqlEndpointResponse(Collections.emptyList());

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("set property;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_REQUEST_ACCEPTED_DOC));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
  }

  @Test
  public void shouldFailToListStreamsViaExecuteStatement() {
    // Given
    final StreamsList entity = new StreamsList("list streams;", Collections.emptyList());
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("list streams;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
    assertThat(e.getCause().getMessage(),
        containsString("Use the listStreams() method instead"));
  }

  @Test
  public void shouldFailToListTablesViaExecuteStatement() {
    // Given
    final TablesList entity = new TablesList("list tables;", Collections.emptyList());
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("list tables;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
    assertThat(e.getCause().getMessage(),
        containsString("Use the listTables() method instead"));
  }

  @Test
  public void shouldFailToListTopicsViaExecuteStatement() {
    // Given
    final KafkaTopicsList entity = new KafkaTopicsList("list topics;", Collections.emptyList());
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("list topics;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
    assertThat(e.getCause().getMessage(),
        containsString("Use the listTopics() method instead"));
  }

  @Test
  public void shouldFailToListQueriesViaExecuteStatement() {
    // Given
    final Queries entity = new Queries("list queries;", Collections.emptyList());
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("list queries;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
    assertThat(e.getCause().getMessage(),
        containsString("Use the listQueries() method instead"));
  }

  @Test
  public void shouldFailToDescribeSourceViaExecuteStatement() {
    // Given
    final SourceDescriptionEntity entity = new SourceDescriptionEntity(
        "describe source;",
        new io.confluent.ksql.rest.entity.SourceDescription(
            "name",
            Optional.empty(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            "type",
            "timestamp",
            "statistics",
            "errorStats",
            false,
            "keyFormat",
            "valueFormat",
            "topic",
            4,
            1,
            "statement",
            Collections.emptyList()),
        Collections.emptyList());
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("describe source;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
    assertThat(e.getCause().getMessage(),
        containsString("does not currently support 'DESCRIBE <STREAM/TABLE>' statements"));
  }

  @Test
  public void shouldFailToListFunctionsViaExecuteStatement() {
    // Given
    final FunctionNameList entity = new FunctionNameList("list functions;", Collections.emptyList());
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("list functions;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
    assertThat(e.getCause().getMessage(),
        containsString("does not currently support 'DESCRIBE <FUNCTION>' statements or listing functions"));
  }

  @Test
  public void shouldFailToDescribeFunctionViaExecuteStatement() {
    // Given
    final FunctionDescriptionList entity = new FunctionDescriptionList(
        "describe function;", "SUM", "sum", "Confluent",
        "version", "path", Collections.emptyList(), FunctionType.AGGREGATE);
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("describe function;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
    assertThat(e.getCause().getMessage(),
        containsString("does not currently support 'DESCRIBE <FUNCTION>' statements or listing functions"));
  }

  @Test
  public void shouldFailToExplainQueryViaExecuteStatement() {
    // Given
    final QueryDescriptionEntity entity = new QueryDescriptionEntity(
        "explain query;",
        new QueryDescription(new QueryId("id"), "sql", Optional.empty(),
            Collections.emptyList(), Collections.emptySet(), Collections.emptySet(), "topology",
            "executionPlan", Collections.emptyMap(), Collections.emptyMap(),
            KsqlQueryType.PERSISTENT, Collections.emptyList()));
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("explain query;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
    assertThat(e.getCause().getMessage(),
        containsString("does not currently support 'EXPLAIN <QUERY_ID>' statements"));
  }

  @Test
  public void shouldFailToListPropertiesViaExecuteStatement() {
    // Given
    final PropertiesList entity = new PropertiesList("list properties;",
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("list properties;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
    assertThat(e.getCause().getMessage(),
        containsString("does not currently support listing properties"));
  }

  @Test
  public void shouldFailToListTypesViaExecuteStatement() {
    // Given
    final TypeList entity = new TypeList("list types;", Collections.emptyMap());
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("list types;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
    assertThat(e.getCause().getMessage(),
        containsString("does not currently support listing custom types"));
  }

  @Test
  public void shouldFailToListConnectorsViaExecuteStatement() {
    // Given
    final ConnectorList entity = new ConnectorList(
        "list connectors;", Collections.emptyList(), Collections.emptyList());
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("list connectors;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
    assertThat(e.getCause().getMessage(),
        containsString("does not currently support listing connectors"));
  }

  @Test
  public void shouldFailToDescribeConnectorViaExecuteStatement() {
    // Given
    final ConnectorDescription entity = new ConnectorDescription("describe connector;",
        "connectorClass",
        new ConnectorStateInfo(
            "name",
            new ConnectorState("state", "worker", "msg"),
            Collections.emptyList(),
            ConnectorType.SOURCE),
        Collections.emptyList(), Collections.singletonList("topic"), Collections.emptyList());
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("describe connector;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
    assertThat(e.getCause().getMessage(),
        containsString("does not currently support 'DESCRIBE <CONNECTOR>' statements"));
  }

  @Test
  public void shouldFailToCreateConnectorViaExecuteStatement() {
    // Given
    final CreateConnectorEntity entity = new CreateConnectorEntity("create connector;",
        new ConnectorInfo("name", Collections.emptyMap(), Collections.emptyList(), ConnectorType.SOURCE));
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("create connector;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_REQUEST_ACCEPTED_DOC));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
    assertThat(e.getCause().getMessage(),
        containsString("does not currently support 'CREATE CONNECTOR' statements"));
  }

  @Test
  public void shouldFailToDropConnectorViaExecuteStatement() {
    // Given
    final DropConnectorEntity entity = new DropConnectorEntity("drop connector;", "name");
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("drop connector;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_REQUEST_ACCEPTED_DOC));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
    assertThat(e.getCause().getMessage(),
        containsString("does not currently support 'DROP CONNECTOR' statements"));
  }

  @Test
  public void shouldFailOnErrorEntityFromExecuteStatement() {
    // Given
    final ErrorEntity entity = new ErrorEntity("create connector;", "error msg");
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.executeStatement("create connector;").get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString(EXECUTE_STATEMENT_USAGE_DOC));
    assertThat(e.getCause().getMessage(),
        containsString("does not currently support statements for creating, dropping, "
            + "listing, or describing connectors"));
  }

  @Test
  public void shouldListStreams() throws Exception {
    // Given
    final List<SourceInfo.Stream> expectedStreams = new ArrayList<>();
    expectedStreams.add(new SourceInfo.Stream("stream1", "topic1", "JSON"));
    expectedStreams.add(new SourceInfo.Stream("stream2", "topic2", "AVRO"));
    final StreamsList entity = new StreamsList("list streams;", expectedStreams);
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final List<StreamInfo> streams = javaClient.listStreams().get();

    // Then
    assertThat(streams, hasSize(expectedStreams.size()));
    assertThat(streams.get(0).getName(), is("stream1"));
    assertThat(streams.get(0).getTopic(), is("topic1"));
    assertThat(streams.get(0).getFormat(), is("JSON"));
    assertThat(streams.get(1).getName(), is("stream2"));
    assertThat(streams.get(1).getTopic(), is("topic2"));
    assertThat(streams.get(1).getFormat(), is("AVRO"));
  }

  @Test
  public void shouldListTables() throws Exception {
    // Given
    final List<SourceInfo.Table> expectedTables = new ArrayList<>();
    expectedTables.add(new SourceInfo.Table("table1", "topic1", "JSON", true));
    expectedTables.add(new SourceInfo.Table("table2", "topic2", "AVRO", false));
    final TablesList entity = new TablesList("list tables;", expectedTables);
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final List<TableInfo> tables = javaClient.listTables().get();

    // Then
    assertThat(tables, hasSize(expectedTables.size()));
    assertThat(tables.get(0).getName(), is("table1"));
    assertThat(tables.get(0).getTopic(), is("topic1"));
    assertThat(tables.get(0).getFormat(), is("JSON"));
    assertThat(tables.get(0).isWindowed(), is(true));
    assertThat(tables.get(1).getName(), is("table2"));
    assertThat(tables.get(1).getTopic(), is("topic2"));
    assertThat(tables.get(1).getFormat(), is("AVRO"));
    assertThat(tables.get(1).isWindowed(), is(false));
  }

  @Test
  public void shouldListTopics() throws Exception {
    // Given
    final List<KafkaTopicInfo> expectedTopics = new ArrayList<>();
    expectedTopics.add(new KafkaTopicInfo("topic1", ImmutableList.of(2, 2, 2)));
    expectedTopics.add(new KafkaTopicInfo("topic2", ImmutableList.of(1, 1)));
    final KafkaTopicsList entity = new KafkaTopicsList("list topics;", expectedTopics);
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final List<TopicInfo> topics = javaClient.listTopics().get();

    // Then
    assertThat(topics, hasSize(expectedTopics.size()));
    assertThat(topics.get(0).getName(), is("topic1"));
    assertThat(topics.get(0).getPartitions(), is(3));
    assertThat(topics.get(0).getReplicasPerPartition(), is(ImmutableList.of(2, 2, 2)));
    assertThat(topics.get(1).getName(), is("topic2"));
    assertThat(topics.get(1).getPartitions(), is(2));
    assertThat(topics.get(1).getReplicasPerPartition(), is(ImmutableList.of(1, 1)));
  }

  @Test
  public void shouldHandleErrorFromListTopics() {
    // Given
    KafkaResponseGetFailedException exception = new KafkaResponseGetFailedException(
        "Failed to retrieve Kafka Topic names", new RuntimeException("boom"));
    testEndpoints.setExecuteKsqlRequestException(exception);

    // When
    final Exception e = assertThrows(
        ExecutionException.class, // thrown from .get() when the future completes exceptionally
        () -> javaClient.listTopics().get()
    );

    // Then
    assertThat(e.getCause(), instanceOf(KsqlClientException.class));
    assertThat(e.getCause().getMessage(), containsString("Received 500 response from server"));
    assertThat(e.getCause().getMessage(), containsString("Failed to retrieve Kafka Topic names"));
  }

  @Test
  public void shouldListQueries() throws Exception {
    // Given
    final List<RunningQuery> expectedQueries = new ArrayList<>();
    expectedQueries.add(new RunningQuery(
        "sql1",
        ImmutableSet.of("sink"),
        ImmutableSet.of("sink_topic"),
        new QueryId("a_persistent_query"),
        new QueryStatusCount(ImmutableMap.of(KsqlQueryStatus.RUNNING, 1)),
        KsqlQueryType.PERSISTENT));
    expectedQueries.add(new RunningQuery(
        "sql2",
        Collections.emptySet(),
        Collections.emptySet(),
        new QueryId("a_push_query"),
        new QueryStatusCount(),
        KsqlQueryType.PUSH));
    final Queries entity = new Queries("list queries;", expectedQueries);
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final List<QueryInfo> queries = javaClient.listQueries().get();

    // Then
    assertThat(queries, hasSize(expectedQueries.size()));
    assertThat(queries.get(0).getQueryType(), is(QueryType.PERSISTENT));
    assertThat(queries.get(0).getId(), is("a_persistent_query"));
    assertThat(queries.get(0).getSql(), is("sql1"));
    assertThat(queries.get(0).getSink(), is(Optional.of("sink")));
    assertThat(queries.get(0).getSinkTopic(), is(Optional.of("sink_topic")));
    assertThat(queries.get(1).getQueryType(), is(QueryType.PUSH));
    assertThat(queries.get(1).getId(), is("a_push_query"));
    assertThat(queries.get(1).getSql(), is("sql2"));
    assertThat(queries.get(1).getSink(), is(Optional.empty()));
    assertThat(queries.get(1).getSinkTopic(), is(Optional.empty()));
  }

  @Test
  public void shouldDescribeSource() throws Exception {
    // Given
    final io.confluent.ksql.rest.entity.SourceDescription sd =
        new io.confluent.ksql.rest.entity.SourceDescription(
            "name",
            Optional.of(WindowType.TUMBLING),
            Collections.singletonList(new RunningQuery(
                "query_sql",
                ImmutableSet.of("sink"),
                ImmutableSet.of("sink_topic"),
                new QueryId("a_persistent_query"),
                new QueryStatusCount(ImmutableMap.of(KsqlQueryStatus.RUNNING, 1)),
                KsqlQueryType.PERSISTENT)),
            Collections.emptyList(),
            ImmutableList.of(
                new FieldInfo("f1", new SchemaInfo(SqlBaseType.STRING, null, null), Optional.of(FieldType.KEY)),
                new FieldInfo("f2", new SchemaInfo(SqlBaseType.INTEGER, null, null), Optional.empty())),
            "TABLE",
            "",
            "",
            "",
            false,
            "KAFKA",
            "JSON",
            "topic",
            4,
            1,
            "sql",
            Collections.emptyList()
        );
    final SourceDescriptionEntity entity = new SourceDescriptionEntity(
        "describe source;", sd, Collections.emptyList());
    testEndpoints.setKsqlEndpointResponse(Collections.singletonList(entity));

    // When
    final SourceDescription description = javaClient.describeSource("source").get();

    // Then
    assertThat(description.name(), is("name"));
    assertThat(description.type(), is("TABLE"));
    assertThat(description.fields(), hasSize(2));
    assertThat(description.fields().get(0).name(), is("f1"));
    assertThat(description.fields().get(0).type().getType(), is(ColumnType.Type.STRING));
    assertThat(description.fields().get(0).isKey(), is(true));
    assertThat(description.fields().get(1).name(), is("f2"));
    assertThat(description.fields().get(1).type().getType(), is(ColumnType.Type.INTEGER));
    assertThat(description.fields().get(1).isKey(), is(false));
    assertThat(description.topic(), is("topic"));
    assertThat(description.keyFormat(), is("KAFKA"));
    assertThat(description.valueFormat(), is("JSON"));
    assertThat(description.readQueries(), hasSize(1));
    assertThat(description.readQueries().get(0).getQueryType(), is(QueryType.PERSISTENT));
    assertThat(description.readQueries().get(0).getId(), is("a_persistent_query"));
    assertThat(description.readQueries().get(0).getSql(), is("query_sql"));
    assertThat(description.readQueries().get(0).getSink(), is(Optional.of("sink")));
    assertThat(description.readQueries().get(0).getSinkTopic(), is(Optional.of("sink_topic")));
    assertThat(description.writeQueries(), hasSize(0));
    assertThat(description.timestampColumn(), is(Optional.empty()));
    assertThat(description.windowType(), is(Optional.of("TUMBLING")));
    assertThat(description.sqlStatement(), is("sql"));
  }

  protected Client createJavaClient() {
    return Client.create(createJavaClientOptions(), vertx);
  }

  protected ClientOptions createJavaClientOptions() {
    return ClientOptions.create()
        .setHost("localhost")
        .setPort(server.getListeners().get(0).getPort());
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

  private void sendQueryPublisherError() {
    final Set<TestQueryPublisher> queryPublishers = testEndpoints.getQueryPublishers();
    assertThat(queryPublishers, hasSize(1));
    final TestQueryPublisher queryPublisher = queryPublishers.stream().findFirst().get();
    queryPublisher.sendError();
  }

  private static void shouldReceiveRows(
      final Publisher<Row> publisher,
      final boolean subscriberCompleted
  ) {
    ClientTestUtil.shouldReceiveRows(
        publisher,
        DEFAULT_JSON_ROWS.size(),
        ClientTest::verifyRows,
        subscriberCompleted
    );
  }

  private static void verifyRows(final List<Row> rows) {
    assertThat(rows, hasSize(DEFAULT_JSON_ROWS.size()));
    for (int i = 0; i < DEFAULT_JSON_ROWS.size(); i++) {
      verifyRowWithIndex(rows.get(i), i);
    }
  }

  private static void verifyRowWithIndex(final Row row, final int index) {
    // verify metadata
    assertThat(row.values(), equalTo(EXPECTED_ROWS.get(index)));
    assertThat(row.columnNames(), equalTo(DEFAULT_COLUMN_NAMES));
    assertThat(row.columnTypes(), equalTo(DEFAULT_COLUMN_TYPES));

    // verify type-based getters
    assertThat(row.getString("f_str"), is("foo" + index));
    assertThat(row.getInteger("f_int"), is(index));
    assertThat(row.getBoolean("f_bool"), is(index % 2 == 0));
    assertThat(row.getLong("f_long"), is(Long.valueOf(index) * index));
    assertThat(row.getDouble("f_double"), is(index + 0.1111));
    assertThat(row.getDecimal("f_decimal"), is(BigDecimal.valueOf(index + 0.1)));
    final KsqlArray arrayVal = row.getKsqlArray("f_array");
    assertThat(arrayVal, is(new KsqlArray().add("s" + index).add("t" + index)));
    assertThat(arrayVal.getString(0), is("s" + index));
    assertThat(arrayVal.getString(1), is("t" + index));
    final KsqlObject mapVal = row.getKsqlObject("f_map");
    assertThat(mapVal, is(new KsqlObject().put("k" + index, "v" + index)));
    assertThat(mapVal.getString("k" + index), is("v" + index));
    final KsqlObject structVal = row.getKsqlObject("f_struct");
    assertThat(structVal, is(new KsqlObject().put("F1", "v" + index).put("F2", index)));
    assertThat(structVal.getString("F1"), is("v" + index));
    assertThat(structVal.getInteger("F2"), is(index));
    assertThat(row.getValue("f_null"), is(nullValue()));

    // verify index-based getters are 1-indexed
    assertThat(row.getString(1), is(row.getString("f_str")));

    // verify isNull() evaluation
    assertThat(row.isNull("f_null"), is(true));
    assertThat(row.isNull("f_bool"), is(false));

    // verify exception on invalid cast
    assertThrows(ClassCastException.class, () -> row.getInteger("f_str"));

    // verify KsqlArray methods
    final KsqlArray values = row.values();
    assertThat(values.size(), is(DEFAULT_COLUMN_NAMES.size()));
    assertThat(values.isEmpty(), is(false));
    assertThat(values.getString(0), is(row.getString("f_str")));
    assertThat(values.getInteger(1), is(row.getInteger("f_int")));
    assertThat(values.getBoolean(2), is(row.getBoolean("f_bool")));
    assertThat(values.getLong(3), is(row.getLong("f_long")));
    assertThat(values.getDouble(4), is(row.getDouble("f_double")));
    assertThat(values.getDecimal(5), is(row.getDecimal("f_decimal")));
    assertThat(values.getKsqlArray(6), is(row.getKsqlArray("f_array")));
    assertThat(values.getKsqlObject(7), is(row.getKsqlObject("f_map")));
    assertThat(values.getKsqlObject(8), is(row.getKsqlObject("f_struct")));
    assertThat(values.getValue(9), is(nullValue()));
    assertThat(values.toJsonString(), is((new JsonArray(values.getList())).toString()));
    assertThat(values.toString(), is(values.toJsonString()));

    // verify KsqlObject methods
    final KsqlObject obj = row.asObject();
    assertThat(obj.size(), is(DEFAULT_COLUMN_NAMES.size()));
    assertThat(obj.isEmpty(), is(false));
    assertThat(obj.fieldNames(), contains(DEFAULT_COLUMN_NAMES.toArray()));
    assertThat(obj.getString("f_str"), is(row.getString("f_str")));
    assertThat(obj.getInteger("f_int"), is(row.getInteger("f_int")));
    assertThat(obj.getBoolean("f_bool"), is(row.getBoolean("f_bool")));
    assertThat(obj.getLong("f_long"), is(row.getLong("f_long")));
    assertThat(obj.getDouble("f_double"), is(row.getDouble("f_double")));
    assertThat(obj.getDecimal("f_decimal"), is(row.getDecimal("f_decimal")));
    assertThat(obj.getKsqlArray("f_array"), is(row.getKsqlArray("f_array")));
    assertThat(obj.getKsqlObject("f_map"), is(row.getKsqlObject("f_map")));
    assertThat(obj.getKsqlObject("f_struct"), is(row.getKsqlObject("f_struct")));
    assertThat(obj.getValue("f_null"), is(nullValue()));
    assertThat(obj.containsKey("f_str"), is(true));
    assertThat(obj.containsKey("f_bad"), is(false));
    assertThat(obj.toJsonString(), is((new JsonObject(obj.getMap())).toString()));
    assertThat(obj.toString(), is(obj.toJsonString()));
  }

  private static List<KsqlArray> convertToClientRows(final List<JsonArray> rows) {
    return rows.stream()
        .map(row -> new KsqlArray(row.getList()))
        .collect(Collectors.toList());
  }

  private static List<JsonObject> convertToJsonRows(final List<KsqlObject> rows) {
    return rows.stream()
        .map(row -> new JsonObject(row.getMap()))
        .collect(Collectors.toList());
  }

  private static List<KsqlObject> generateInsertRows() {
    List<KsqlObject> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      KsqlObject row = new KsqlObject()
          .put("f_str", "foo" + i)
          .put("f_int", i)
          .put("f_bool", i % 2 == 0)
          .put("f_long", i * i)
          .put("f_double", i + 0.1111)
          .put("f_decimal", new BigDecimal(i + 0.1))
          .put("f_array", new KsqlArray().add("s" + i).add("t" + i))
          .put("f_map", new KsqlObject().put("k" + i, "v" + i))
          .put("f_struct", new KsqlObject().put("F1", "v" + i).put("F2", i))
          .putNull("f_null");
      rows.add(row);
    }
    return rows;
  }
}