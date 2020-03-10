/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources.streaming;

import static io.confluent.ksql.GenericRow.genericRow;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_FORBIDDEN_KAFKA_ACCESS;
import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorCode;
import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionErrorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionStatusCode;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.execution.streams.RoutingFilter.RoutingFilterFactory;
import io.confluent.ksql.execution.streams.RoutingFilters;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.LimitHandler;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.execution.PullQueryExecutor;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.io.EOFException;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.codehaus.plexus.util.StringUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpStatus.Code;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StreamedQueryResourceTest {

  private static final Duration DISCONNECT_CHECK_INTERVAL = Duration.ofMillis(1000);
  private static final Duration COMMAND_QUEUE_CATCHUP_TIMOEUT = Duration.ofMillis(1000);
  private static final LogicalSchema SOME_SCHEMA = LogicalSchema.builder()
      .noImplicitColumns()
      .valueColumn(ColumnName.of("f1"), SqlTypes.INTEGER)
      .build();

  private static final KsqlConfig VALID_CONFIG = new KsqlConfig(ImmutableMap.of(
      StreamsConfig.APPLICATION_SERVER_CONFIG, "something:1"
  ));
  private static final Long closeTimeout = KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_DEFAULT;
  
  private static final Response AUTHORIZATION_ERROR_RESPONSE = Response
      .status(FORBIDDEN)
      .entity(new KsqlErrorMessage(ERROR_CODE_FORBIDDEN_KAFKA_ACCESS, "some error"))
      .build();

  private static final String TOPIC_NAME = "test_stream";
  private static final String PUSH_QUERY_STRING = "SELECT * FROM " + TOPIC_NAME + " EMIT CHANGES;";
  private static final String PULL_QUERY_STRING = "SELECT * FROM " + TOPIC_NAME + " WHERE ROWKEY='null';";
  private static final String PRINT_TOPIC = "Print TEST_TOPIC;";

  private static final RoutingFilterFactory ROUTING_FILTER_FACTORY =
      (routingOptions, hosts, active, applicationQueryId, storeName, partition) ->
          new RoutingFilters(ImmutableList.of());

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KsqlEngine mockKsqlEngine;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KafkaTopicClient mockKafkaTopicClient;
  @Mock
  private StatementParser mockStatementParser;
  @Mock
  private CommandQueue commandQueue;
  @Mock
  private ActivenessRegistrar activenessRegistrar;
  @Mock
  private Consumer<QueryMetadata> queryCloseCallback;
  @Mock
  private KsqlAuthorizationValidator authorizationValidator;
  @Mock
  private Errors errorsHandler;

  private StreamedQueryResource testResource;
  private PreparedStatement<Statement> invalid;
  private PreparedStatement<Query> query;
  private PreparedStatement<PrintTopic> print;
  private KsqlSecurityContext securityContext;
  private PullQueryExecutor pullQueryExecutor;

  @Before
  public void setup() {
    when(serviceContext.getTopicClient()).thenReturn(mockKafkaTopicClient);
    query = PreparedStatement.of(PUSH_QUERY_STRING, mock(Query.class));
    invalid = PreparedStatement.of("sql", mock(Statement.class));
    when(mockStatementParser.parseSingleStatement(PUSH_QUERY_STRING)).thenReturn(invalid);

    final Query pullQuery = mock(Query.class);
    when(pullQuery.isPullQuery()).thenReturn(true);
    final PreparedStatement<Statement> pullQueryStatement = PreparedStatement.of(PULL_QUERY_STRING, pullQuery);
    when(mockStatementParser.parseSingleStatement(PULL_QUERY_STRING)).thenReturn(pullQueryStatement);
    when(errorsHandler.accessDeniedFromKafkaResponse(any(Exception.class))).thenReturn(AUTHORIZATION_ERROR_RESPONSE);

    securityContext = new KsqlSecurityContext(Optional.empty(), serviceContext);

    pullQueryExecutor = new PullQueryExecutor(
        mockKsqlEngine, Optional.empty(), ROUTING_FILTER_FACTORY);
    testResource = new StreamedQueryResource(
        mockKsqlEngine,
        mockStatementParser,
        commandQueue,
        DISCONNECT_CHECK_INTERVAL,
        COMMAND_QUEUE_CATCHUP_TIMOEUT,
        activenessRegistrar,
        Optional.of(authorizationValidator),
        errorsHandler,
        pullQueryExecutor
    );

    testResource.configure(VALID_CONFIG);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnConfigureIfAppServerNotSet() {
    // Given:
    final KsqlConfig configNoAppServer = new KsqlConfig(ImmutableMap.of());

    // When:
    testResource.configure(configNoAppServer);
  }

  @Test
  public void shouldThrowOnHandleStatementIfNotConfigured() {
    // Given:
    testResource = new StreamedQueryResource(
        mockKsqlEngine,
        mockStatementParser,
        commandQueue,
        DISCONNECT_CHECK_INTERVAL,
        COMMAND_QUEUE_CATCHUP_TIMOEUT,
        activenessRegistrar,
        Optional.of(authorizationValidator),
        errorsHandler,
        pullQueryExecutor
    );

    // Then:
    expectedException.expect(KsqlRestException.class);
    expectedException.expect(exceptionStatusCode(is(Code.SERVICE_UNAVAILABLE)));
    expectedException
        .expect(exceptionErrorMessage(errorMessage(Matchers.is("Server initializing"))));

    // When:
    testResource.streamQuery(
        securityContext,
        new KsqlRequest("query", Collections.emptyMap(), null)
    );
  }

  @Test
  public void shouldReturn400OnBadStatement() {
    // Given:
    when(mockStatementParser.parseSingleStatement(any()))
        .thenThrow(new IllegalArgumentException("some error message"));

    // Expect
    expectedException.expect(KsqlRestException.class);
    expectedException.expect(exceptionStatusCode(is(Code.BAD_REQUEST)));
    expectedException.expect(exceptionErrorMessage(errorMessage(is("some error message"))));
    expectedException.expect(
        exceptionErrorMessage(errorCode(is(Errors.ERROR_CODE_BAD_STATEMENT))));

    // When:
    testResource.streamQuery(
        securityContext,
        new KsqlRequest("query", Collections.emptyMap(), null)
    );
  }

  @Test
  public void shouldNotWaitIfCommandSequenceNumberSpecified() throws Exception {
    // When:
    testResource.streamQuery(
        securityContext,
        new KsqlRequest(PUSH_QUERY_STRING, Collections.emptyMap(), null)
    );

    // Then:
    verify(commandQueue, never()).ensureConsumedPast(anyLong(), any());
  }

  @Test
  public void shouldWaitIfCommandSequenceNumberSpecified() throws Exception {
    // When:
    testResource.streamQuery(
        securityContext,
        new KsqlRequest(PUSH_QUERY_STRING, Collections.emptyMap(), 3L)
    );

    // Then:
    verify(commandQueue).ensureConsumedPast(eq(3L), any());
  }

  @Test
  public void shouldReturnServiceUnavailableIfTimeoutWaitingForCommandSequenceNumber()
      throws Exception {
    // Given:
    doThrow(new TimeoutException("whoops"))
        .when(commandQueue).ensureConsumedPast(anyLong(), any());

    // Expect
    expectedException.expect(KsqlRestException.class);
    expectedException.expect(exceptionStatusCode(is(Code.SERVICE_UNAVAILABLE)));
    expectedException.expect(exceptionErrorMessage(errorMessage(
        containsString("Timed out while waiting for a previous command to execute"))));
    expectedException.expect(
        exceptionErrorMessage(errorCode(is(Errors.ERROR_CODE_COMMAND_QUEUE_CATCHUP_TIMEOUT))));

    // When:
    testResource.streamQuery(
        securityContext,
        new KsqlRequest(PUSH_QUERY_STRING, Collections.emptyMap(), 3L)
    );
  }

  @Test
  public void shouldNotCreateExternalClientsForPullQuery() {
    // Given
    testResource.configure(new KsqlConfig(ImmutableMap.of(
        StreamsConfig.APPLICATION_SERVER_CONFIG, "something:1"
    )));

    // When:
    testResource.streamQuery(
        securityContext,
        new KsqlRequest(PULL_QUERY_STRING, Collections.emptyMap(), null)
    );

    // Then:
    verify(serviceContext, never()).getAdminClient();
    verify(serviceContext, never()).getConnectClient();
    verify(serviceContext, never()).getSchemaRegistryClient();
    verify(serviceContext, never()).getTopicClient();
  }

  @Test
  public void shouldReturnForbiddenKafkaAccessForPullQueryAuthorizationDenied() {
    // Given:
    when(mockStatementParser.<Query>parseSingleStatement(PULL_QUERY_STRING))
        .thenReturn(query);
    doThrow(
        new KsqlTopicAuthorizationException(AclOperation.READ, Collections.singleton(TOPIC_NAME)))
        .when(authorizationValidator).checkAuthorization(any(), any(), any());

    // When:
    final Response response = testResource.streamQuery(
        securityContext,
        new KsqlRequest(PULL_QUERY_STRING, Collections.emptyMap(), null)
    );

    final KsqlErrorMessage responseEntity = (KsqlErrorMessage) response.getEntity();
    final KsqlErrorMessage expectedEntity = (KsqlErrorMessage) AUTHORIZATION_ERROR_RESPONSE.getEntity();
    assertEquals(response.getStatus(), AUTHORIZATION_ERROR_RESPONSE.getStatus());
    assertEquals(responseEntity.getMessage(), expectedEntity.getMessage());
  }

  @Test
  public void shouldStreamRowsCorrectly() throws Throwable {
    final int NUM_ROWS = 5;
    final AtomicReference<Throwable> threadException = new AtomicReference<>(null);
    final Thread.UncaughtExceptionHandler threadExceptionHandler =
        (thread, exception) -> threadException.compareAndSet(null, exception);

    final String queryString = "SELECT * FROM test_stream;";

    final SynchronousQueue<KeyValue<String, GenericRow>> rowQueue = new SynchronousQueue<>();

    final LinkedList<GenericRow> writtenRows = new LinkedList<>();

    final Thread rowQueuePopulatorThread = new Thread(() -> {
      try {
        for (int i = 0; i != NUM_ROWS; i++) {
          final String key = Integer.toString(i);
          final GenericRow value = genericRow(i);
          synchronized (writtenRows) {
            writtenRows.add(value);
          }
          rowQueue.put(new KeyValue<>(key, value));
        }
      } catch (final InterruptedException exception) {
        // This should happen during the test, so it's fine
      }
    }, "Row Queue Populator");
    rowQueuePopulatorThread.setUncaughtExceptionHandler(threadExceptionHandler);
    rowQueuePopulatorThread.start();

    final KafkaStreams mockKafkaStreams = mock(KafkaStreams.class);

    when(mockStatementParser.<Query>parseSingleStatement(queryString))
        .thenReturn(query);

    final Map<String, Object> requestStreamsProperties = Collections.emptyMap();

    final TransientQueryMetadata transientQueryMetadata =
        new TransientQueryMetadata(
            queryString,
            mockKafkaStreams,
            SOME_SCHEMA,
            Collections.emptySet(),
            "",
            new TestRowQueue(rowQueue),
            "",
            mock(Topology.class),
            Collections.emptyMap(),
            Collections.emptyMap(),
            queryCloseCallback,
            closeTimeout);

    when(mockKsqlEngine.executeQuery(serviceContext,
        ConfiguredStatement.of(query, requestStreamsProperties, VALID_CONFIG)))
        .thenReturn(transientQueryMetadata);

    final Response response =
        testResource.streamQuery(
            securityContext,
            new KsqlRequest(queryString, requestStreamsProperties, null)
        );
    final PipedOutputStream responseOutputStream = new EOFPipedOutputStream();
    final PipedInputStream responseInputStream = new PipedInputStream(responseOutputStream, 1);
    final StreamingOutput responseStream = (StreamingOutput) response.getEntity();

    final Thread queryWriterThread = new Thread(() -> {
      try {
        responseStream.write(responseOutputStream);
      } catch (final EOFException exception) {
        // It's fine
      } catch (final IOException exception) {
        throw new RuntimeException(exception);
      }
    }, "Query Writer");
    queryWriterThread.setUncaughtExceptionHandler(threadExceptionHandler);
    queryWriterThread.start();

    final Scanner responseScanner = new Scanner(responseInputStream, "UTF-8");
    final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;
    for (int i = 0; i != NUM_ROWS; i++) {
      if (!responseScanner.hasNextLine()) {
        throw new Exception("Response input stream failed to have expected line available");
      }
      final String responseLine = responseScanner.nextLine();

      String jsonLine = StringUtils.stripStart(responseLine, "[");
      jsonLine = StringUtils.stripEnd(jsonLine, ",");
      jsonLine = StringUtils.stripEnd(jsonLine, "]");

      if (jsonLine.isEmpty()) {
        i--;
        continue;
      }

      if (i == 0) {
        // Header:
        assertThat(jsonLine, is("{\"header\":{\"queryId\":\"none\",\"schema\":\"`f1` INTEGER\"}}"));
        continue;
      }

      final GenericRow expectedRow;
      synchronized (writtenRows) {
        expectedRow = writtenRows.poll();
      }

      final GenericRow testRow = objectMapper
          .readValue(jsonLine, StreamedRow.class)
          .getRow()
          .get();

      assertEquals(expectedRow, testRow);
    }

    responseOutputStream.close();

    queryWriterThread.join();
    rowQueuePopulatorThread.interrupt();
    rowQueuePopulatorThread.join();

    // Definitely want to make sure that the Kafka Streams instance has been closed and cleaned up
    verify(mockKafkaStreams).start();
    verify(mockKafkaStreams).setUncaughtExceptionHandler(any());
    verify(mockKafkaStreams).cleanUp();
    verify(mockKafkaStreams).close(Duration.ofMillis(closeTimeout));

    // If one of the other threads has somehow managed to throw an exception without breaking things up until this
    // point, we throw that exception now in the main thread and cause the test to fail
    final Throwable exception = threadException.get();
    if (exception != null) {
      throw exception;
    }
  }

  // Have to mimic the behavior of the OutputStream that's usually passed to the QueryStreamWriter class's write()
  // method, which is to throw an EOFException if any write attempts are made after the connection has terminated
  private static class EOFPipedOutputStream extends PipedOutputStream {

    private boolean closed;

    private EOFPipedOutputStream() {
      super();
      closed = false;
    }

    private void throwIfClosed() throws IOException {
      if (closed) {
        throw new EOFException();
      }
    }

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }

    @Override
    public void flush() throws IOException {
      throwIfClosed();
      try {
        super.flush();
      } catch (final IOException exception) {
        // Might have been closed during the call to super.flush();
        throwIfClosed();
        throw exception;
      }
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
      throwIfClosed();
      try {
        super.write(b, off, len);
      } catch (final IOException exception) {
        // Might have been closed during the call to super.write();
        throwIfClosed();
        throw exception;
      }
    }

    @Override
    public void write(final int b) throws IOException {
      throwIfClosed();
      try {
        super.write(b);
      } catch (final IOException exception) {
        // Might have been closed during the call to super.write();
        throwIfClosed();
        throw exception;
      }
    }
  }

  @Test
  public void shouldUpdateTheLastRequestTime() {
    /// When:
    testResource.streamQuery(
        securityContext,
        new KsqlRequest(PUSH_QUERY_STRING, Collections.emptyMap(), null)
    );

    // Then:
    verify(activenessRegistrar).updateLastRequestTime();
  }

  @Test
  public void shouldReturnForbiddenKafkaAccessIfKsqlTopicAuthorizationException() {
    // Given:
    when(mockStatementParser.<Query>parseSingleStatement(PUSH_QUERY_STRING))
        .thenReturn(query);
    doThrow(
        new KsqlTopicAuthorizationException(AclOperation.READ, Collections.singleton(TOPIC_NAME)))
        .when(authorizationValidator).checkAuthorization(any(), any(), any());

    // When:
    final Response response = testResource.streamQuery(
        securityContext,
        new KsqlRequest(PUSH_QUERY_STRING, Collections.emptyMap(), null)
    );

    final KsqlErrorMessage responseEntity = (KsqlErrorMessage) response.getEntity();
    final KsqlErrorMessage expectedEntity = (KsqlErrorMessage) AUTHORIZATION_ERROR_RESPONSE.getEntity();
    assertEquals(response.getStatus(), AUTHORIZATION_ERROR_RESPONSE.getStatus());
    assertEquals(responseEntity.getMessage(), expectedEntity.getMessage());
  }

  @Test
  public void shouldReturnForbiddenKafkaAccessIfPrintTopicKsqlTopicAuthorizationException() {
    // Given:
    print = PreparedStatement.of("print", mock(PrintTopic.class));
    when(mockStatementParser.<PrintTopic>parseSingleStatement(PRINT_TOPIC))
        .thenReturn(print);

    doThrow(
        new KsqlTopicAuthorizationException(AclOperation.READ, Collections.singleton(TOPIC_NAME)))
        .when(authorizationValidator).checkAuthorization(any(), any(), any());

    // When:
    final Response response = testResource.streamQuery(
        securityContext,
        new KsqlRequest(PRINT_TOPIC, Collections.emptyMap(), null)
    );

    assertEquals(response.getStatus(), AUTHORIZATION_ERROR_RESPONSE.getStatus());
    assertEquals(response.getEntity(), AUTHORIZATION_ERROR_RESPONSE.getEntity());
  }

  @Test
  public void shouldSuggestAlternativesIfPrintTopicDoesNotExist() {
    // Given:
    final PrintTopic cmd = mock(PrintTopic.class);
    when(cmd.getTopic()).thenReturn("TEST_TOPIC");
    print = PreparedStatement.of("print", cmd);
    when(mockStatementParser.<PrintTopic>parseSingleStatement(any()))
        .thenReturn(print);

    when(mockKafkaTopicClient.isTopicExists(any())).thenReturn(false);
    when(mockKafkaTopicClient.listTopicNames()).thenReturn(ImmutableSet.of(
        "aTopic",
        "test_topic",
        "Test_Topic"
    ));

    // Then:
    expectedException.expect(KsqlRestException.class);
    expectedException.expect(exceptionStatusCode(is(HttpStatus.Code.BAD_REQUEST)));
    expectedException.expect(exceptionErrorMessage(
        errorMessage(containsString(
            "Could not find topic 'TEST_TOPIC', "
                + "or the KSQL user does not have permissions to list the topic. "
                + "Topic names are case-sensitive."
                + System.lineSeparator()
                + "Did you mean:"
        ))));

    expectedException.expect(exceptionErrorMessage(
        errorMessage(containsString("\tprint test_topic;"
        ))));

    expectedException.expect(exceptionErrorMessage(
        errorMessage(containsString("\tprint Test_Topic;"
        ))));

    // When:
    testResource.streamQuery(
        securityContext,
        new KsqlRequest(PRINT_TOPIC, Collections.emptyMap(), null)
    );
  }

  private static class TestRowQueue implements BlockingRowQueue {

    private final SynchronousQueue<KeyValue<String, GenericRow>> rowQueue;

    TestRowQueue(
        final SynchronousQueue<KeyValue<String, GenericRow>> rowQueue
    ) {
      this.rowQueue = Objects.requireNonNull(rowQueue, "rowQueue");
    }

    @Override
    public void setLimitHandler(final LimitHandler limitHandler) {

    }

    @Override
    public KeyValue<String, GenericRow> poll(final long timeout, final TimeUnit unit)
        throws InterruptedException {
      return rowQueue.poll(timeout, unit);
    }

    @Override
    public void drainTo(final Collection<? super KeyValue<String, GenericRow>> collection) {
      rowQueue.drainTo(collection);
    }

    @Override
    public int size() {
      return rowQueue.size();
    }

    @Override
    public void close() {

    }
  }
}
