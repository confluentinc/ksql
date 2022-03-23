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
import static io.confluent.ksql.rest.Errors.ERROR_CODE_BAD_STATEMENT;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_FORBIDDEN_KAFKA_ACCESS;
import static io.confluent.ksql.rest.Errors.badRequest;
import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorCode;
import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorMessage;
import static io.confluent.ksql.rest.entity.KsqlStatementErrorMessageMatchers.statement;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionErrorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionStatementErrorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionStatusCode;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.server.MetricsCallbackHolder;
import io.confluent.ksql.api.server.StreamingOutput;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.execution.pull.PullQueryResult;
import io.confluent.ksql.properties.DenyListPropertyValidator;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.CompletionHandler;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.LimitHandler;
import io.confluent.ksql.query.PullQueryQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.StreamedRow.DataRow;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.query.QueryExecutor;
import io.confluent.ksql.rest.server.query.QueryMetadataHolder;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.rest.server.validation.CustomValidators;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KeyValueMetadata;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PushQueryMetadata.ResultType;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import io.vertx.core.Context;
import java.io.EOFException;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.codehaus.plexus.util.StringUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("deprecation") // https://github.com/confluentinc/ksql/issues/6639
public class StreamedQueryResourceTest {

  private static final Duration DISCONNECT_CHECK_INTERVAL = Duration.ofMillis(1000);
  private static final Duration COMMAND_QUEUE_CATCHUP_TIMOEUT = Duration.ofMillis(1000);

  private static final LogicalSchema SOME_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("f1"), SqlTypes.INTEGER)
      .build();

  private static final KsqlConfig VALID_CONFIG = new KsqlConfig(ImmutableMap.of(
      StreamsConfig.APPLICATION_SERVER_CONFIG, "something:1",
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "anything:2",
      KsqlConfig.KSQL_QUERY_STREAM_PULL_QUERY_ENABLED, true,
      KsqlConfig.KSQL_ENDPOINT_MIGRATE_QUERY_CONFIG, false
  ));
  private static final Long closeTimeout = KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_DEFAULT;

  private static final EndpointResponse AUTHORIZATION_ERROR_RESPONSE = EndpointResponse.create()
      .status(FORBIDDEN.code())
      .entity(new KsqlErrorMessage(ERROR_CODE_FORBIDDEN_KAFKA_ACCESS, "some error"))
      .build();

  private static final String TOPIC_NAME = "test_stream";
  private static final String PUSH_QUERY_STRING = "SELECT * FROM " + TOPIC_NAME + " EMIT CHANGES;";
  private static final String PULL_QUERY_STRING =
      "SELECT * FROM " + TOPIC_NAME + " WHERE ROWKEY='null';";
  private static final String PRINT_TOPIC = "Print TEST_TOPIC;";

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
  private KsqlAuthorizationValidator authorizationValidator;
  @Mock
  private Errors errorsHandler;
  @Mock
  private DenyListPropertyValidator denyListPropertyValidator;
  @Mock
  private QueryId queryId;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private KsqlRestConfig ksqlRestConfig;
  @Mock
  private PullQueryResult pullQueryResult;
  @Mock
  private PullQueryQueue pullQueryQueue;
  @Captor
  private ArgumentCaptor<Exception> exception;
  @Mock
  private QueryMetadata.Listener listener;
  @Mock
  private Context context;
  @Mock
  private QueryExecutor queryExecutor;
  @Mock
  private QueryMetadataHolder queryMetadataHolder;

  private StreamedQueryResource testResource;
  private PreparedStatement<Statement> invalid;
  private PreparedStatement<Query> query;
  private PreparedStatement<PrintTopic> print;
  private KsqlSecurityContext securityContext;

  @Before
  public void setup() {
    when(serviceContext.getTopicClient()).thenReturn(mockKafkaTopicClient);
    query = PreparedStatement.of(PUSH_QUERY_STRING, mock(Query.class));
    invalid = PreparedStatement.of("sql", mock(Statement.class));
    when(mockStatementParser.parseSingleStatement(PUSH_QUERY_STRING)).thenReturn(invalid);

    final PreparedStatement<Statement> pullQueryStatement = PreparedStatement.of(PULL_QUERY_STRING,  mock(Query.class));
    when(mockStatementParser.parseSingleStatement(PULL_QUERY_STRING)).thenReturn(pullQueryStatement);
    when(errorsHandler.accessDeniedFromKafkaResponse(any(Exception.class))).thenReturn(AUTHORIZATION_ERROR_RESPONSE);
    when(errorsHandler.generateResponse(exception.capture(), any()))
        .thenReturn(EndpointResponse.failed(500));
    when(queryExecutor.handleStatement(any(), any(), any(), any(), any(), any(), any(),
        anyBoolean()))
        .thenReturn(queryMetadataHolder);

    securityContext = new KsqlSecurityContext(Optional.empty(), serviceContext);

    testResource =  new StreamedQueryResource(
        mockKsqlEngine,
        ksqlRestConfig,
        mockStatementParser,
        commandQueue,
        DISCONNECT_CHECK_INTERVAL,
        COMMAND_QUEUE_CATCHUP_TIMOEUT,
        activenessRegistrar,
        Optional.of(authorizationValidator),
        errorsHandler,
        denyListPropertyValidator,
        queryExecutor
    );

    testResource.configure(VALID_CONFIG);
  }

  @Test
  public void shouldRedirectQueriesToQueryEndPoint() {
    // Given:
    final ConfiguredStatement<Query> query = ConfiguredStatement
        .of(PreparedStatement.of("SELECT * FROM test_table;", mock(Query.class)),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> CustomValidators.QUERY_ENDPOINT.validate(
            query,
            mock(SessionProperties.class),
            mockKsqlEngine,
            serviceContext
        )
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionStatementErrorMessage(errorMessage(containsString(
        "The following statement types should be issued to the websocket endpoint '/query'"
    ))));
    assertThat(e, exceptionStatementErrorMessage(statement(containsString(
        "SELECT * FROM test_table;"))));
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
        ksqlRestConfig,
        mockStatementParser,
        commandQueue,
        DISCONNECT_CHECK_INTERVAL,
        COMMAND_QUEUE_CATCHUP_TIMOEUT,
        activenessRegistrar,
        Optional.of(authorizationValidator),
        errorsHandler,
        denyListPropertyValidator,
        queryExecutor
    );

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> testResource.streamQuery(
            securityContext,
            new KsqlRequest("query", Collections.emptyMap(), Collections.emptyMap(), null),
            new CompletableFuture<>(),
            Optional.empty(),
            new MetricsCallbackHolder(),
            context
        )
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(SERVICE_UNAVAILABLE.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(Matchers.is("Server initializing"))));
  }

  @Test
  public void shouldReturn400OnBadStatement() {
    // Given:
    when(mockStatementParser.parseSingleStatement(any()))
        .thenThrow(new IllegalArgumentException("some error message"));

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> testResource.streamQuery(
            securityContext,
            new KsqlRequest("query", Collections.emptyMap(), Collections.emptyMap(), null),
            new CompletableFuture<>(),
            Optional.empty(),
            new MetricsCallbackHolder(),
            context
        )
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(is("some error message"))));
    assertThat(e, exceptionErrorMessage(errorCode(is(ERROR_CODE_BAD_STATEMENT))));
  }

  @Test
  public void shouldNotWaitIfCommandSequenceNumberSpecified() throws Exception {
    // When:
    testResource.streamQuery(
        securityContext,
        new KsqlRequest(PUSH_QUERY_STRING, Collections.emptyMap(), Collections.emptyMap(), null),
        new CompletableFuture<>(),
        Optional.empty(),
        new MetricsCallbackHolder(),
        context
    );

    // Then:
    verify(commandQueue, never()).ensureConsumedPast(anyLong(), any());
  }

  @Test
  public void shouldWaitIfCommandSequenceNumberSpecified() throws Exception {
    // When:
    testResource.streamQuery(
        securityContext,
        new KsqlRequest(PUSH_QUERY_STRING, Collections.emptyMap(), Collections.emptyMap(), 3L),
        new CompletableFuture<>(),
        Optional.empty(),
        new MetricsCallbackHolder(),
        context
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

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> testResource.streamQuery(
            securityContext,
            new KsqlRequest(PUSH_QUERY_STRING, Collections.emptyMap(), Collections.emptyMap(), 3L),
            new CompletableFuture<>(),
            Optional.empty(),
            new MetricsCallbackHolder(),
            context
        )
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(SERVICE_UNAVAILABLE.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(
        containsString("Timed out while waiting for a previous command to execute"))));
    assertThat(e, exceptionErrorMessage(errorCode(is(Errors.ERROR_CODE_COMMAND_QUEUE_CATCHUP_TIMEOUT))));
  }

  @Test
  public void shouldNotCreateExternalClientsForPullQuery() {
    // Given:
    testResource.configure(new KsqlConfig(ImmutableMap.of(
        StreamsConfig.APPLICATION_SERVER_CONFIG, "something:1"
    )));

    // When:
    testResource.streamQuery(
        securityContext,
        new KsqlRequest(PULL_QUERY_STRING, Collections.emptyMap(), Collections.emptyMap(), null),
        new CompletableFuture<>(),
        Optional.empty(),
        new MetricsCallbackHolder(),
        context
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
    final EndpointResponse response = testResource.streamQuery(
        securityContext,
        new KsqlRequest(PULL_QUERY_STRING, Collections.emptyMap(), Collections.emptyMap(), null),
        new CompletableFuture<>(),
        Optional.empty(),
        new MetricsCallbackHolder(),
        context
    );

    final KsqlErrorMessage responseEntity = (KsqlErrorMessage) response.getEntity();
    final KsqlErrorMessage expectedEntity = (KsqlErrorMessage) AUTHORIZATION_ERROR_RESPONSE.getEntity();
    assertEquals(response.getStatus(), AUTHORIZATION_ERROR_RESPONSE.getStatus());
    assertEquals(responseEntity.getMessage(), expectedEntity.getMessage());
  }

  @Test
  public void shouldThrowOnDenyListedStreamProperty() {
    // Given:
    when(mockStatementParser.<Query>parseSingleStatement(PULL_QUERY_STRING)).thenReturn(query);
    testResource = new StreamedQueryResource(
        mockKsqlEngine,
        ksqlRestConfig,
        mockStatementParser,
        commandQueue,
        DISCONNECT_CHECK_INTERVAL,
        COMMAND_QUEUE_CATCHUP_TIMOEUT,
        activenessRegistrar,
        Optional.of(authorizationValidator),
        errorsHandler,
        denyListPropertyValidator,
        queryExecutor
      );
    final Map<String, Object> props = new HashMap<>(ImmutableMap.of(
        StreamsConfig.APPLICATION_SERVER_CONFIG, "something:1"
    ));
    props.put(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_DENYLIST,
        StreamsConfig.NUM_STREAM_THREADS_CONFIG);
    testResource.configure(new KsqlConfig(props));
    final Map<String, Object> overrides =
        ImmutableMap.of(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
    doThrow(new KsqlException("deny override")).when(denyListPropertyValidator)
        .validateAll(overrides);
    when(errorsHandler.generateResponse(any(), any()))
        .thenReturn(badRequest("A property override was set locally for a property that the "
            + "server prohibits overrides for: 'num.stream.threads'"));

    // When:
    final EndpointResponse response = testResource.streamQuery(
        securityContext,
        new KsqlRequest(
            PULL_QUERY_STRING,
            overrides, // stream properties
            Collections.emptyMap(),
            null
        ),
        new CompletableFuture<>(),
        Optional.empty(),
        new MetricsCallbackHolder(),
        context
    );

    // Then:
    verify(denyListPropertyValidator).validateAll(overrides);
    assertThat(response.getStatus(), CoreMatchers.is(BAD_REQUEST.code()));
    assertThat(((KsqlErrorMessage) response.getEntity()).getMessage(),
        is("A property override was set locally for a property that the server prohibits "
            + "overrides for: '" + StreamsConfig.NUM_STREAM_THREADS_CONFIG + "'"));
  }

  @Test
  public void shouldStreamRowsCorrectly() throws Throwable {
    final int NUM_ROWS = 5;
    final AtomicReference<Throwable> threadException = new AtomicReference<>(null);
    final Thread.UncaughtExceptionHandler threadExceptionHandler =
        (thread, exception) -> threadException.compareAndSet(null, exception);

    final String queryString = "SELECT * FROM test_stream;";

    final SynchronousQueue<KeyValueMetadata<List<?>, GenericRow>> rowQueue
        = new SynchronousQueue<>();

    final LinkedList<GenericRow> writtenRows = new LinkedList<>();

    final Thread rowQueuePopulatorThread = new Thread(() -> {
      try {
        for (int i = 0; i != NUM_ROWS; i++) {
          final GenericRow value = genericRow(i);
          synchronized (writtenRows) {
            writtenRows.add(value);
          }
          rowQueue.put(new KeyValueMetadata<>(KeyValue.keyValue(null, value)));
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
    final KafkaStreamsBuilder kafkaStreamsBuilder = mock(KafkaStreamsBuilder.class);
    when(kafkaStreamsBuilder.build(any(), any())).thenReturn(mockKafkaStreams);
    MutableBoolean closed = new MutableBoolean(false);
    when(mockKafkaStreams.close(any())).thenAnswer(i -> {
      closed.setValue(true);
      return true;
    });
    when(mockKafkaStreams.state()).thenAnswer(i ->
        closed.getValue() ? State.NOT_RUNNING : State.RUNNING);

    final TransientQueryMetadata transientQueryMetadata =
        new TransientQueryMetadata(
            queryString,
            SOME_SCHEMA,
            Collections.emptySet(),
            "",
            new TestRowQueue(rowQueue),
            queryId,
            "appId",
            mock(Topology.class),
            kafkaStreamsBuilder,
            Collections.emptyMap(),
            Collections.emptyMap(),
            closeTimeout,
            10,
            ResultType.STREAM,
            0L,
            0L,
            listener
        );
    transientQueryMetadata.initialize();

    when(queryMetadataHolder.getPushQueryMetadata())
        .thenReturn(Optional.of(transientQueryMetadata));

    final EndpointResponse response =
        testResource.streamQuery(
            securityContext,
            new KsqlRequest(queryString, requestStreamsProperties, Collections.emptyMap(), null),
            new CompletableFuture<>(),
            Optional.empty(),
            new MetricsCallbackHolder(),
            context
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
    final ObjectMapper objectMapper = ApiJsonMapper.INSTANCE.get();
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
        assertThat(jsonLine,
            is("{\"header\":{\"queryId\":\"queryId\",\"schema\":\"`f1` INTEGER\"}}"));
        continue;
      }

      final GenericRow expectedRow;
      synchronized (writtenRows) {
        expectedRow = writtenRows.poll();
      }

      final DataRow testRow = objectMapper
          .readValue(jsonLine, StreamedRow.class)
          .getRow()
          .get();

      assertThat(testRow.getColumns(), is(expectedRow.values()));
    }

    responseOutputStream.close();

    queryWriterThread.join();
    rowQueuePopulatorThread.interrupt();
    rowQueuePopulatorThread.join();

    // Definitely want to make sure that the Kafka Streams instance has been closed and cleaned up
    verify(mockKafkaStreams).start();
    // called on init and when setting uncaught exception handler manually
    verify(mockKafkaStreams, times(2)).setUncaughtExceptionHandler(any(StreamsUncaughtExceptionHandler.class));
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
        new KsqlRequest(PUSH_QUERY_STRING, Collections.emptyMap(), Collections.emptyMap(), null),
        new CompletableFuture<>(),
        Optional.empty(),
        new MetricsCallbackHolder(),
        context
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
    final EndpointResponse response = testResource.streamQuery(
        securityContext,
        new KsqlRequest(PUSH_QUERY_STRING, Collections.emptyMap(), Collections.emptyMap(), null),
        new CompletableFuture<>(),
        Optional.empty(),
        new MetricsCallbackHolder(),
        context
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
    final EndpointResponse response = testResource.streamQuery(
        securityContext,
        new KsqlRequest(PRINT_TOPIC, Collections.emptyMap(), Collections.emptyMap(), null),
        new CompletableFuture<>(),
        Optional.empty(),
        new MetricsCallbackHolder(),
        context
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

    // When:
    final KsqlRestException e = assertThrows(
        KsqlRestException.class,
        () -> testResource.streamQuery(
            securityContext,
            new KsqlRequest(PRINT_TOPIC, Collections.emptyMap(), Collections.emptyMap(), null),
            new CompletableFuture<>(),
            Optional.empty(),
            new MetricsCallbackHolder(),
            context
        )
    );

    // Then:
    assertThat(e, exceptionStatusCode(is(BAD_REQUEST.code())));
    assertThat(e, exceptionErrorMessage(errorMessage(containsString(
        "Could not find topic 'TEST_TOPIC', "
            + "or the KSQL user does not have permissions to list the topic. "
            + "Topic names are case-sensitive."
            + System.lineSeparator()
            + "Did you mean:"
    ))));
    assertThat(e, exceptionErrorMessage(errorMessage(containsString("\tprint test_topic;"
    ))));
    assertThat(e, exceptionErrorMessage(errorMessage(containsString("\tprint Test_Topic;"
    ))));
  }

  private static class TestRowQueue implements BlockingRowQueue {

    private final SynchronousQueue<KeyValueMetadata<List<?>, GenericRow>> rowQueue;

    TestRowQueue(
        final SynchronousQueue<KeyValueMetadata<List<?>, GenericRow>> rowQueue
    ) {
      this.rowQueue = Objects.requireNonNull(rowQueue, "rowQueue");
    }

    @Override
    public void setLimitHandler(final LimitHandler limitHandler) {

    }

    @Override
    public void setCompletionHandler(final CompletionHandler completionHandler) {

    }

    @Override
    public void setQueuedCallback(final Runnable callback) {

    }

    @Override
    public KeyValueMetadata<List<?>, GenericRow> poll(final long timeout, final TimeUnit unit)
        throws InterruptedException {
      return rowQueue.poll(timeout, unit);
    }

    @Override
    public KeyValueMetadata<List<?>, GenericRow> poll() {
      return rowQueue.poll();
    }

    @Override
    public void drainTo(final Collection<? super KeyValueMetadata<List<?>, GenericRow>> collection) {
      rowQueue.drainTo(collection);
    }

    @Override
    public int size() {
      return rowQueue.size();
    }

    @Override
    public boolean isEmpty() {
      return rowQueue.isEmpty();
    }

    @Override
    public void close() {

    }
  }
}
