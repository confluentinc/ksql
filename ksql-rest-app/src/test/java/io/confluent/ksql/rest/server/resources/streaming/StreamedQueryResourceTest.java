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

import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorCode;
import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionErrorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionStatusCode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import io.confluent.ksql.version.metrics.ActivenessRegistrar;
import java.io.EOFException;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.SynchronousQueue;
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

  private static final String TOPIC_NAME = "test_stream";
  private static final String PUSH_QUERY_STRING = "SELECT * FROM " + TOPIC_NAME + " EMIT CHANGES;";
  private static final String PULL_QUERY_STRING = "SELECT * FROM " + TOPIC_NAME + " WHERE ROWKEY='null';";
  private static final String PRINT_TOPIC = "Print TEST_TOPIC;";

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
  private StreamedQueryResource testResource;
  private PreparedStatement<Statement> statement;

  @Before
  public void setup() {
    when(serviceContext.getTopicClient()).thenReturn(mockKafkaTopicClient);
    statement = PreparedStatement.of(PUSH_QUERY_STRING, mock(Statement.class));
    when(mockStatementParser.parseSingleStatement(PUSH_QUERY_STRING)).thenReturn(statement);

    final Query pullQuery = mock(Query.class);
    when(pullQuery.isPullQuery()).thenReturn(true);
    final PreparedStatement<Statement> pullQueryStatement = PreparedStatement.of(PULL_QUERY_STRING, pullQuery);
    when(mockStatementParser.parseSingleStatement(PULL_QUERY_STRING)).thenReturn(pullQueryStatement);

    testResource = new StreamedQueryResource(
        mockKsqlEngine,
        mockStatementParser,
        commandQueue,
        DISCONNECT_CHECK_INTERVAL,
        COMMAND_QUEUE_CATCHUP_TIMOEUT,
        activenessRegistrar,
        Optional.of(authorizationValidator)
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
        Optional.of(authorizationValidator)
    );

    // Then:
    expectedException.expect(KsqlRestException.class);
    expectedException.expect(exceptionStatusCode(is(Code.SERVICE_UNAVAILABLE)));
    expectedException
        .expect(exceptionErrorMessage(errorMessage(Matchers.is("Server initializing"))));

    // When:
    testResource.streamQuery(
        serviceContext,
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
        serviceContext,
        new KsqlRequest("query", Collections.emptyMap(), null)
    );
  }

  @Test
  public void shouldNotWaitIfCommandSequenceNumberSpecified() throws Exception {
    // When:
    testResource.streamQuery(
        serviceContext,
        new KsqlRequest(PUSH_QUERY_STRING, Collections.emptyMap(), null)
    );

    // Then:
    verify(commandQueue, never()).ensureConsumedPast(anyLong(), any());
  }

  @Test
  public void shouldWaitIfCommandSequenceNumberSpecified() throws Exception {
    // When:
    testResource.streamQuery(
        serviceContext,
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
        serviceContext,
        new KsqlRequest(PUSH_QUERY_STRING, Collections.emptyMap(), 3L)
    );
  }

  @Test
  public void shouldNotCreateExternalClientsForPullQuery() {
    // Given
    testResource.configure(new KsqlConfig(ImmutableMap.of(
        StreamsConfig.APPLICATION_SERVER_CONFIG, "something:1",
        KsqlConfig.KSQL_PULL_QUERIES_SKIP_ACCESS_VALIDATOR_CONFIG, true
    )));

    // When:
    testResource.streamQuery(
        serviceContext,
        new KsqlRequest(PULL_QUERY_STRING, Collections.emptyMap(), null)
    );

    // Then:
    verify(serviceContext, never()).getAdminClient();
    verify(serviceContext, never()).getConnectClient();
    verify(serviceContext, never()).getSchemaRegistryClient();
    verify(serviceContext, never()).getTopicClient();
  }

  @Test
  public void shouldThrowExceptionForPullQueryIfValidating() {
    // When:
    final Response response = testResource.streamQuery(
        serviceContext,
        new KsqlRequest(PULL_QUERY_STRING, Collections.emptyMap(), null)
    );

    // Then:
    assertThat(response.getStatus(), is(Errors.badRequest("").getStatus()));
    assertThat(response.getEntity(), is(instanceOf(KsqlErrorMessage.class)));
    final KsqlErrorMessage expectedEntity = (KsqlErrorMessage) response.getEntity();
    assertThat(
        expectedEntity.getMessage(),
        containsString(KsqlConfig.KSQL_PULL_QUERIES_SKIP_ACCESS_VALIDATOR_CONFIG)
    );
  }

  @Test
  public void shouldPassCheckForPullQueryIfNotValidating() {
    // Given
    testResource.configure(new KsqlConfig(ImmutableMap.of(
        StreamsConfig.APPLICATION_SERVER_CONFIG, "something:1",
        KsqlConfig.KSQL_PULL_QUERIES_SKIP_ACCESS_VALIDATOR_CONFIG, true
    )));

    // When:
    final Response response = testResource.streamQuery(
        serviceContext,
        new KsqlRequest(PULL_QUERY_STRING, Collections.emptyMap(), null)
    );

    // Then:
    assertThat(response.getStatus(), is(Errors.badRequest("").getStatus()));
    final KsqlErrorMessage expectedEntity = (KsqlErrorMessage) response.getEntity();
    assertThat(
        expectedEntity.getMessage(),
        not(containsString(KsqlConfig.KSQL_PULL_QUERIES_SKIP_ACCESS_VALIDATOR_CONFIG))
    );
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
          final GenericRow value = new GenericRow(Collections.singletonList(i));
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

    final Map<String, Object> requestStreamsProperties = Collections.emptyMap();

    statement = PreparedStatement.of("query", mock(Query.class));
    when(mockStatementParser.parseSingleStatement(queryString))
        .thenReturn(statement);

    final TransientQueryMetadata transientQueryMetadata =
        new TransientQueryMetadata(
            queryString,
            mockKafkaStreams,
            SOME_SCHEMA,
            Collections.emptySet(),
            limitHandler -> {},
            "",
            rowQueue,
            "",
            mock(Topology.class),
            Collections.emptyMap(),
            Collections.emptyMap(),
            queryCloseCallback);

    when(mockKsqlEngine.execute(serviceContext,
        ConfiguredStatement.of(statement, requestStreamsProperties, VALID_CONFIG)))
        .thenReturn(ExecuteResult.of(transientQueryMetadata));

    final Response response =
        testResource.streamQuery(
            serviceContext,
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
    verify(mockKafkaStreams).close();

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
        serviceContext,
        new KsqlRequest(PUSH_QUERY_STRING, Collections.emptyMap(), null)
    );

    // Then:
    verify(activenessRegistrar).updateLastRequestTime();
  }

  @Test
  public void shouldReturnForbiddenKafkaAccessIfKsqlTopicAuthorizationException() {
    // Given:
    statement = PreparedStatement.of("query", mock(Query.class));
    when(mockStatementParser.parseSingleStatement(PUSH_QUERY_STRING))
        .thenReturn(statement);

    doThrow(
        new KsqlTopicAuthorizationException(AclOperation.READ, Collections.singleton(TOPIC_NAME)))
        .when(authorizationValidator).checkAuthorization(any(), any(), any());

    // When:
    final Response response = testResource.streamQuery(
        serviceContext,
        new KsqlRequest(PUSH_QUERY_STRING, Collections.emptyMap(), null)
    );

    final Response expected = Errors.accessDeniedFromKafka(
        new KsqlTopicAuthorizationException(AclOperation.READ, Collections.singleton(TOPIC_NAME)));

    final KsqlErrorMessage responseEntity = (KsqlErrorMessage) response.getEntity();
    final KsqlErrorMessage expectedEntity = (KsqlErrorMessage) expected.getEntity();
    assertEquals(response.getStatus(), expected.getStatus());
    assertEquals(responseEntity.getMessage(), expectedEntity.getMessage());
  }

  @Test
  public void shouldReturnForbiddenKafkaAccessIfRootCauseKsqlTopicAuthorizationException() {
    // Given:
    statement = PreparedStatement.of("query", mock(Query.class));
    when(mockStatementParser.parseSingleStatement(PUSH_QUERY_STRING))
        .thenReturn(statement);
    doThrow(new KsqlException(
        "",
        new KsqlTopicAuthorizationException(AclOperation.READ, Collections.singleton(TOPIC_NAME))))
        .when(authorizationValidator).checkAuthorization(any(), any(), any());

    // When:
    final Response response = testResource.streamQuery(
        serviceContext,
        new KsqlRequest(PUSH_QUERY_STRING, Collections.emptyMap(), null)
    );

    final Response expected = Errors.accessDeniedFromKafka(
        new KsqlException(
            "",
            new KsqlTopicAuthorizationException(AclOperation.READ, Collections.singleton(TOPIC_NAME))));

    final KsqlErrorMessage responseEntity = (KsqlErrorMessage) response.getEntity();
    final KsqlErrorMessage expectedEntity = (KsqlErrorMessage) expected.getEntity();
    assertEquals(response.getStatus(), expected.getStatus());
    assertEquals(responseEntity.getMessage(), expectedEntity.getMessage());
  }

  @Test
  public void shouldReturnForbiddenKafkaAccessIfPrintTopicKsqlTopicAuthorizationException() {
    // Given:
    statement = PreparedStatement.of("print", mock(PrintTopic.class));
    when(mockStatementParser.parseSingleStatement(PRINT_TOPIC))
        .thenReturn(statement);

    doThrow(
        new KsqlTopicAuthorizationException(AclOperation.READ, Collections.singleton(TOPIC_NAME)))
        .when(authorizationValidator).checkAuthorization(any(), any(), any());

    // When:
    final Response response = testResource.streamQuery(
        serviceContext,
        new KsqlRequest(PRINT_TOPIC, Collections.emptyMap(), null)
    );

    final Response expected = Errors.accessDeniedFromKafka(
        new KsqlTopicAuthorizationException(AclOperation.READ, Collections.singleton(TOPIC_NAME)));

    assertEquals(response.getStatus(), expected.getStatus());
    assertEquals(response.getEntity(), expected.getEntity());
  }

  @Test
  public void shouldSuggestAlternativesIfPrintTopicDoesNotExist() {
    // Given:
    final PrintTopic cmd = mock(PrintTopic.class);
    when(cmd.getTopic()).thenReturn("TEST_TOPIC");
    statement = PreparedStatement.of("print", cmd);
    when(mockStatementParser.parseSingleStatement(any()))
        .thenReturn(statement);

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
        serviceContext,
        new KsqlRequest(PRINT_TOPIC, Collections.emptyMap(), null)
    );
  }
}
