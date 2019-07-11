/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources;

import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorCode;
import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionErrorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionStatusCode;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.json.JsonMapper;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.planner.PlanSourceExtractorVisitor;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.schema.ksql.LogicalSchema;
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
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.eclipse.jetty.http.HttpStatus.Code;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class StreamedQueryResourceTest {

  private static final Duration DISCONNECT_CHECK_INTERVAL = Duration.ofMillis(1000);
  private static final Duration COMMAND_QUEUE_CATCHUP_TIMOEUT = Duration.ofMillis(1000);
  private static final LogicalSchema SOME_SCHEMA = LogicalSchema.of(SchemaBuilder.struct()
      .field("f1", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
      .build());

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock(MockType.NICE)
  private KsqlConfig ksqlConfig;
  @Mock(MockType.NICE)
  private KsqlEngine mockKsqlEngine;
  @Mock(MockType.NICE)
  private ServiceContext serviceContext;
  @Mock(MockType.NICE)
  private KafkaTopicClient mockKafkaTopicClient;
  @Mock(MockType.NICE)
  private StatementParser mockStatementParser;
  @Mock
  private CommandQueue commandQueue;
  @Mock(MockType.NICE)
  private ActivenessRegistrar activenessRegistrar;
  @Mock
  private Consumer<QueryMetadata> queryCloseCallback;
  private StreamedQueryResource testResource;

  private final static String queryString = "SELECT * FROM test_stream;";
  private PreparedStatement<Statement> statement;

  @Before
  public void setup() {
    expect(mockKsqlEngine.isAcceptingStatements()).andReturn(true);
    expect(serviceContext.getTopicClient()).andReturn(mockKafkaTopicClient);
    expect(mockKsqlEngine.hasActiveQueries()).andReturn(false);
    statement = PreparedStatement.of("s", mock(Statement.class));
    expect(mockStatementParser.parseSingleStatement(queryString))
        .andReturn(statement);
    replay(mockKsqlEngine, mockStatementParser);

    testResource = new StreamedQueryResource(
        ksqlConfig,
        mockKsqlEngine,
        mockStatementParser,
        commandQueue,
        DISCONNECT_CHECK_INTERVAL,
        COMMAND_QUEUE_CATCHUP_TIMOEUT,
        activenessRegistrar,
        (sc, metastore, statement) -> {
          return;
        });
  }

  @Test
  public void shouldReturn400OnBadStatement() throws Exception {
    // Given:
    reset(mockStatementParser);
    expect(mockStatementParser.parseSingleStatement(anyString()))
        .andThrow(new IllegalArgumentException("some error message"));

    replay(mockStatementParser);

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
    // Given:
    replay(commandQueue);

    // When:
    testResource.streamQuery(
        serviceContext,
        new KsqlRequest(queryString, Collections.emptyMap(), null)
    );

    // Then:
    verify(commandQueue);
  }

  @Test
  public void shouldWaitIfCommandSequenceNumberSpecified() throws Exception {
    // Given:
    commandQueue.ensureConsumedPast(eq(3L), anyObject());
    expectLastCall();

    replay(commandQueue);

    // When:
    testResource.streamQuery(
        serviceContext,
        new KsqlRequest(queryString, Collections.emptyMap(), 3L)
    );

    // Then:
    verify(commandQueue);
  }

  @Test
  public void shouldReturnServiceUnavailableIfTimeoutWaitingForCommandSequenceNumber()
      throws Exception {
    // Given:
    commandQueue.ensureConsumedPast(anyLong(), anyObject());
    expectLastCall().andThrow(new TimeoutException("whoops"));

    replay(commandQueue);

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
        new KsqlRequest(queryString, Collections.emptyMap(), 3L)
    );
  }

  @SuppressWarnings("unchecked")
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
    mockKafkaStreams.start();
    expectLastCall();
    mockKafkaStreams.setUncaughtExceptionHandler(anyObject(Thread.UncaughtExceptionHandler.class));
    expectLastCall();
    mockKafkaStreams.cleanUp();
    expectLastCall();
    mockKafkaStreams.close();
    expectLastCall();

    final OutputNode mockOutputNode = niceMock(OutputNode.class);
    expect(mockOutputNode.accept(anyObject(PlanSourceExtractorVisitor.class), anyObject()))
        .andReturn(null);

    final Map<String, Object> requestStreamsProperties = Collections.emptyMap();

    reset(mockStatementParser);
    statement = PreparedStatement.of("query", mock(Query.class));
    expect(mockStatementParser.parseSingleStatement(queryString))
        .andReturn(statement);

    reset(mockKsqlEngine);

    final TransientQueryMetadata transientQueryMetadata =
        new TransientQueryMetadata(
            queryString,
            mockKafkaStreams,
            SOME_SCHEMA,
            Collections.emptySet(),
            limitHandler -> {},
            "",
            rowQueue,
            DataSourceType.KSTREAM,
            "",
            mock(Topology.class),
            Collections.emptyMap(),
            Collections.emptyMap(),
            queryCloseCallback);
    reset(mockOutputNode);
    expect(mockKsqlEngine.execute(serviceContext,
        ConfiguredStatement.of(statement, requestStreamsProperties, ksqlConfig)))
        .andReturn(ExecuteResult.of(transientQueryMetadata));

    expect(mockKsqlEngine.isAcceptingStatements()).andReturn(true);
    replay(mockKsqlEngine, mockStatementParser, mockKafkaStreams, mockOutputNode);

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
      if (responseLine.trim().isEmpty()) {
        i--;
      } else {
        final GenericRow expectedRow;
        synchronized (writtenRows) {
          expectedRow = writtenRows.poll();
        }
        final GenericRow testRow = objectMapper.readValue(responseLine, StreamedRow.class).getRow();
        assertEquals(expectedRow, testRow);
      }
    }

    responseOutputStream.close();

    queryWriterThread.join();
    rowQueuePopulatorThread.interrupt();
    rowQueuePopulatorThread.join();

    // Definitely want to make sure that the Kafka Streams instance has been closed and cleaned up
    verify(mockKafkaStreams);

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
  public void shouldUpdateTheLastRequestTime() throws Exception {
    // Given:
    activenessRegistrar.updateLastRequestTime();
    EasyMock.expectLastCall();

    EasyMock.replay(activenessRegistrar);

    // When:
    testResource.streamQuery(
        serviceContext,
        new KsqlRequest(queryString, Collections.emptyMap(), null)
    );

    // Then:
    EasyMock.verify(activenessRegistrar);
  }

}
