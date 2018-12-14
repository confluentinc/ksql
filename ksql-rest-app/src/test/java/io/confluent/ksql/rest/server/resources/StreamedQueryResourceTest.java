/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.resources;

import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorCode;
import static io.confluent.ksql.rest.entity.KsqlErrorMessageMatchers.errorMessage;
import static io.confluent.ksql.rest.server.resources.KsqlRestExceptionMatchers.exceptionKsqlErrorMessage;
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
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.planner.PlanSourceExtractorVisitor;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.computation.ReplayableCommandQueue;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.rest.util.JsonMapper;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueuedQueryMetadata;
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
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
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

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock(MockType.NICE)
  private KsqlConfig ksqlConfig;
  @Mock(MockType.NICE)
  private KsqlEngine mockKsqlEngine;
  @Mock(MockType.NICE)
  private KafkaTopicClient mockKafkaTopicClient;
  @Mock(MockType.NICE)
  private StatementParser mockStatementParser;
  @Mock
  private ReplayableCommandQueue replayableCommandQueue;
  @Mock(MockType.NICE)
  private ActivenessRegistrar activenessRegistrar;
  private StreamedQueryResource testResource;

  private final static String queryString = "SELECT * FROM test_stream;";

  @Before
  public void setup() {
    expect(mockKsqlEngine.getTopicClient()).andReturn(mockKafkaTopicClient);
    expect(mockKsqlEngine.hasActiveQueries()).andReturn(false);
    expect(mockStatementParser.parseSingleStatement(queryString))
        .andReturn(mock(Statement.class));
    replay(mockKsqlEngine, mockStatementParser);

    testResource = new StreamedQueryResource(
        ksqlConfig,
        mockKsqlEngine,
        mockStatementParser,
        replayableCommandQueue,
        DISCONNECT_CHECK_INTERVAL,
        activenessRegistrar);
  }

  @Test
  public void shouldReturn400OnBadStatement() throws Exception {
    // Given:
    reset(mockStatementParser);
    expect(mockStatementParser.parseSingleStatement(anyString()))
        .andThrow(new IllegalArgumentException("some error message"));

    replay(mockStatementParser);

    // When:
    final Response response = testResource.
        streamQuery(new KsqlRequest("query", Collections.emptyMap(), null));

    // Then:
    verify(mockStatementParser);
    assertThat(response.getStatus(), equalTo(Response.Status.BAD_REQUEST.getStatusCode()));
    assertThat(response.getEntity(), instanceOf(KsqlErrorMessage.class));
    final KsqlErrorMessage errorMessage = (KsqlErrorMessage)response.getEntity();
    assertThat(errorMessage.getErrorCode(), equalTo(Errors.ERROR_CODE_BAD_REQUEST));
    assertThat(errorMessage.getMessage(), containsString("some error message"));
  }

  @Test
  public void shouldNotWaitIfCommandSequenceNumberSpecified() throws Exception {
    // Given:
    replay(replayableCommandQueue);

    // When:
    testResource.streamQuery(new KsqlRequest(queryString, Collections.emptyMap(), null));

    // Then:
    verify(replayableCommandQueue);
  }

  @Test
  public void shouldWaitIfCommandSequenceNumberSpecified() throws Exception {
    // Given:
    replayableCommandQueue.ensureConsumedPast(eq(3L), anyObject());
    expectLastCall();

    replay(replayableCommandQueue);

    // When:
    testResource.streamQuery(new KsqlRequest(queryString, Collections.emptyMap(), 3L));

    // Then:
    verify(replayableCommandQueue);
  }

  @Test
  public void shouldReturnServiceUnavailableIfTimeoutWaitingForCommandSequenceNumber()
      throws Exception {
    // Given:
    replayableCommandQueue.ensureConsumedPast(anyLong(), anyObject());
    expectLastCall().andThrow(new TimeoutException("whoops"));

    replay(replayableCommandQueue);

    // Expect
    expectedException.expect(KsqlRestException.class);
    expectedException.expect(exceptionStatusCode(is(Code.SERVICE_UNAVAILABLE)));
    expectedException.expect(exceptionKsqlErrorMessage(errorMessage(is("whoops"))));
    expectedException.expect(
        exceptionKsqlErrorMessage(errorCode(is(Errors.ERROR_CODE_COMMAND_QUEUE_CATCHUP_TIMEOUT))));

    // When:
    testResource.streamQuery(new KsqlRequest(queryString, Collections.emptyMap(), 3L));
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

    final Thread rowQueuePopulatorThread = new Thread(new Runnable() {
      @Override
      public void run() {
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
      }
    }, "Row Queue Populator");
    rowQueuePopulatorThread.setUncaughtExceptionHandler(threadExceptionHandler);
    rowQueuePopulatorThread.start();

    final KafkaStreams mockKafkaStreams = mock(KafkaStreams.class);
    mockKafkaStreams.start();
    expectLastCall();
    mockKafkaStreams.setUncaughtExceptionHandler(anyObject(Thread.UncaughtExceptionHandler.class));
    expectLastCall();
    expect(mockKafkaStreams.state()).andReturn(State.RUNNING).once();
    expect(mockKafkaStreams.state()).andReturn(KafkaStreams.State.NOT_RUNNING).once();
    mockKafkaStreams.close();
    expectLastCall();

    final OutputNode mockOutputNode = niceMock(OutputNode.class);
    expect(mockOutputNode.accept(anyObject(PlanSourceExtractorVisitor.class), anyObject()))
        .andReturn(null);

    final Map<String, Object> requestStreamsProperties = Collections.emptyMap();

    reset(mockKsqlEngine);
    expect(mockKsqlEngine.getTopicClient()).andReturn(mockKafkaTopicClient);
    expect(mockKsqlEngine.getSchemaRegistryClient()).andReturn(new MockSchemaRegistryClient());
    expect(mockKsqlEngine.hasActiveQueries()).andReturn(false);

    final QueuedQueryMetadata queuedQueryMetadata =
        new QueuedQueryMetadata(queryString, mockKafkaStreams, mockOutputNode, "",
            rowQueue, DataSource.DataSourceType.KSTREAM, "",
            mockKafkaTopicClient, null, Collections.emptyMap());
    reset(mockOutputNode);
    expect(mockOutputNode.getSchema())
        .andReturn(SchemaBuilder.struct().field("f1", SchemaBuilder.OPTIONAL_INT32_SCHEMA));
    expect(mockKsqlEngine.execute(queryString, ksqlConfig, requestStreamsProperties))
        .andReturn(Collections.singletonList(queuedQueryMetadata));
    mockKsqlEngine.removeTemporaryQuery(queuedQueryMetadata);
    expectLastCall();

    reset(mockStatementParser);
    expect(mockStatementParser.parseSingleStatement(queryString)).andReturn(mock(Query.class));
    replay(mockKsqlEngine, mockStatementParser, mockKafkaStreams, mockOutputNode);

    final Response response =
        testResource.streamQuery(new KsqlRequest(queryString, requestStreamsProperties, null));
    final PipedOutputStream responseOutputStream = new EOFPipedOutputStream();
    final PipedInputStream responseInputStream = new PipedInputStream(responseOutputStream, 1);
    final StreamingOutput responseStream = (StreamingOutput) response.getEntity();

    final Thread queryWriterThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          responseStream.write(responseOutputStream);
        } catch (final EOFException exception) {
          // It's fine
        } catch (final IOException exception) {
          throw new RuntimeException(exception);
        }
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

    public EOFPipedOutputStream() {
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
    testResource.streamQuery(new KsqlRequest(queryString, Collections.emptyMap(), null));

    // Then:
    EasyMock.verify(activenessRegistrar);
  }

}