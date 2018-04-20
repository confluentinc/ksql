/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.server.resources;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KafkaTopicClientImpl;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.QueuedQueryMetadata;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.junit.Test;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicReference;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StreamedQueryResourceTest {
  @Test
  public void shouldReturn400OnBadStatement() throws Exception {
    String queryString = "SELECT * FROM test_stream;";

    KsqlEngine mockKsqlEngine = mock(KsqlEngine.class);
    KafkaTopicClient mockKafkaTopicClient = mock(KafkaTopicClientImpl.class);
    expect(mockKsqlEngine.getTopicClient()).andReturn(mockKafkaTopicClient);

    StatementParser mockStatementParser = mock(StatementParser.class);
    expect(mockStatementParser.parseSingleStatement(queryString))
        .andThrow(new IllegalArgumentException("some msg only the parser would use"));

    replay(mockKsqlEngine, mockKafkaTopicClient, mockStatementParser);

    StreamedQueryResource testResource = new StreamedQueryResource(
        mockKsqlEngine, mockStatementParser, 1000);

    Response response =
        testResource.streamQuery(new KsqlRequest(queryString, Collections.emptyMap()));
    assertThat(response.getStatus(), equalTo(Response.Status.BAD_REQUEST.getStatusCode()));
    assertThat(response.getEntity(), instanceOf(KsqlErrorMessage.class));
    KsqlErrorMessage errorMessage = (KsqlErrorMessage)response.getEntity();
    assertThat(errorMessage.getErrorCode(), equalTo(Errors.ERROR_CODE_BAD_REQUEST));
    assertThat(
        errorMessage.getMessage(), containsString("some msg only the parser would use"));
  }

  @Test
  public void shouldReturn400OnBuildMultipleQueriesError() throws Exception {
    String queryString = "SELECT * FROM test_stream;";

    KsqlEngine mockKsqlEngine = mock(KsqlEngine.class);
    KafkaTopicClient mockKafkaTopicClient = mock(KafkaTopicClientImpl.class);
    expect(mockKsqlEngine.getTopicClient()).andReturn(mockKafkaTopicClient);

    StatementParser mockStatementParser = mock(StatementParser.class);
    expect(mockStatementParser.parseSingleStatement(queryString))
        .andReturn(mock(Query.class));

    expect(mockKsqlEngine.buildMultipleQueries(queryString, Collections.emptyMap()))
        .andThrow(new KsqlException("some msg only the engine would use"));

    replay(mockKsqlEngine, mockKafkaTopicClient, mockStatementParser);

    StreamedQueryResource testResource = new StreamedQueryResource(
        mockKsqlEngine, mockStatementParser, 1000);

    Response response =
        testResource.streamQuery(new KsqlRequest(queryString, Collections.emptyMap()));
    assertThat(response.getStatus(), equalTo(Response.Status.BAD_REQUEST.getStatusCode()));
    assertThat(response.getEntity(), instanceOf(KsqlErrorMessage.class));
    KsqlErrorMessage errorMessage = (KsqlErrorMessage)response.getEntity();
    assertThat(errorMessage.getErrorCode(), equalTo(Errors.ERROR_CODE_BAD_REQUEST));
    assertThat(
        errorMessage.getMessage(), containsString("some msg only the engine would use"));
  }

  @Test
  public void shouldStreamRowsCorrectly() throws Throwable {
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
          for (int i = 0; ; i++) {
            String key = Integer.toString(i);
            GenericRow value = new GenericRow(Collections.singletonList(i));
            synchronized (writtenRows) {
              writtenRows.add(value);
            }
            rowQueue.put(new KeyValue<>(key, value));
          }
        } catch (InterruptedException exception) {
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
    expect(mockKafkaStreams.state()).andReturn(KafkaStreams.State.NOT_RUNNING);
    mockKafkaStreams.close();
    expectLastCall();
    mockKafkaStreams.cleanUp();
    expectLastCall();

    final OutputNode mockOutputNode = mock(OutputNode.class);
    expect(mockOutputNode.getSchema())
        .andReturn(SchemaBuilder.struct().field("f1", SchemaBuilder.INT32_SCHEMA));

    final Map<String, Object> requestStreamsProperties = Collections.emptyMap();

    KsqlEngine mockKsqlEngine = mock(KsqlEngine.class);
    KafkaTopicClient mockKafkaTopicClient = mock(KafkaTopicClientImpl.class);
    expect(mockKsqlEngine.getTopicClient()).andReturn(mockKafkaTopicClient);
    expect(mockKsqlEngine.getSchemaRegistryClient()).andReturn(new MockSchemaRegistryClient());

    final QueuedQueryMetadata queuedQueryMetadata =
        new QueuedQueryMetadata(queryString, mockKafkaStreams, mockOutputNode, "",
                                rowQueue, DataSource.DataSourceType.KSTREAM, "",
                                mockKafkaTopicClient, null, Collections.emptyMap());
    expect(mockKsqlEngine.buildMultipleQueries(queryString, requestStreamsProperties))
        .andReturn(Collections.singletonList(queuedQueryMetadata));
    mockKsqlEngine.removeTemporaryQuery(queuedQueryMetadata);
    expectLastCall();

    StatementParser mockStatementParser = mock(StatementParser.class);
    expect(mockStatementParser.parseSingleStatement(queryString)).andReturn(mock(Query.class));

    replay(mockKsqlEngine, mockStatementParser, mockKafkaStreams, mockOutputNode);

    StreamedQueryResource testResource = new StreamedQueryResource(mockKsqlEngine, mockStatementParser, 1000);

    Response response =
        testResource.streamQuery(new KsqlRequest(queryString, requestStreamsProperties));
    PipedOutputStream responseOutputStream = new EOFPipedOutputStream();
    PipedInputStream responseInputStream = new PipedInputStream(responseOutputStream, 1);
    StreamingOutput responseStream = (StreamingOutput) response.getEntity();

    final Thread queryWriterThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          responseStream.write(responseOutputStream);
        } catch (EOFException exception) {
          // It's fine
        } catch (IOException exception) {
          throw new RuntimeException(exception);
        }
      }
    }, "Query Writer");
    queryWriterThread.setUncaughtExceptionHandler(threadExceptionHandler);
    queryWriterThread.start();

    Scanner responseScanner = new Scanner(responseInputStream);
    ObjectMapper objectMapper = new ObjectMapper();
    for (int i = 0; i < 5; i++) {
      if (!responseScanner.hasNextLine()) {
        throw new Exception("Response input stream failed to have expected line available");
      }
      String responseLine = responseScanner.nextLine();
      if (responseLine.trim().isEmpty()) {
        i--;
      } else {
        GenericRow expectedRow;
        synchronized (writtenRows) {
          expectedRow = writtenRows.poll();
        }
        GenericRow testRow = objectMapper.readValue(responseLine, StreamedRow.class).getRow();
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
    Throwable exception = threadException.get();
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
      } catch (IOException exception) {
        // Might have been closed during the call to super.flush();
        throwIfClosed();
        throw exception;
      }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      throwIfClosed();
      try {
        super.write(b, off, len);
      } catch (IOException exception) {
        // Might have been closed during the call to super.write();
        throwIfClosed();
        throw exception;
      }
    }

    @Override
    public void write(int b) throws IOException {
      throwIfClosed();
      try {
        super.write(b);
      } catch (IOException exception) {
        // Might have been closed during the call to super.write();
        throwIfClosed();
        throw exception;
      }
    }
  }
}
