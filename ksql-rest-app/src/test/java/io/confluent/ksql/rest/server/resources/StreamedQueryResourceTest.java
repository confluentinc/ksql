package io.confluent.ksql.rest.server.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.KSQLEngine;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.rest.entity.KSQLRequest;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.StatementParser;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
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
import java.util.Scanner;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicReference;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertTrue;

public class StreamedQueryResourceTest {

  @Test
  public void testStreamQuery() throws Throwable {
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
    mockKafkaStreams.close();
    expectLastCall();
    mockKafkaStreams.cleanUp();
    expectLastCall();

    final OutputNode mockOutputNode = mock(OutputNode.class);
    expect(mockOutputNode.getSchema()).andReturn(SchemaBuilder.struct().field("f1", SchemaBuilder.INT32_SCHEMA));

    final QueuedQueryMetadata queuedQueryMetadata =
        new QueuedQueryMetadata(queryString, mockKafkaStreams, mockOutputNode, rowQueue);

    KSQLEngine mockKSQLEngine = mock(KSQLEngine.class);
    expect(mockKSQLEngine.buildMultipleQueries(true, queryString))
        .andReturn(Collections.singletonList(queuedQueryMetadata));

    StatementParser mockStatementParser = mock(StatementParser.class);
    expect(mockStatementParser.parseSingleStatement(queryString)).andReturn(mock(Query.class));

    replay(mockKSQLEngine, mockStatementParser, mockKafkaStreams, mockOutputNode);

    StreamedQueryResource testResource = new StreamedQueryResource(mockKSQLEngine, mockStatementParser, 1000);

    Response response = testResource.streamQuery(new KSQLRequest(queryString));
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
        assertTrue(expectedRow.hasTheSameContent(testRow));
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
