package io.confluent.kql.rest.server.resources.streaming;

import io.confluent.kql.KQLEngine;
import io.confluent.kql.rest.server.resources.KQLExceptionMapper;
import io.confluent.kql.util.QueryMetadata;
import io.confluent.kql.util.QueuedQueryMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.StreamingOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class QueryStreamWriter implements StreamingOutput {

  private static final Logger log = LoggerFactory.getLogger(QueryStreamWriter.class);

  private final QueuedQueryMetadata queryMetadata;
  private final long disconnectCheckInterval;
  private final AtomicReference<Throwable> streamsException;

  QueryStreamWriter(KQLEngine kqlEngine, long disconnectCheckInterval, String queryString) throws Exception {
    QueryMetadata queryMetadata = kqlEngine.runMultipleQueries(true, queryString).get(0);
    if (!(queryMetadata instanceof QueuedQueryMetadata)) {
      throw new Exception(String.format(
          "Unexpected metadata type: expected QueuedQueryMetadata, found %s instead",
          queryMetadata.getClass()
      ));
    }

    this.disconnectCheckInterval = disconnectCheckInterval;
    this.queryMetadata = ((QueuedQueryMetadata) queryMetadata);

    this.streamsException = new AtomicReference<>(null);
    this.queryMetadata.getQueryKafkaStreams().setUncaughtExceptionHandler(new StreamsExceptionHandler());
  }

  @Override
  public void write(OutputStream out) throws IOException {
    try {
      AtomicBoolean rowsWritten = new AtomicBoolean(false);
      QueryRowWriter queryRowWriter = new QueryRowWriter(
          out,
          streamsException,
          queryMetadata.getRowQueue(),
          queryMetadata.getQueryOutputNode().getSchema().fields(),
          rowsWritten
      );
      Thread rowWriterThread = new Thread(queryRowWriter);
      rowWriterThread.start();
      try {
        while (true) {
          Thread.sleep(disconnectCheckInterval);
          Throwable exception = streamsException.get();
          if (exception != null) {
            throw exception;
          }
          // If no new rows have been written, the user may have terminated the connection without us knowing.
          // Check by trying to write a single newline.
          if (!rowsWritten.getAndSet(false)) {
            synchronized (out) {
              out.write("\n".getBytes());
              out.flush();
            }
          }
        }
      } catch (EOFException exception) {
        // The user has terminated the connection; we can stop writing
      } catch (Throwable exception) {
        log.error("Exception occurred while writing to connection stream: ", exception);
        out.write(("\n" + KQLExceptionMapper.stackTraceJson(exception).toString() + "\n").getBytes());
        out.flush();
      }

      if (rowWriterThread.isAlive()) {
        try {
          rowWriterThread.interrupt();
          rowWriterThread.join();
        } catch (InterruptedException exception) {
          log.warn("Failed to join row writer thread; setting to daemon to avoid hanging on shutdown");
          rowWriterThread.setDaemon(true);
        }
      }

    } finally {
      queryMetadata.getQueryKafkaStreams().close();
      queryMetadata.getQueryKafkaStreams().cleanUp();
    }
  }

  private class StreamsExceptionHandler implements Thread.UncaughtExceptionHandler {
    @Override
    public void uncaughtException(Thread thread, Throwable exception) {
      streamsException.compareAndSet(null, exception);
    }
  }
}
