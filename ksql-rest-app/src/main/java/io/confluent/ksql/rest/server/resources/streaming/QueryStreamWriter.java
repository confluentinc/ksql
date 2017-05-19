/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server.resources.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.KSQLEngine;
import io.confluent.ksql.rest.entity.ErrorMessage;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
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

  QueryStreamWriter(KSQLEngine ksqlEngine, long disconnectCheckInterval, String queryString) throws Exception {
    QueryMetadata queryMetadata = ksqlEngine.buildMultipleQueries(true, queryString).get(0);
    if (!(queryMetadata instanceof QueuedQueryMetadata)) {
      throw new Exception(String.format(
          "Unexpected metadata type: expected QueuedQueryMetadata, found %s instead",
          queryMetadata.getClass()
      ));
    }

    queryMetadata.getKafkaStreams().start();

    this.disconnectCheckInterval = disconnectCheckInterval;
    this.queryMetadata = ((QueuedQueryMetadata) queryMetadata);

    this.streamsException = new AtomicReference<>(null);
    this.queryMetadata.getKafkaStreams().setUncaughtExceptionHandler(new StreamsExceptionHandler());
  }

  @Override
  public void write(OutputStream out) throws IOException {
    try {
      AtomicBoolean rowsWritten = new AtomicBoolean(false);
      QueryRowWriter queryRowWriter = new QueryRowWriter(
          out,
          streamsException,
          queryMetadata.getRowQueue(),
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
        synchronized (out) {
          out.write("\n".getBytes());
          new ObjectMapper().writeValue(out, new ErrorMessage(exception));
          out.write("\n".getBytes());
          out.flush();
        }
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
      queryMetadata.getKafkaStreams().close();
      queryMetadata.getKafkaStreams().cleanUp();
    }
  }

  private class StreamsExceptionHandler implements Thread.UncaughtExceptionHandler {
    @Override
    public void uncaughtException(Thread thread, Throwable exception) {
      streamsException.compareAndSet(null, exception);
    }
  }
}
