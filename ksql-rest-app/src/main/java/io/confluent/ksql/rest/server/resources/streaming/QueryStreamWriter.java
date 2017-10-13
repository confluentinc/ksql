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

package io.confluent.ksql.rest.server.resources.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueuedQueryMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.StreamingOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class QueryStreamWriter implements StreamingOutput {

  private static final Logger log = LoggerFactory.getLogger(QueryStreamWriter.class);

  private final QueuedQueryMetadata queryMetadata;
  private final long disconnectCheckInterval;
  private final AtomicReference<Throwable> streamsException;
  private final KafkaTopicClient kafkaTopicClient;

  QueryStreamWriter(
      KsqlEngine ksqlEngine,
      long disconnectCheckInterval,
      String queryString,
      Map<String, Object> overriddenProperties
  )
      throws Exception {
    this.kafkaTopicClient = ksqlEngine.getTopicClient();
    QueryMetadata queryMetadata =
        ksqlEngine.buildMultipleQueries(true, queryString, overriddenProperties).get(0);
    if (!(queryMetadata instanceof QueuedQueryMetadata)) {
      throw new Exception(String.format(
          "Unexpected metadata type: expected QueuedQueryMetadata, found %s instead",
          queryMetadata.getClass()
      ));
    }

    this.disconnectCheckInterval = disconnectCheckInterval;
    this.queryMetadata = ((QueuedQueryMetadata) queryMetadata);

    this.streamsException = new AtomicReference<>(null);
    this.queryMetadata.getKafkaStreams().setUncaughtExceptionHandler(new StreamsExceptionHandler());

    queryMetadata.getKafkaStreams().start();
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
          // If no new rows have been written, the user may have terminated the connection without
          // us knowing. Check by trying to write a single newline.
          if (!rowsWritten.getAndSet(false)) {
            synchronized (out) {
              out.write("\n".getBytes());
              out.flush();
            }
          }
        }
      } catch (EOFException exception) {
        // The user has terminated the connection; we can stop writing
      } catch (InterruptedException exception) {
        // The most likely cause of this is the server shutting down. Should just try to close
        // gracefully, without writing any more to the connection stream.
        log.warn("Interrupted while writing to connection stream");
      } catch (Throwable exception) {
        log.error("Exception occurred while writing to connection stream: ", exception);
        synchronized (out) {
          out.write("\n".getBytes());
          if (exception.getCause() instanceof KsqlException) {
            new ObjectMapper().writeValue(out, new StreamedRow(exception.getCause()));
          } else {
            new ObjectMapper().writeValue(out, new StreamedRow(exception));
          }
          out.write("\n".getBytes());
          out.flush();
        }
      }

      if (rowWriterThread.isAlive()) {
        try {
          rowWriterThread.interrupt();
          rowWriterThread.join();
        } catch (InterruptedException exception) {
          log.warn(
              "Failed to join row writer thread; setting to daemon to avoid hanging on shutdown"
          );
          rowWriterThread.setDaemon(true);
        }
      }

    } finally {
      queryMetadata.getKafkaStreams().close(100L, TimeUnit.MILLISECONDS);
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
