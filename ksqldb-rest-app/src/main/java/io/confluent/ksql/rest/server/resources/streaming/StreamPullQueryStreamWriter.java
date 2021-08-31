/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.server.StreamingOutput;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.logging.query.QueryLogger;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.StreamPullQueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamPullQueryStreamWriter implements StreamingOutput {

  private static final Logger LOG = LoggerFactory.getLogger(StreamPullQueryStreamWriter.class);
  private static final int WRITE_TIMEOUT_MS = 3000;

  private final long disconnectCheckInterval;
  private final TransientQueryMetadata transientQueryMetadata;
  private final Supplier<Boolean> isComplete;
  private final TombstoneFactory tombstoneFactory;
  private final ObjectMapper objectMapper;
  private volatile Exception streamsException;
  private final AtomicBoolean connectionClosed = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);


  StreamPullQueryStreamWriter(
      final TransientQueryMetadata transientQueryMetadata,
      final long disconnectCheckInterval,
      final ObjectMapper objectMapper,
      final CompletableFuture<Void> connectionClosedFuture,
      final Supplier<Boolean> isComplete) {
    this.transientQueryMetadata = Objects.requireNonNull(transientQueryMetadata, "result");
    this.isComplete = Objects.requireNonNull(isComplete, "isComplete");
    this.transientQueryMetadata
        .setUncaughtExceptionHandler(new StreamsExceptionHandler());

    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    this.disconnectCheckInterval = disconnectCheckInterval;
    this.tombstoneFactory =
        TombstoneFactory.create(transientQueryMetadata);
    connectionClosedFuture.thenAccept(v -> connectionClosed.set(true));
    transientQueryMetadata.start();
  }

  @Override
  public void write(final OutputStream output) {
    try {
      System.out.println("WO [");
      output.write("[".getBytes(StandardCharsets.UTF_8));
      write(output, buildHeader());

      final BlockingRowQueue rowQueue =
          transientQueryMetadata.getRowQueue();

      // While the query is still running, and the client hasn't closed the connection, continue to
      // poll new rows.
      while (!connectionClosed.get()
          && transientQueryMetadata.isRunning()
          && !isComplete.get()
      ) {
        final KeyValue<List<?>, GenericRow> row = rowQueue.poll(
            disconnectCheckInterval,
            TimeUnit.MILLISECONDS
        );
        if (row != null) {
          write(output, buildRow(row));
        } else {
          // If no new rows have been written, the user may have terminated the connection without
          // us knowing. Check by trying to write a single newline.
          output.write("\n".getBytes(StandardCharsets.UTF_8));
          output.flush();
        }
        drainAndThrowOnError(output);
      }

      if (connectionClosed.get()) {
        return;
      }

      drain(output);
      System.out.println("WO ]");
      output.write("]\n".getBytes(StandardCharsets.UTF_8));
      output.flush();

      QueryLogger.info(
          "Finished stream pull query results",
          transientQueryMetadata.getStatementString()
      );

    } catch (InterruptedException e) {
      // The most likely cause of this is the server shutting down. Should just try to close
      // gracefully, without writing any more to the connection stream.
      LOG.warn("Interrupted while writing to connection stream");
    } catch (Throwable e) {
      LOG.error("Exception occurred while writing to connection stream: ", e);
      outputException(output, e);
    } finally {
      close();
    }
  }

  private void write(final OutputStream output, final StreamedRow row) throws IOException {
    objectMapper.writeValue(output, row);
    output.write(",\n".getBytes(StandardCharsets.UTF_8));
    output.flush();
  }

  private StreamedRow buildHeader() {
    final QueryId queryId = transientQueryMetadata.getQueryId();

    // Push queries only return value columns, but query metadata schema includes key and meta:
    final LogicalSchema storedSchema = transientQueryMetadata.getLogicalSchema();

    final Builder projectionSchema = LogicalSchema.builder();

    storedSchema.value().forEach(projectionSchema::valueColumn);

    return StreamedRow.header(queryId, projectionSchema.build());
  }

  private StreamedRow buildRow(final KeyValue<List<?>, GenericRow> row) {
    return row.value() == null
        ? StreamedRow.tombstone(tombstoneFactory.createRow(row))
        : StreamedRow.pushRow(row.value());
  }

  private void drainAndThrowOnError(final OutputStream out) throws Exception {
    if (streamsException != null) {
      drain(out);
      throw streamsException;
    }
  }

  private void drain(final OutputStream out) throws IOException {
    final List<KeyValue<List<?>, GenericRow>> rows = Lists.newArrayList();
    transientQueryMetadata.getRowQueue().drainTo(rows);

    for (final KeyValue<List<?>, GenericRow> row : rows) {
      write(out, buildRow(row));
    }
  }

  /**
   * Outputs the given exception to the output stream.
   *
   * @param out       The output stream
   * @param exception The exception to write
   */
  private void outputException(final OutputStream out, final Throwable exception) {
    if (connectionClosed.get()) {
      return;
    }
    try {
      out.write(",\n".getBytes(StandardCharsets.UTF_8));
      if (exception.getCause() instanceof KsqlException) {
        objectMapper.writeValue(out, StreamedRow
            .error(exception.getCause(), Errors.ERROR_CODE_SERVER_ERROR));
      } else {
        objectMapper.writeValue(out, StreamedRow
            .error(exception, Errors.ERROR_CODE_SERVER_ERROR));
      }
      out.write("]\n".getBytes(StandardCharsets.UTF_8));
      out.flush();
    } catch (final IOException e) {
      LOG.debug("Client disconnected while attempting to write an error message");
    }
  }

  @Override
  public void close() {
    if (!closed.get()) {
      transientQueryMetadata.close();
      closed.set(true);
    }
  }

  @Override
  public int getWriteTimeoutMs() {
    return WRITE_TIMEOUT_MS;
  }

  /**
   * Converts the object to json and returns the string.
   *
   * @param object The object to convert
   * @return The serialized JSON
   */
  private String writeValueAsString(final Object object) {
    try {
      return objectMapper.writeValueAsString(object);
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Converts the KsqlNode to KsqlHostInfoEntity
   */
  private static Optional<KsqlHostInfoEntity> toKsqlHostInfo(final Optional<KsqlNode> ksqlNode) {
    return ksqlNode.map(
        node -> new KsqlHostInfoEntity(node.location().getHost(), node.location().getPort()));
  }

  private class StreamsExceptionHandler implements StreamsUncaughtExceptionHandler {

    @Override
    public StreamThreadExceptionResponse handle(final Throwable throwable) {
      streamsException = throwable instanceof Exception
          ? (Exception) throwable
          : new RuntimeException(throwable);
      return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
    }
  }
}
