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

package io.confluent.ksql.rest.server.resources.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.server.StreamingOutput;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.PushContinuationToken;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.util.KeyValueMetadata;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PushQueryMetadata;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QueryStreamWriter implements StreamingOutput {

  private static final int WRITE_TIMEOUT_MS = 10 * 60000;

  private static final Logger log = LoggerFactory.getLogger(QueryStreamWriter.class);

  private final PushQueryMetadata queryMetadata;
  private final long disconnectCheckInterval;
  private final ObjectMapper objectMapper;
  private final TombstoneFactory tombstoneFactory;
  private volatile Exception streamsException;
  private volatile boolean limitReached = false;
  private volatile boolean complete;
  private volatile boolean connectionClosed;
  private boolean closed;

  QueryStreamWriter(
      final PushQueryMetadata queryMetadata,
      final long disconnectCheckInterval,
      final ObjectMapper objectMapper,
      final CompletableFuture<Void> connectionClosedFuture
  ) {
    this(queryMetadata, disconnectCheckInterval, objectMapper, connectionClosedFuture, false);
  }

  QueryStreamWriter(
      final PushQueryMetadata queryMetadata,
      final long disconnectCheckInterval,
      final ObjectMapper objectMapper,
      final CompletableFuture<Void> connectionClosedFuture,
      final boolean emptyStream
  ) {
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    this.disconnectCheckInterval = disconnectCheckInterval;
    this.queryMetadata = Objects.requireNonNull(queryMetadata, "queryMetadata");
    this.queryMetadata.setLimitHandler(() -> limitReached = true);
    this.queryMetadata.setCompletionHandler(() -> complete = true);
    this.queryMetadata.setUncaughtExceptionHandler(new StreamsExceptionHandler());
    this.tombstoneFactory = TombstoneFactory.create(
        queryMetadata.getLogicalSchema(), queryMetadata.getResultType());
    connectionClosedFuture.thenAccept(v -> connectionClosed = true);
    if (emptyStream) {
      // if we're writing an empty stream, it's already complete,
      // and we don't even need to bother starting Streams.
      complete = true;
    } else {
      queryMetadata.start();
    }
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  @Override
  public void write(final OutputStream out) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    try {
      out.write("[".getBytes(StandardCharsets.UTF_8));
      write(out, buildHeader());

      final BlockingRowQueue rowQueue = queryMetadata.getRowQueue();

      while (!connectionClosed && queryMetadata.isRunning() && !limitReached && !complete) {
        final KeyValueMetadata<List<?>, GenericRow> row = rowQueue.poll(
            disconnectCheckInterval,
            TimeUnit.MILLISECONDS
        );
        if (row != null) {
          write(out, buildRow(row));
        } else {
          // If no new rows have been written, the user may have terminated the connection without
          // us knowing. Check by trying to write a single newline.
          out.write("\n".getBytes(StandardCharsets.UTF_8));
          out.flush();
        }
        drainAndThrowOnError(out);
      }

      if (connectionClosed) {
        return;
      }

      drain(out);

      if (limitReached) {
        objectMapper.writeValue(out, StreamedRow.finalMessage("Limit Reached"));
      } else if (complete) {
        objectMapper.writeValue(out, StreamedRow.finalMessage("Query Completed"));
      }

      out.write("]\n".getBytes(StandardCharsets.UTF_8));
      out.flush();
    } catch (final EOFException exception) {
      // The user has terminated the connection; we can stop writing
      log.warn("Query terminated due to exception:" + exception.toString());
    } catch (final InterruptedException exception) {
      // The most likely cause of this is the server shutting down. Should just try to close
      // gracefully, without writing any more to the connection stream.
      log.warn("Interrupted while writing to connection stream");
    } catch (final Exception exception) {
      log.error("Exception occurred while writing to connection stream: ", exception);
      outputException(out, exception);
    } finally {
      close();
    }
  }

  private void write(final OutputStream output, final StreamedRow row) throws IOException {
    objectMapper.writeValue(output, row);
    output.write(",\n".getBytes(StandardCharsets.UTF_8));
    output.flush();
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      queryMetadata.close();
      closed = true;
    }
  }

  @Override
  public int getWriteTimeoutMs() {
    return WRITE_TIMEOUT_MS;
  }

  private StreamedRow buildHeader() {
    final QueryId queryId = queryMetadata.getQueryId();

    // Push queries only return value columns, but query metadata schema includes key and meta:
    final LogicalSchema storedSchema = queryMetadata.getLogicalSchema();

    final Builder projectionSchema = LogicalSchema.builder();

    storedSchema.value().forEach(projectionSchema::valueColumn);

    // No session consistency offered for push or stream pull queries
    return StreamedRow.header(queryId, projectionSchema.build());
  }

  private StreamedRow buildRow(final KeyValueMetadata<List<?>, GenericRow> row) {
    if (row.getRowMetadata().isPresent()
        && row.getRowMetadata().get().getPushOffsetsRange().isPresent()) {
      return StreamedRow.continuationToken(new PushContinuationToken(
          row.getRowMetadata().get().getPushOffsetsRange().get().serialize()));
    } else {
      if (row.getKeyValue().value() == null) {
        return StreamedRow.tombstone(tombstoneFactory.createRow(row.getKeyValue()));
      } else {
        return StreamedRow.pushRow(row.getKeyValue().value());
      }
    }
  }

  private void outputException(final OutputStream out, final Throwable exception) {
    try {
      out.write("\n".getBytes(StandardCharsets.UTF_8));
      if (exception.getCause() instanceof KsqlException) {
        objectMapper.writeValue(out, StreamedRow
            .error(exception.getCause(), Errors.ERROR_CODE_SERVER_ERROR));
      } else {
        objectMapper.writeValue(out, StreamedRow
            .error(exception, Errors.ERROR_CODE_SERVER_ERROR));
      }
      out.write(",\n".getBytes(StandardCharsets.UTF_8));
      out.flush();
    } catch (final IOException e) {
      log.debug("Client disconnected while attempting to write an error message");
    }
  }

  private void drainAndThrowOnError(final OutputStream out) throws Exception {
    if (streamsException != null) {
      drain(out);
      throw streamsException;
    }
  }

  private void drain(final OutputStream out) throws IOException {
    final List<KeyValueMetadata<List<?>, GenericRow>> rows = Lists.newArrayList();
    queryMetadata.getRowQueue().drainTo(rows);

    for (final KeyValueMetadata<List<?>, GenericRow> row : rows) {
      write(out, buildRow(row));
    }
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
