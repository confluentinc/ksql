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
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.util.KeyValue;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QueryStreamWriter implements StreamingOutput {
  private static final int WRITE_TIMEOUT_MS = 10 * 60000;

  private static final Logger log = LoggerFactory.getLogger(QueryStreamWriter.class);

  private final TransientQueryMetadata queryMetadata;
  private final long disconnectCheckInterval;
  private final ObjectMapper objectMapper;
  private final TombstoneFactory tombstoneFactory;
  private volatile Exception streamsException;
  private volatile boolean limitReached = false;
  private volatile boolean connectionClosed;
  private boolean closed;

  QueryStreamWriter(
      final TransientQueryMetadata queryMetadata,
      final long disconnectCheckInterval,
      final ObjectMapper objectMapper,
      final CompletableFuture<Void> connectionClosedFuture
  ) {
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    this.disconnectCheckInterval = disconnectCheckInterval;
    this.queryMetadata = Objects.requireNonNull(queryMetadata, "queryMetadata");
    this.queryMetadata.setLimitHandler(new LimitHandler());
    this.queryMetadata.setUncaughtExceptionHandler(new StreamsExceptionHandler());
    this.tombstoneFactory = TombstoneFactory.create(queryMetadata);
    connectionClosedFuture.thenAccept(v -> connectionClosed = true);
    queryMetadata.start();
  }

  @Override
  public void write(final OutputStream out) {
    try {
      out.write("[".getBytes(StandardCharsets.UTF_8));
      write(out, buildHeader());

      while (!connectionClosed && queryMetadata.isRunning() && !limitReached) {
        final KeyValue<List<?>, GenericRow> row = queryMetadata.getRowQueue().poll(
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

      drain(out);

      if (limitReached) {
        objectMapper.writeValue(out, StreamedRow.finalMessage("Limit Reached"));
        out.write("]\n".getBytes(StandardCharsets.UTF_8));
        out.flush();
      }
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

    return StreamedRow.header(queryId, projectionSchema.build());
  }

  private StreamedRow buildRow(final KeyValue<List<?>, GenericRow> row) {
    return row.value() == null
        ? StreamedRow.tombstone(tombstoneFactory.createRow(row))
        : StreamedRow.pushRow(row.value());
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
    final List<KeyValue<List<?>, GenericRow>> rows = Lists.newArrayList();
    queryMetadata.getRowQueue().drainTo(rows);

    for (final KeyValue<List<?>, GenericRow> row : rows) {
      write(out, buildRow(row));
    }
  }

  private class StreamsExceptionHandler implements Thread.UncaughtExceptionHandler {
    @Override
    public void uncaughtException(final Thread thread, final Throwable exception) {
      streamsException = exception instanceof Exception
          ? (Exception) exception
          : new RuntimeException(exception);
    }
  }

  private class LimitHandler implements io.confluent.ksql.query.LimitHandler {
    @Override
    public void limitReached() {
      limitReached = true;
    }
  }
}
