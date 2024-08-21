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
import io.confluent.ksql.api.server.StreamingOutput;
import io.confluent.ksql.execution.pull.PullQueryResult;
import io.confluent.ksql.execution.pull.PullQueryRow;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.query.PullQueryQueue;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.ConsistencyToken;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PullQueryStreamWriter implements StreamingOutput {
  private static final Logger LOG = LoggerFactory.getLogger(PullQueryStreamWriter.class);
  private static final int WRITE_TIMEOUT_MS = 3000;

  private static final int FLUSH_SIZE_BYTES = 50 * 1024;
  private static final long MAX_FLUSH_MS = 1000;

  private final long disconnectCheckInterval;
  private final PullQueryQueue pullQueryQueue;
  private final Clock clock;
  private final PullQueryResult result;
  private final ObjectMapper objectMapper;
  private AtomicBoolean completed = new AtomicBoolean(false);
  private AtomicBoolean connectionClosed = new AtomicBoolean(false);
  private AtomicReference<Throwable> pullQueryException = new AtomicReference<>(null);
  private AtomicBoolean closed = new AtomicBoolean(false);
  private boolean sentAtLeastOneRow = false;

  PullQueryStreamWriter(
      final PullQueryResult result,
      final long disconnectCheckInterval,
      final ObjectMapper objectMapper,
      final PullQueryQueue pullQueryQueue,
      final Clock clock,
      final CompletableFuture<Void> connectionClosedFuture,
      final PreparedStatement<Query> statement
  ) {
    this.result = Objects.requireNonNull(result, "result");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    this.disconnectCheckInterval = disconnectCheckInterval;
    this.pullQueryQueue = Objects.requireNonNull(pullQueryQueue, "pullQueryQueue");
    this.clock = Objects.requireNonNull(clock, "clock");
    connectionClosedFuture.thenAccept(v -> connectionClosed.set(true));
    result.onException(t -> {
      if (pullQueryException.getAndSet(t) == null) {
        interruptWriterThread();
      }
    });
    result.onCompletion(v -> {
      if (!completed.getAndSet(true)) {
        result.getConsistencyOffsetVector().ifPresent(pullQueryQueue::putConsistencyVector);
        interruptWriterThread();
      }
    });
    try {
      result.start();
    } catch (Exception e) {
      throw new KsqlStatementException(
          e.getMessage() == null
              ? "Server Error"
              : e.getMessage(),
          statement.getMaskedStatementText(),
          e
      );
    }
  }

  @Override
  public void write(final OutputStream output) {
    try {
      final WriterState writerState = new WriterState(clock);
      final QueueWrapper queueWrapper = new QueueWrapper(pullQueryQueue, disconnectCheckInterval);

      // First write the header with the schema
      final StreamedRow header = StreamedRow.header(result.getQueryId(), result.getSchema());
      writerState.append("[").append(writeValueAsString(header));

      // While the query is still running, and the client hasn't closed the connection, continue to
      // poll new rows.
      while (!connectionClosed.get() && !isCompletedOrHasException()) {
        processRow(output, writerState, queueWrapper);
      }

      if (connectionClosed.get()) {
        return;
      }

      // If the query finished quickly, we might not have thrown the error
      drainAndThrowOnError(output, writerState, queueWrapper);

      // If no error was thrown above, drain the queue
      drainAndWrite(writerState, queueWrapper);
      writerState.append("]");
      if (writerState.length() > 0) {
        output.write(writerState.getStringToFlush().getBytes(StandardCharsets.UTF_8));
        output.flush();
      }
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

  /**
   * Processes a single row from the queue or times out waiting for one.  If an error has occurred
   * during pull query execution, the queue is drained and the error is thrown.
   *
   * <p>Also, the thread may be interrupted by the completion callback, in which case, this method
   * completes immediately.
   * @param output The output stream to write to
   * @param writerState writer state
   * @param queueWrapper the queue wrapper
   * @throws Throwable If an exception is found while running the pull query, it's rethrown here.
   */
  private void processRow(
      final OutputStream output,
      final WriterState writerState,
      final QueueWrapper queueWrapper
  ) throws Throwable {
    final PullQueryRow toProcess = queueWrapper.pollNextRow();
    if (toProcess == QueueWrapper.END_ROW) {
      return;
    }
    if (toProcess != null) {
      writeRow(toProcess, writerState, queueWrapper.hasAnotherRow());
      if (writerState.length() >= FLUSH_SIZE_BYTES
          || (clock.millis() - writerState.getLastFlushMs()) >= MAX_FLUSH_MS
      ) {
        output.write(writerState.getStringToFlush().getBytes(StandardCharsets.UTF_8));
        output.flush();
      }
    }
    drainAndThrowOnError(output, writerState, queueWrapper);
  }

  /**
   * Does the job of writing the row to the writer state.
   * @param row The row to write
   * @param writerState writer state
   * @param hasAnotherRow if there's another row after this one.  This is used for determining how
   *                      to write proper JSON, e.g. whether to add a comma.
   */
  private void writeRow(
      final PullQueryRow row,
      final WriterState writerState,
      final boolean hasAnotherRow
  ) {
    // Send for a comma after the header
    if (!sentAtLeastOneRow) {
      writerState.append(",").append(System.lineSeparator());
      sentAtLeastOneRow = true;
    }
    StreamedRow streamedRow = null;
    if (row.getConsistencyOffsetVector().isPresent()) {
      streamedRow = StreamedRow.consistencyToken(new ConsistencyToken(
          row.getConsistencyOffsetVector().get().serialize()));
    } else {
      streamedRow = StreamedRow.pullRow(row.getGenericRow(), toKsqlHostInfo(row.getSourceNode()));
    }
    writerState.append(writeValueAsString(streamedRow));
    if (hasAnotherRow) {
      writerState.append(",").append(System.lineSeparator());
    }
  }

  /**
   * If an error has been stored in pullQueryException, drains the queue and throws the exception.
   * @param output The output stream to write to
   * @param writerState writer state
   * @throws Throwable If an exception is stored, it's rethrown.
   */
  private void drainAndThrowOnError(
      final OutputStream output,
      final WriterState writerState,
      final QueueWrapper queueWrapper
  ) throws Throwable {
    if (pullQueryException.get() != null) {
      drainAndWrite(writerState, queueWrapper);
      output.write(writerState.getStringToFlush().getBytes(StandardCharsets.UTF_8));
      output.flush();
      throw pullQueryException.get();
    }
  }

  /**
   * Drains the queue and writes the contained rows.
   * @param writerState writer state
   * @param queueWrapper the queue wrapper
   */
  private void drainAndWrite(final WriterState writerState, final QueueWrapper queueWrapper) {
    final List<PullQueryRow> rows = queueWrapper.drain();
    int i = 0;
    for (final PullQueryRow row : rows) {
      writeRow(row, writerState, i + 1 < rows.size());
      i++;
    }
  }

  /**
   * Outputs the given exception to the output stream.
   * @param out The output stream
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
    if (!closed.getAndSet(true)) {
      result.stop();
    }
  }

  @Override
  public int getWriteTimeoutMs() {
    return WRITE_TIMEOUT_MS;
  }

  public boolean isClosed() {
    return closed.get();
  }

  private boolean isCompletedOrHasException() {
    return completed.get() || pullQueryException.get() != null;
  }

  private void interruptWriterThread() {
    pullQueryQueue.putSentinelRow(QueueWrapper.END_ROW);
  }

  /**
   * Converts the object to json and returns the string.
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

  /**
   * State that's kept for the buffered response and the last flush time.
   */
  private static class WriterState {
    private final Clock clock;
    // The buffer of JSON that we're always flushing as we hit either time or size thresholds.
    private StringBuilder sb = new StringBuilder();
    // Last flush timestamp in millis
    private long lastFlushMs;

    WriterState(final Clock clock) {
      this.clock = clock;
    }

    public WriterState append(final String str) {
      sb.append(str);
      return this;
    }

    public int length() {
      return sb.length();
    }

    public long getLastFlushMs() {
      return lastFlushMs;
    }

    public String getStringToFlush() {
      final String str = sb.toString();
      sb = new StringBuilder();
      lastFlushMs = clock.millis();
      return str;
    }
  }

  /**
   * Wraps the PullQueryQueue to keep a hold of the head of the queue explicitly so it always knows
   * if there's something next.
   */
  static final class QueueWrapper {
    public static final PullQueryRow END_ROW = new PullQueryRow(null, null, null, null);
    private final PullQueryQueue pullQueryQueue;
    private final long disconnectCheckInterval;
    // We always keep a reference to the head of the queue so that we know if there's another
    // row in the result in order to produce proper JSON.
    private PullQueryRow head = null;

    QueueWrapper(final PullQueryQueue pullQueryQueue, final long disconnectCheckInterval) {
      this.pullQueryQueue = pullQueryQueue;
      this.disconnectCheckInterval = disconnectCheckInterval;
    }

    public boolean hasAnotherRow() {
      return head != null;
    }

    public PullQueryRow pollNextRow() throws InterruptedException {
      final PullQueryRow row = pullQueryQueue.pollRow(
          disconnectCheckInterval,
          TimeUnit.MILLISECONDS
      );

      if (row == END_ROW) {
        return END_ROW;
      }

      if (row != null) {
        // The head becomes the next thing to process and the newly polled row becomes the head.
        // The first time this is run, we'll always return null since we're keeping the row as the
        // new head.
        final PullQueryRow toProcess = head;
        head = row;
        return toProcess;
      }
      return null;
    }

    public List<PullQueryRow> drain() {
      final List<PullQueryRow> rows = Lists.newArrayList();
      if (head != null) {
        rows.add(head);
      }
      head = null;
      pullQueryQueue.drainRowsTo(rows);
      rows.remove(END_ROW);
      return rows;
    }
  }
}
