/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.scalablepush;

import io.confluent.ksql.execution.common.QueryRow;
import io.confluent.ksql.query.QueryId;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A queue for storing pre-processed rows for a given scalable push query request. This queue
 * starts dropping rows if they're past the capacity, and keeps track so it can be reported to the
 * request.
 *
 * <p>The class is threadsafe since it's assumed that different threads are producing and consuming
 * the data.
 */
public class ProcessingQueue {

  static final int BLOCKING_QUEUE_CAPACITY = 1000;

  private final Deque<QueryRow> rowQueue;
  private final QueryId queryId;
  private final int queueSizeLimit;
  private boolean closed = false;
  private boolean droppedRows = false;
  private boolean hasError = false;
  private Runnable newRowCallback = () -> { };

  public ProcessingQueue(final QueryId queryId) {
    this(queryId, BLOCKING_QUEUE_CAPACITY);
  }

  public ProcessingQueue(final QueryId queryId, final int queueSizeLimit) {
    this.queryId = queryId;
    this.queueSizeLimit = queueSizeLimit;
    this.rowQueue = new ArrayDeque<>();
  }

  /**
   * Adds a {@link QueryRow} to the queue. This is expected to be called from the processor streams
   * thread when a new row arrives.
   * @param queryRow The row to add
   * @return if the row has been successfully added to the queue or if it's been dropped due to
   *     being at the size limit.
   */
  public synchronized boolean offer(final QueryRow queryRow) {
    if (closed) {
      return false;
    } else if (rowQueue.size() < queueSizeLimit && !droppedRows) {
      rowQueue.offer(queryRow);
      newRowCallback.run();
      return true;
    }
    droppedRows = true;
    return false;
  }

  /**
   * Reads a row from the queue. This is expected to be called from the plan's physical operator
   * which is called from the Vertx context.
   * @return The next row or null if either the queue is closed or there's no data to return.
   */
  public synchronized QueryRow poll() {
    if (!closed) {
      return rowQueue.poll();
    }
    return null;
  }

  /**
   * Closes the queue which causes rows to stop being returned.
   */
  public synchronized void close() {
    closed = true;
  }

  public synchronized boolean isClosed() {
    return closed;
  }

  /**
   * Sets a callback which is invoked every time a new row has been enqueued.
   * @param newRowCallback The callback to invoke
   */
  public synchronized void setNewRowCallback(final Runnable newRowCallback) {
    this.newRowCallback = newRowCallback;
  }

  /**
   * Whether rows have been dropped due to hitting the queue limit.
   */
  public synchronized boolean hasDroppedRows() {
    return droppedRows;
  }

  public synchronized void onError() {
    hasError = true;
  }

  public synchronized boolean getHasError() {
    return hasError;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public synchronized boolean isAtLimit() {
    return rowQueue.size() >= queueSizeLimit;
  }
}
