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

package io.confluent.ksql.physical.scalablepush;

import io.confluent.ksql.execution.streams.materialization.TableRow;
import java.util.LinkedList;

/**
 * A queue for storing pre-processed rows for a given scalable push query request. This queue
 * starts dropping rows if they're past the capacity, and keeps track so it can be reported to the
 * request.
 */
public class ProcessingQueue {

  static final int BLOCKING_QUEUE_CAPACITY = 100;

  private final LinkedList<TableRow> rowQueue;
  private final int queueSizeLimit;
  private boolean closed = false;
  private boolean droppedRows = false;
  private Runnable newRowCallback = () -> { };

  public ProcessingQueue() {
    this(BLOCKING_QUEUE_CAPACITY);
  }

  public ProcessingQueue(final int queueSizeLimit) {
    this.queueSizeLimit = queueSizeLimit;
    this.rowQueue = new LinkedList<>();
  }

  public synchronized boolean offer(final TableRow tableRow) {
    if (rowQueue.size() < queueSizeLimit && !droppedRows) {
      rowQueue.offer(tableRow);
      newRowCallback.run();
      return true;
    }
    droppedRows = true;
    return false;
  }

  public synchronized TableRow poll() {
    if (!closed) {
      return rowQueue.poll();
    }
    return null;
  }

  public synchronized void close() {
    closed = true;
  }

  public synchronized boolean isClosed() {
    return closed;
  }

  public synchronized void setNewRowCallback(final Runnable newRowCallback) {
    this.newRowCallback = newRowCallback;
  }

  public synchronized boolean hasDroppedRows() {
    return droppedRows;
  }
}
