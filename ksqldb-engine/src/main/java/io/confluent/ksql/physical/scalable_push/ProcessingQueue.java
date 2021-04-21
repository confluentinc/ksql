package io.confluent.ksql.physical.scalable_push;

import io.confluent.ksql.execution.streams.materialization.TableRow;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class ProcessingQueue {

  private static final int BLOCKING_QUEUE_CAPACITY = 100;

  private final LinkedList<TableRow> rowQueue;
  private final int queueSizeLimit;
  private boolean closed = false;
  private boolean droppedRows = false;
  private Runnable newRowCallback = () -> {};

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
