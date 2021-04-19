package io.confluent.ksql.physical.scalable_push;

import io.confluent.ksql.execution.streams.materialization.TableRow;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ProcessingQueue {

  private static final int BLOCKING_QUEUE_CAPACITY = 100;

  private final BlockingQueue<TableRow> rowQueue;
  private volatile boolean closed = false;

  public ProcessingQueue() {
    this(BLOCKING_QUEUE_CAPACITY);
  }

  public ProcessingQueue(final int queueSizeLimit) {
    this.rowQueue = new ArrayBlockingQueue<>(queueSizeLimit);
  }

  public boolean offer(final TableRow tableRow) {
    return rowQueue.offer(tableRow);
  }

  public TableRow get() {
    while (!closed) {
      try {
        TableRow row = rowQueue.poll(1000, TimeUnit.MILLISECONDS);
        if (row != null) {
          return row;
        }
      } catch (InterruptedException e) {
        return null;
      }
    }
    return null;
  }

  public void close() {
    closed = true;
  }
}
