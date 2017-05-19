/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server.resources.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.physical.GenericRow;
import org.apache.kafka.streams.KeyValue;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class QueryRowWriter implements Runnable {
  private final OutputStream output;
  private final AtomicReference<Throwable> streamsException;
  private final SynchronousQueue<KeyValue<String, GenericRow>> rowQueue;
  private final AtomicBoolean rowsWritten;
  private final ObjectMapper objectMapper;

  QueryRowWriter(
      OutputStream output,
      AtomicReference<Throwable> streamsException,
      SynchronousQueue<KeyValue<String, GenericRow>> rowQueue,
      AtomicBoolean rowsWritten
  ) {
    this.output = output;
    this.streamsException = streamsException;
    this.rowQueue = rowQueue;
    this.rowsWritten = rowsWritten;

    this.objectMapper = new ObjectMapper();
  }

  @Override
  public void run() {
    try {
      while (true) {
        write(rowQueue.take().value);
      }
    } catch (InterruptedException exception) {
      // Interrupt is used to end the thread
    } catch (Exception exception) {
      // Would just throw the exception, but 1) can't throw checked exceptions from Runnable.run(), and 2) seems easier
      // than converting the exception into an unchecked exception and then throwing it to a custom
      // Thread.UncaughtExceptionHandler
      streamsException.compareAndSet(null, exception);
    }
  }

  private void write(GenericRow row) throws IOException {
    synchronized (output) {
      objectMapper.writeValue(output, row);
      output.write("\n".getBytes());
      output.flush();
      rowsWritten.set(true);
    }
  }
}
