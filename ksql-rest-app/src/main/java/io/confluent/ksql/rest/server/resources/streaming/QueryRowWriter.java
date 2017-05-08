/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.server.resources.streaming;

import io.confluent.ksql.physical.GenericRow;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.streams.KeyValue;

import javax.json.Json;
import javax.json.JsonObjectBuilder;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class QueryRowWriter implements Runnable {
  private final OutputStream output;
  private final AtomicReference<Throwable> streamsException;
  private final SynchronousQueue<KeyValue<String, GenericRow>> rowQueue;
  private final List<Field> columns;
  private final AtomicBoolean rowsWritten;

  QueryRowWriter(
      OutputStream output,
      AtomicReference<Throwable> streamsException,
      SynchronousQueue<KeyValue<String, GenericRow>> rowQueue,
      List<Field> columns,
      AtomicBoolean rowsWritten
  ) {
    this.output = output;
    this.streamsException = streamsException;
    this.rowQueue = rowQueue;
    this.columns = columns;
    this.rowsWritten = rowsWritten;
  }

  @Override
  public void run() {
    try {
      while (true) {
        KeyValue<String, GenericRow> row = rowQueue.take();
        write(row.key, row.value);
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

  private void write(String key, GenericRow genericRow) throws IOException {
    JsonObjectBuilder values = Json.createObjectBuilder();
    List<Object> rowValues = genericRow.getColumns();

    if (rowValues.size() != columns.size()) {
      throw new RuntimeException(String.format(
          "Lengths of columns and rowValues differ: %d vs %d, respectively",
          columns.size(),
          rowValues.size()
      ));
    }

    for (int i = 0; i < rowValues.size(); i++) {
      addRowValue(values, columns.get(i), rowValues.get(i));
    }

    JsonObjectBuilder row = Json.createObjectBuilder();
    row.add("values", values.build());
    row.add("key", key);

    JsonObjectBuilder message = Json.createObjectBuilder().add("row", row.build());

    synchronized (output) {
      output.write((message.build().toString() + "\n").getBytes());
      output.flush();
      rowsWritten.set(true);
    }
  }

  private void addRowValue(JsonObjectBuilder row, Field columnField, Object value) {
    String fieldName = columnField.name();
    if (value == null) {
      row.addNull(fieldName);
      return;
    }
    switch (columnField.schema().type()) {
      case FLOAT64:
        row.add(fieldName, (double) value);
        return;
      case FLOAT32:
        row.add(fieldName, (float) value);
        return;
      case INT8:
        row.add(fieldName, (byte) value);
        return;
      case INT16:
        row.add(fieldName, (short) value);
        return;
      case INT32:
        row.add(fieldName, (int) value);
        return;
      case INT64:
        row.add(fieldName, (long) value);
        return;
      case STRING:
        row.add(fieldName, (String) value);
        return;
      default:
        throw new RuntimeException(String.format(
            "Cannot handle Field schema of type `%s' yet",
            columnField.schema().type()
        ));
    }
  }
}
