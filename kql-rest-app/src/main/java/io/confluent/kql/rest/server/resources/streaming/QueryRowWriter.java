package io.confluent.kql.rest.server.resources.streaming;

import io.confluent.kql.physical.GenericRow;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Windowed;

import javax.json.Json;
import javax.json.JsonObjectBuilder;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class QueryRowWriter<K> implements ForeachAction<K, GenericRow> {
  private final OutputStream output;
  private final AtomicReference<Throwable> streamsException;
  private final List<Field> columns;
  private final AtomicBoolean rowsWritten;

  public QueryRowWriter(
      OutputStream output,
      AtomicReference<Throwable> streamsException,
      List<Field> columns,
      AtomicBoolean rowsWritten
  ) {
    this.output = output;
    this.streamsException = streamsException;
    this.columns = columns;
    this.rowsWritten = rowsWritten;
  }

  @Override
  public void apply(K key, GenericRow row) {
    String keyString;
    if (key instanceof Windowed) {
      keyString = ((Windowed) key).key().toString();
    } else {
      keyString = key.toString();
    }

    try {
      write(keyString, row);
    } catch (Exception exception) {
      streamsException.compareAndSet(null, exception);
    }
  }

  private void write(String key, GenericRow genericRow) throws Exception {
    JsonObjectBuilder values = Json.createObjectBuilder();
    List<Object> rowValues = genericRow.getColumns();

    if (rowValues.size() != columns.size()) {
      throw new Exception(String.format(
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

  private void addRowValue(JsonObjectBuilder row, Field columnField, Object value) throws Exception {
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
        throw new Exception(String.format(
            "Cannot handle Field schema of type `%s' yet",
            columnField.schema().type()
        ));
    }
  }
}
