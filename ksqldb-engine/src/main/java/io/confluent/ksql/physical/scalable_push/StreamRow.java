package io.confluent.ksql.physical.scalable_push;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.TableRow;
import io.confluent.ksql.execution.streams.materialization.Window;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Optional;
import org.apache.kafka.streams.kstream.Windowed;

public class StreamRow implements TableRow {

  private final LogicalSchema schema;
  private final GenericKey key;
  private final GenericRow value;
  private final long rowTime;
  private final Windowed windowed;

  public StreamRow(
      final LogicalSchema schema,
      final GenericKey key,
      final GenericRow value,
      final long rowTime
  ) {
    this.schema = schema;
    this.key = key;
    this.value = value;
    this.rowTime = rowTime;
    this.windowed = null;
  }

  public StreamRow(
      final LogicalSchema schema,
      final Windowed windowed,
      final GenericRow value,
      final long rowTime
  ) {
    this.schema = schema;
    this.key = (GenericKey) windowed.key();
    this.value = value;
    this.rowTime = rowTime;
    this.windowed = windowed;
  }

  @Override
  public LogicalSchema schema() {
    return schema;
  }

  @Override
  public long rowTime() {
    return rowTime;
  }

  @Override
  public GenericKey key() {
    return key;
  }

  @Override
  public Optional<Window> window() {
    return Optional.ofNullable(windowed)
        .map(w -> Window.of(w.window().startTime(), w.window().endTime()));
  }

  @Override
  public GenericRow value() {
    return value;
  }

  @Override
  public TableRow withValue(GenericRow newValue, LogicalSchema newSchema) {
    return new StreamRow(newSchema, key, newValue, rowTime);
  }
}
