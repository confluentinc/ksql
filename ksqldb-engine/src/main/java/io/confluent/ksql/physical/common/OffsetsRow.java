package io.confluent.ksql.physical.common;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.Window;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.PushOffsetRange;
import java.util.Optional;

public class OffsetsRow implements QueryRow {

  private static final LogicalSchema EMPTY_SCHEMA = LogicalSchema.builder().build();
  private final long rowTime;
  private final PushOffsetRange pushOffsetRange;

  public static OffsetsRow of(
      final long rowTime,
      final PushOffsetRange pushOffsetRange
  ) {
    return new OffsetsRow(rowTime, pushOffsetRange);
  }

  OffsetsRow(
      final long rowTime,
      final PushOffsetRange pushOffsetRange
  ) {
    this.rowTime = rowTime;
    this.pushOffsetRange = pushOffsetRange;
  }

  @Override
  public LogicalSchema schema() {
    return EMPTY_SCHEMA;
  }

  @Override
  public long rowTime() {
    return rowTime;
  }

  @Override
  public GenericKey key() {
    return GenericKey.genericKey();
  }

  @Override
  public Optional<Window> window() {
    return Optional.empty();
  }

  @Override
  public GenericRow value() {
    return GenericRow.genericRow();
  }

  @Override
  public Optional<PushOffsetRange> getOffsetRange() {
    return Optional.of(pushOffsetRange);
  }
}
